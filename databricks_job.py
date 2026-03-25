import logging
import json
import os
import asyncio
from pathlib import Path

from config.settings import setup_logging, get_api_key, inject_databricks_secrets_into_env
from api.genius_client import GeniusClient
from processing.match_evaluator import evaluate_webcast_data, evaluate_end_game_past_match_data, format_match_data
from storage.excel_writer import create_excel_file_with_competitions
from storage.gdrive_uploader import upload_to_gdrive

logger = logging.getLogger(__name__)

def load_competition_whitelist(whitelist_file='competition_whitelist.json'):
    """Load which competitions to process from competition_whitelist.json."""
    try:
        with open(whitelist_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        competition_ids = [comp['id'] for comp in config['active_competitions']]
        
        logger.info(f"Loaded whitelist with {len(competition_ids)} competitions:")
        for comp in config['active_competitions']:
            logger.info(f"  - {comp['name']} (ID: {comp['id']})")
        
        return competition_ids, config
        
    except FileNotFoundError:
        logger.error(f"Whitelist file not found: {whitelist_file}")
        return [], {}
    except Exception as e:
        logger.error(f"Error loading whitelist: {e}")
        return [], {}

async def process_single_match(client, match, competition_live_data_source, league_abbrev, semaphore):
    """
    Process all checks for a single match.
    Runs validation checks: livestream, data source, connections, webcast, and end game data.
    """
    async with semaphore:
        match_id = match.get('matchId', '')
        if not match_id:
            match['livestream_status'] = "N/A"
            match['whst_live_data_source_match'] = "N/A"
            match['publish_connection_status'] = "N/A"
            match['webcast_status'] = "N/A"
            match['end_game_status'] = ''
            return match
        
        try:
            # Extract details
            match_data = await client.fetch_match_details(match_id)
            if not match_data:
                match['livestream_status'] = "N/A"
                match['match_live_data_source'] = "N/A"
            else:
                live_stream_value = match_data.get('liveStream')
                match['livestream_status'] = "Yes" if live_stream_value == 1 else "No" if live_stream_value == 0 else "N/A"
                
                match['match_live_data_source'] = (
                    match_data.get('LiveDataSource') or
                    (match_data.get('internalConfiguration') or {}).get('LiveDataSource') or
                    match_data.get('competitionLiveDataSource') or
                    match_data.get('liveDataSource')
                )
            
            # Check live data source match
            if competition_live_data_source and match.get('match_live_data_source'):
                comp_ds = str(competition_live_data_source).strip()
                match_ds = str(match['match_live_data_source']).strip()
                match['whst_live_data_source_match'] = "Yes" if comp_ds.lower() == match_ds.lower() else "No"
            else:
                match['whst_live_data_source_match'] = "N/A"
            
            from processing.match_evaluator import match_has_started
            from datetime import datetime, timezone, timedelta
            
            utc_str = (match.get('matchTimeUTC') or '').strip()
            local_str = (match.get('matchTime') or '').strip()
            kickoff_time = None
            use_utc = False
            
            if utc_str:
                try:
                    kickoff_time = datetime.strptime(utc_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                    use_utc = True
                except ValueError:
                    pass
            
            if kickoff_time is None and local_str:
                try:
                    kickoff_time = datetime.strptime(local_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass
                    
            now = datetime.now(timezone.utc) if use_utc else datetime.now()
            
            if kickoff_time is not None:
                time_diff = (now - kickoff_time).total_seconds() / 3600
                is_live_window = -2 <= time_diff <= 48
            else:
                is_live_window = True
                
            if is_live_window:
                # Check publish connection
                match['publish_connection_status'] = await client.check_publish_connection(match_id)
                
                # Check webcast
                webcast_data = await client.fetch_webcast_json(match_id)
                match['webcast_status'] = evaluate_webcast_data(match_id, webcast_data)
            else:
                match['publish_connection_status'] = "N/A"
                match['webcast_status'] = "N/A"
            
            # Run HS end-game check
            kickoff_plus_2h = kickoff_time + timedelta(hours=2) if kickoff_time else None
            
            if kickoff_plus_2h is not None and kickoff_plus_2h <= now:
                federation_code = league_abbrev
                if not federation_code:
                    # Need to find the code
                    league_obj = match_data.get('league') or {} if match_data else {}
                    federation_code = (
                        match_data.get('leagueAbbrev') or match_data.get('abbreviation') or
                        match_data.get('federationCode') or match_data.get('code') or match_data.get('slug') or
                        league_obj.get('leagueAbbrev') or league_obj.get('abbreviation') or
                        league_obj.get('federationCode') or league_obj.get('code') or league_obj.get('slug')
                    ) if match_data else None
                    
                    if not federation_code:
                        league_id = match_data.get('leagueId') or (match_data.get('league') or {}).get('leagueId') if match_data else None
                        if league_id:
                            federation_code = await client.fetch_league_details(league_id)

                if federation_code:
                    logger.debug(f"Match {match_id}: Running HS end-game check...")
                    fed_slug = str(federation_code).strip().lower()
                    hs_data = await client.fetch_hs_summary_json(fed_slug, match_id)
                    
                    if hs_data:
                        unregistered = await client.fetch_unregistered_players(match_id)
                        end_game_status = evaluate_end_game_past_match_data(match_id, hs_data, unregistered)
                    else:
                        end_game_status = "N/A"
                    
                    if end_game_status == "No":
                        match['end_game_status'] = "Match check required"
                        match['end_game_hs_url'] = f"https://hosted.dcd.shared.geniussports.com/{fed_slug}/en/match/{match_id}/summary"
                    elif end_game_status == "N/A":
                        match['end_game_status'] = "N/A - Match check required"
                        match['end_game_hs_url'] = f"https://hosted.dcd.shared.geniussports.com/{fed_slug}/en/match/{match_id}/summary"
                    else:
                        match['end_game_status'] = end_game_status
                else:
                    logger.debug(f"Match {match_id}: HS check N/A (no federation code)")
                    match['end_game_status'] = "N/A - Match check required"
            else:
                match['end_game_status'] = 'Too early'
        except Exception as e:
            logger.error(f"Match {match_id}: Failed processing end game check: {e}")
            match['end_game_status'] = 'Too early'
            match['livestream_status'] = match.get('livestream_status', "N/A")
            match['whst_live_data_source_match'] = match.get('whst_live_data_source_match', "N/A")
            match['publish_connection_status'] = match.get('publish_connection_status', "N/A")
            match['webcast_status'] = match.get('webcast_status', "N/A")
        
        return match

async def fetch_competition_data(client, comp_id, whitelist_lookup, from_date, to_date):
    """Fetches matches and competition data for a single competition."""
    logger.info(f"Processing competition ID: {comp_id}")
    
    comp_data = whitelist_lookup.get(comp_id, {})
    comp_name = comp_data.get('name', f"Competition {comp_id}")
    league_id = comp_data.get('league_id', 0)
    league_name = comp_data.get('league_name', "Unknown League")
    live_data_source = None
    league_abbrev = None
    
    if league_id:
        league_abbrev = await client.fetch_league_details(league_id)
        if not league_abbrev and comp_data.get('federation_code'):
            league_abbrev = str(comp_data.get('federation_code')).strip()
    
    if league_id:
        comp_list = await client.fetch_competitions_for_league(league_id)
        for c in comp_list:
            if c.get('competitionId') == comp_id:
                live_data_source = (
                    (c.get('internalConfiguration') or {}).get('LiveDataSource')
                    or c.get('competitionLiveDataSource')
                )
                break
                
    logger.info(f"Fetching matches for {comp_name} ({from_date} to {to_date})")
    raw_matches = await client.fetch_matches_for_competition(comp_id, from_date, to_date)
    
    formatted_matches = []
    if raw_matches:
        sample = raw_matches[0]
        logger.info(f"DEBUG: Raw match keys: {list(sample.keys())}")
        logger.info(f"DEBUG: Raw match sample (first 500 chars): {str(sample)[:500]}")
    for match_info in raw_matches:
        formatted_match = format_match_data(match_info)
        formatted_matches.append(formatted_match)
        
    return {
        'competitionId': comp_id,
        'competitionName': comp_name,
        'leagueId': league_id,
        'leagueName': league_name,
        'leagueAbbrev': league_abbrev or '',
        'liveDataSource': live_data_source or '',
        'formatted_matches': formatted_matches
    }

async def process_whitelisted_competitions(client, whitelist_ids, whitelist_config):
    """Load league abbrev and LiveDataSource per whitelist entry, then fetch and process matches."""
    whitelist_competitions = whitelist_config.get('active_competitions', [])
    whitelist_lookup = {comp['id']: comp for comp in whitelist_competitions}
    
    from datetime import datetime, timedelta
    today = datetime.now().date()
    one_week_ahead = today + timedelta(days=7)
    one_week_ago = today - timedelta(days=7)
    from_date = one_week_ago.strftime('%Y-%m-%d')
    to_date = one_week_ahead.strftime('%Y-%m-%d')

    competition_results = []
    
    # 1. Fetch competition data in parallel
    logger.info("Fetching competition data in parallel...")
    comp_tasks = [
        fetch_competition_data(client, comp_id, whitelist_lookup, from_date, to_date)
        for comp_id in whitelist_ids
    ]
    
    comp_results = await asyncio.gather(*comp_tasks, return_exceptions=True)
    
    for i, res in enumerate(comp_results):
        if isinstance(res, Exception):
            logger.error(f"Error fetching data for competition {whitelist_ids[i]}: {res}")
        else:
            competition_results.append(res)

    # 2. Flatten all matches to process them globally
    all_match_tasks = []
    for comp in competition_results:
        for match in comp['formatted_matches']:
            all_match_tasks.append({
                'match': match,
                'liveDataSource': comp['liveDataSource'],
                'leagueAbbrev': comp['leagueAbbrev'],
                'competitionId': comp['competitionId']
            })

    logger.info(f"Processing {len(all_match_tasks)} total matches globally...")
    
    processed_matches_by_comp = {comp['competitionId']: [] for comp in competition_results}
    
    semaphore = asyncio.Semaphore(50)
    
    if all_match_tasks:
        match_coros = [
            process_single_match(client, task['match'], task['liveDataSource'], task['leagueAbbrev'], semaphore)
            for task in all_match_tasks
        ]
        
        match_results = await asyncio.gather(*match_coros, return_exceptions=True)
        
        for i, res in enumerate(match_results):
            task = all_match_tasks[i]
            comp_id = task['competitionId']
            if isinstance(res, Exception):
                match = task['match']
                logger.error(f"Error processing match {match.get('matchId')}: {res}")
                match['livestream_status'] = "N/A"
                match['whst_live_data_source_match'] = "N/A"
                match['publish_connection_status'] = "N/A"
                match['webcast_status'] = "N/A"
                match['end_game_status'] = ''
                processed_matches_by_comp[comp_id].append(match)
            else:
                processed_matches_by_comp[comp_id].append(res)

    # 3. Reconstruct the final list
    competitions_with_matches = []
    for comp in competition_results:
        comp_id = comp['competitionId']
        processed_matches = processed_matches_by_comp[comp_id]
        
        competition_entry = {
            'competitionId': comp_id,
            'competitionName': comp['competitionName'],
            'leagueId': comp['leagueId'],
            'leagueName': comp['leagueName'],
            'leagueAbbrev': comp['leagueAbbrev'],
            'liveDataSource': comp['liveDataSource'],
            'matches': processed_matches
        }
        competitions_with_matches.append(competition_entry)
        logger.debug(f"Found {len(processed_matches)} matches for competition {comp_id}")
        
    return competitions_with_matches

async def main_async():
    setup_logging()
    inject_databricks_secrets_into_env()  # load from Databricks secrets into os.environ when available (e.g. serverless)
    logger.info("=== FOOTBALL DATA FETCH STARTING ===")

    api_key = get_api_key()
    if not api_key:
        logger.error("Exiting: No Genius Sports API key provided.")
        return
        
    # Get whitelist config path based on current environment
    # In Databricks, you might need an absolute path to DBFS or a repo path
    whitelist_path = os.getenv("WHITELIST_PATH", "competition_whitelist.json")
    
    whitelist_ids, whitelist_config = load_competition_whitelist(whitelist_path)
    if not whitelist_ids:
        logger.error("Exiting: No competitions in whitelist.")
        return
        
    client = GeniusClient(api_key=api_key)
    
    try:
        competitions_with_matches = await process_whitelisted_competitions(client, whitelist_ids, whitelist_config)
    finally:
        await client.close()
    
    if not competitions_with_matches:
        logger.error("No whitelisted competitions found or no matches available.")
        return

    # Output path: use env if set; on Databricks prefer a persistent DBFS location when available
    output_path = os.getenv("OUTPUT_EXCEL_PATH")
    if not output_path:
        if os.getenv("DATABRICKS_RUNTIME_VERSION"):
            if Path("/dbfs").exists():
                output_path = "/dbfs/FileStore/fmm/football_competitions_fetch.xlsx"
                logger.info(
                    "OUTPUT_EXCEL_PATH not set; using persistent DBFS path %s. Override OUTPUT_EXCEL_PATH to use a UC Volume or custom DBFS location.",
                    output_path,
                )
            else:
                output_path = "/tmp/football_competitions_fetch.xlsx"
                logger.info(
                    "OUTPUT_EXCEL_PATH not set and /dbfs is unavailable; using /tmp (ephemeral). Set OUTPUT_EXCEL_PATH to a persistent location to retain Excel and Slack notification dedupe state."
                )
        else:
            output_path = "football_competitions_fetch.xlsx"

    success = create_excel_file_with_competitions(competitions_with_matches, output_path)
    
    if success:
        logger.info("FMM Automation run completed successfully.")
        upload_to_gdrive(output_path)
    else:
        logger.error("FMM Automation run failed to save output.")

def main():
    # Allow asyncio.run() when already inside a running loop (e.g. Databricks / IPython)
    try:
        import nest_asyncio
        nest_asyncio.apply()
    except ImportError:
        pass
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
