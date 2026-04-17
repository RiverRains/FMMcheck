import logging
import json
import os
import asyncio
from pathlib import Path

from config.settings import setup_logging, get_api_key
from api.genius_client import GeniusClient
from processing.match_evaluator import evaluate_webcast_data, evaluate_end_game_past_match_data, format_match_data
from storage.excel_writer import create_excel_file_with_competitions, read_whitelist_from_excel
from storage.gdrive_uploader import upload_to_gdrive, download_from_gdrive

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

async def resolve_incomplete_whitelist(client, whitelist_config):
    """
    For whitelist entries where only the ID is provided (no name/league_id),
    fetch the competition info from the API and fill in the missing fields.
    """
    competitions = whitelist_config.get('active_competitions', [])
    updated = False
    for comp in competitions:
        if comp.get('name') and comp.get('league_id'):
            continue
        comp_id = comp['id']
        logger.info(f"Resolving competition info for ID {comp_id} from API...")
        info = await client.fetch_competition_info(comp_id)
        if info:
            comp['name'] = info.get('name', f'Competition {comp_id}')
            comp['league_id'] = info.get('league_id', 0)
            comp['league_name'] = info.get('league_name', '')
            from datetime import datetime
            comp['added_date'] = datetime.now().strftime('%Y-%m-%d')
            logger.info(f"  Resolved: {comp['name']} (League: {comp['league_name']})")
            updated = True
        else:
            logger.warning(f"  Could not resolve competition {comp_id} from API")
    if updated:
        whitelist_config['active_competitions'] = competitions
        whitelist_config['total_competitions'] = len(competitions)
    return whitelist_config


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
                match_started = time_diff >= 0
                is_live_window = -2 <= time_diff <= 48
                # Webcast data persists long after the match — check for any past match up to 14 days
                is_webcast_window = time_diff >= -2 and time_diff <= 336
            else:
                match_started = False
                is_live_window = True
                is_webcast_window = True

            # Skip live and webcast checks for matches that haven't started yet
            if match_started and is_live_window:
                # Check publish connection (only meaningful around match time)
                match['publish_connection_status'] = await client.check_publish_connection(match_id)
                logger.info(f"Match {match_id}: publish_connection={match['publish_connection_status']}")

            if match_started and is_webcast_window:
                # Check webcast (data stays available long after the match)
                webcast_data = await client.fetch_webcast_json(match_id)
                match['webcast_status'] = evaluate_webcast_data(match_id, webcast_data)
                logger.info(f"Match {match_id}: webcast_status={match['webcast_status']}")

            # Run HS end-game check (separate try/except so failures don't clobber other fields)
            try:
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
                        fed_slug = str(federation_code).strip()
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
                # Keep existing status if already set; if unknown, treat as needing a check
                # rather than 'Too early' (which would silently clear the issue from the state)
                existing_eg = match.get('end_game_status', '')
                match['end_game_status'] = existing_eg if existing_eg else 'N/A - Match check required'

        except Exception as e:
            logger.error(f"Match {match_id}: Failed processing match checks: {e}")
            match.setdefault('livestream_status', "N/A")
            match.setdefault('whst_live_data_source_match', "N/A")
            match.setdefault('publish_connection_status', "N/A")
            match.setdefault('webcast_status', "N/A")
            match.setdefault('end_game_status', 'Too early')
        
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
    two_weeks_ago = today - timedelta(days=14)
    from_date = two_weeks_ago.strftime('%Y-%m-%d')
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
    logger.info("=== FOOTBALL DATA FETCH STARTING ===")

    api_key = get_api_key()
    if not api_key:
        logger.error("Exiting: No Genius Sports API key provided.")
        return

    output_path = os.getenv("OUTPUT_EXCEL_PATH", "football_competitions_fetch.xlsx")

    # Download Excel from Google Drive to preserve manual edits between runs
    download_from_gdrive(Path(output_path).name, output_path)

    # Google Drive is the authoritative source for notification state — it is
    # updated at the end of every successful run.  Always download from Drive
    # so that a stale GitHub Actions cache snapshot never silently wins.
    # The cache (restored by the workflow) acts only as a fallback when Drive
    # is unavailable.
    state_dir = Path(output_path).parent
    notification_state_file = state_dir / "notification_state.json"
    drive_state_ok = download_from_gdrive("notification_state.json", str(notification_state_file))
    if drive_state_ok:
        logger.info("Notification state downloaded from Google Drive (authoritative source).")
    elif notification_state_file.exists():
        logger.info("Drive unavailable — using cached notification state as fallback.")
    else:
        logger.info("No prior notification state found — starting fresh this run.")

    # Try reading whitelist from the Excel file's Whitelist tab first
    whitelist_config = read_whitelist_from_excel(output_path)
    if whitelist_config:
        whitelist_ids = [comp['id'] for comp in whitelist_config['active_competitions']]
        logger.info(f"Using whitelist from Excel file ({len(whitelist_ids)} competitions)")
    else:
        # Fall back to JSON file
        whitelist_path = os.getenv("WHITELIST_PATH", "competition_whitelist.json")
        whitelist_ids, whitelist_config = load_competition_whitelist(whitelist_path)

    if not whitelist_ids:
        logger.error("Exiting: No competitions in whitelist.")
        return

    client = GeniusClient(api_key=api_key)

    try:
        # Resolve incomplete whitelist entries (user added only an ID)
        whitelist_config = await resolve_incomplete_whitelist(client, whitelist_config)
        whitelist_ids = [comp['id'] for comp in whitelist_config['active_competitions']]

        competitions_with_matches = await process_whitelisted_competitions(client, whitelist_ids, whitelist_config)
    finally:
        await client.close()

    if not competitions_with_matches:
        logger.error("No whitelisted competitions found or no matches available.")
        return

    success = create_excel_file_with_competitions(competitions_with_matches, output_path, whitelist_config)

    if success:
        logger.info("FMM Automation run completed successfully.")
        upload_to_gdrive(output_path)
        if notification_state_file.exists():
            upload_to_gdrive(str(notification_state_file))
    else:
        logger.error("FMM Automation run failed to save output.")

def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
