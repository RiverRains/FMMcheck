import logging
from datetime import datetime, timedelta, time, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)

def match_has_started(match):
    """True if kickoff is in the past (game started). Prefers matchTimeUTC when present."""
    utc_str = match.get("matchTimeUTC", "").strip()
    if utc_str:
        try:
            kickoff = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            return kickoff <= datetime.now(timezone.utc)
        except Exception:
            pass
    local_str = match.get("matchTime", "").strip()
    if local_str:
        try:
            kickoff = datetime.strptime(local_str, "%Y-%m-%d %H:%M:%S")
            return kickoff <= datetime.now()
        except Exception:
            pass
    return False

def collect_check_issues(competitions):
    """
    Build a list of matches that failed one or more checks (DM, WHST, Statistician, Webcast, End game).
    Used for the Slack issues section. Returns per-competition blocks with match details and webcast/HS links.
    """
    result = []
    for comp in competitions:
        league_name = comp.get("leagueName", "")
        league_id = comp.get("leagueId", "")
        competition_name = comp.get("competitionName", "")
        competition_id = comp.get("competitionId", "")
        problem_matches = []
        for match in comp.get("matches", []):
            failed = []
            webcast_url = None
            hs_url = None
            if (match.get("livestream_status") or "").strip() == "No":
                failed.append("Pre-game DM check")
            if (match.get("whst_live_data_source_match") or "").strip() == "No":
                failed.append("Pre-game WHST Live Data Source")
            started = match_has_started(match)
            if started:
                if (match.get("publish_connection_status") or "").strip() == "No":
                    failed.append("Live game Statistician check")
                status = (match.get("webcast_status") or "").strip()
                if status in ("No", "N/A"):
                    failed.append("Live game Webcast check")
                    mid = match.get("matchId", "")
                    league_abbrev = (comp.get("leagueAbbrev") or "").strip().lower()
                    if mid and league_abbrev:
                        webcast_url = f"https://livestats.dcd.shared.geniussports.com/u/{league_abbrev}/{mid}/"
            end_game = (match.get("end_game_status") or "").strip()
            if end_game == "Match check required":
                failed.append("End game past match data")
                hs_url = match.get("end_game_hs_url") or ""
            elif end_game == "N/A - Match check required":
                failed.append("End game past match data")
                hs_url = match.get("end_game_hs_url") or ""
            if failed:
                problem_matches.append({
                    "game": (match.get("game") or "").strip() or "Match",
                    "matchId": match.get("matchId", ""),
                    "failed_checks": failed,
                    "webcast_url": webcast_url,
                    "hs_url": hs_url,
                })
        if problem_matches:
            result.append({
                "league_name": league_name,
                "league_id": league_id,
                "competition_name": competition_name,
                "competition_id": competition_id,
                "matches": problem_matches,
            })
    return result


def _normalize_issue_key_part(value):
    return str(value or "").strip().replace("|", "/")


def build_issue_fingerprint(competition_id, match_id, check_name, competition_name="", game=""):
    """
    Build a stable key for a single match/check issue.
    Prefer IDs when available and fall back to names so malformed payloads still dedupe consistently.
    """
    competition_part = _normalize_issue_key_part(competition_id) or _normalize_issue_key_part(competition_name) or "unknown_competition"
    match_part = _normalize_issue_key_part(match_id) or _normalize_issue_key_part(game) or "unknown_match"
    check_part = _normalize_issue_key_part(check_name) or "unknown_check"
    return f"{competition_part}|{match_part}|{check_part}"


def flatten_check_issues(issue_groups):
    """
    Flatten grouped issue blocks into a stable issue map keyed by fingerprint.
    Used to diff current issues against the previously notified issue set.
    """
    flattened = {}

    for comp_issue in issue_groups:
        league_name = str(comp_issue.get("league_name", "")).strip()
        league_id = str(comp_issue.get("league_id", "")).strip()
        competition_name = str(comp_issue.get("competition_name", "")).strip()
        competition_id = str(comp_issue.get("competition_id", "")).strip()

        for match in comp_issue.get("matches", []):
            match_id = str(match.get("matchId", "")).strip()
            game = str(match.get("game", "")).strip() or "Match"
            webcast_url = str(match.get("webcast_url", "")).strip()
            hs_url = str(match.get("hs_url", "")).strip()

            for check_name in match.get("failed_checks", []):
                detail_label = ""
                detail_url = ""

                if check_name == "Live game Webcast check" and webcast_url:
                    detail_label = "Webcast"
                    detail_url = webcast_url
                elif check_name == "End game past match data" and hs_url:
                    detail_label = "HS"
                    detail_url = hs_url

                issue_key = build_issue_fingerprint(
                    competition_id=competition_id,
                    match_id=match_id,
                    check_name=check_name,
                    competition_name=competition_name,
                    game=game,
                )

                flattened[issue_key] = {
                    "issue_key": issue_key,
                    "league_name": league_name,
                    "league_id": league_id,
                    "competition_name": competition_name,
                    "competition_id": competition_id,
                    "game": game,
                    "match_id": match_id,
                    "check_name": check_name,
                    "detail_label": detail_label,
                    "detail_url": detail_url,
                }

    return flattened


def _extract_team_names(tm):
    team1_name = None
    team2_name = None
    name_fields = ['name', 'nameInternational', 'teamName', 'teamNameInternational', 'n', 'nm', 'team', 'teamNameInt']
    
    if not isinstance(tm, dict):
        return None, None
        
    for key, val in tm.items():
        if str(key) in ['1', 'team1', 'home', '0']:
            if isinstance(val, dict):
                team1_name = next((val.get(f) for f in name_fields if val.get(f)), team1_name)
        elif str(key) in ['2', 'team2', 'away', '1']:
            if isinstance(val, dict):
                team2_name = next((val.get(f) for f in name_fields if val.get(f)), team2_name)
                
    if not team1_name or not team2_name:
        for val in tm.values():
            if isinstance(val, dict):
                has_name = any(f in val for f in name_fields)
                has_players = 'pl' in val or 'players' in val
                if has_name or has_players:
                    found_name = next((val.get(f) for f in name_fields if val.get(f)), None)
                    if not team1_name:
                        team1_name = found_name
                    elif not team2_name and team1_name != val.get('name'):
                        team2_name = found_name
                    
                    if team1_name and team2_name:
                        break
                        
    return team1_name, team2_name

def _extract_team_players(tm, teams):
    team1_players = []
    team2_players = []
    
    if isinstance(tm, dict):
        team_objects = [v for v in tm.values() if isinstance(v, dict) and any(f in v for f in ['pl', 'players', 'player', 'p', 'name', 'teamName', 'n'])]
        
        for team_obj in team_objects:
            players = next((team_obj.get(f) for f in ['pl', 'players', 'player', 'p'] if team_obj.get(f)), None)
            
            if isinstance(players, dict):
                players_list = list(players.values())
            elif isinstance(players, list):
                players_list = players
            else:
                continue
                
            if not team1_players:
                team1_players = players_list
            elif not team2_players and players_list != team1_players:
                team2_players = players_list
            
            if team1_players and team2_players:
                break
                
    if (not team1_players or not team2_players) and isinstance(teams, dict) and teams:
        if not team1_players:
            team1_players = [p for p in teams.values() if isinstance(p, dict) and str(p.get('tno')) == '1']
        if not team2_players:
            team2_players = [p for p in teams.values() if isinstance(p, dict) and str(p.get('tno')) == '2']
            
    return [p for p in team1_players if isinstance(p, dict)], [p for p in team2_players if isinstance(p, dict)]

def _validate_lineups(team1_players, team2_players):
    has_team1_lineup = any(p.get('starter') == 1 for p in team1_players)
    has_team2_lineup = any(p.get('starter') == 1 for p in team2_players)
    return has_team1_lineup and has_team2_lineup

def evaluate_webcast_data(match_id, data):
    """
    Evaluate if the webcast page has all the data it needs before a match starts.
    We need team names and player lineups for both teams.
    Returns "Yes", "No", or "N/A".
    """
    if not data:
        return "N/A"
    
    try:
        tm = data.get('tm', {})
        teams = data.get('teams', {})
        
        team1_name, team2_name = _extract_team_names(tm)
        team1_players, team2_players = _extract_team_players(tm, teams)
        
        if not team1_name or not team2_name:
            logger.debug(f"Match {match_id}: Webcast missing team names")
            return "No"
            
        if not team1_players or not team2_players:
            logger.debug(f"Match {match_id}: Webcast missing players for one or both teams")
            return "No"
            
        has_team1_lineup = any(p.get('starter') == 1 for p in team1_players)
        has_team2_lineup = any(p.get('starter') == 1 for p in team2_players)
        
        if has_team1_lineup and has_team2_lineup:
            return "Yes"
        else:
            return "Yes"
            
    except Exception as e:
        logger.error(f"Error evaluating webcast data for match {match_id}: {e}")
        return "N/A"

def _check_basic_stats(data):
    team1_name = team2_name = team1_id = team2_id = None
    
    match_detail = data.get('matchDetail', {}) or {}
    competitors = match_detail.get('competitors', []) or data.get('competitors', [])
    
    if isinstance(competitors, list):
        for comp in competitors:
            if not isinstance(comp, dict): continue
            is_home = comp.get('isHomeCompetitor', 0)
            comp_name = comp.get('competitorName') or comp.get('teamName') or comp.get('teamNameInternational') or comp.get('name')
            t_id = comp.get('teamId') or comp.get('competitorId')
            
            if comp_name:
                if is_home == 1:
                    team1_name = team1_name or comp_name
                    team1_id = team1_id or t_id
                else:
                    team2_name = team2_name or comp_name
                    team2_id = team2_id or t_id
                    
    has_team_names = bool(team1_name and team2_name)
    
    has_time = any(key in match_detail for key in ['matchTime', 'matchTimeUTC']) or \
               any(key in data for key in ['matchTime', 'time', 'date', 'startTime', 'matchDate', 'kickoffTime'])
               
    has_scores = False
    scores_found = []
    
    if isinstance(competitors, list):
        for comp in competitors:
            if not isinstance(comp, dict): continue
            score_str = comp.get('scoreString')
            score_val = comp.get('score')
            if score_str is not None and score_str != '':
                scores_found.append(score_str)
            elif score_val is not None:
                scores_found.append(score_val)
    
    if len(scores_found) >= 2:
        has_scores = True
    else:
        score = data.get('score')
        if isinstance(score, dict):
            has_scores = bool(score.get('home') is not None and score.get('away') is not None)
        elif isinstance(score, str):
            has_scores = bool(score)
        else:
            scores = data.get('scores')
            if isinstance(scores, dict):
                has_scores = bool(scores.get('home') is not None and scores.get('away') is not None)
            elif data.get('homeScore') is not None and data.get('awayScore') is not None:
                has_scores = True
                
    return has_team_names, has_time, has_scores, team1_id, team2_id

def _compare_summary_to_boxscore(data, team1_id, team2_id, unregistered_players):
    actions_valid = True
    actions_issues = []
    
    unreg_by_team = defaultdict(list)
    if unregistered_players:
        for p in unregistered_players:
            if isinstance(p, dict) and p.get('teamId'):
                unreg_by_team[p['teamId']].append(p)
                
    comparison_stats = data.get('comparisonStats', []) or []
    boxscore_home = data.get('boxscore_hometeam', []) or []
    boxscore_away = data.get('boxscore_awayteam', []) or []
    
    team_to_boxscore = {}
    match_detail = data.get('matchDetail', {}) or {}
    competitors = match_detail.get('competitors', []) or []
    if isinstance(competitors, list):
        for comp in competitors:
            if isinstance(comp, dict) and (comp.get('teamId') or comp.get('competitorId')):
                t_id = comp.get('teamId') or comp.get('competitorId')
                is_home = comp.get('isHomeCompetitor', 0)
                if t_id:
                    team_to_boxscore[t_id] = boxscore_home if is_home == 1 else boxscore_away
                    
    if team1_id and team1_id not in team_to_boxscore: team_to_boxscore[team1_id] = boxscore_home
    if team2_id and team2_id not in team_to_boxscore: team_to_boxscore[team2_id] = boxscore_away
    
    if not isinstance(comparison_stats, list): comparison_stats = []
    
    for summary in comparison_stats:
        if not isinstance(summary, dict) or not summary.get('teamId'): continue
        t_id = summary['teamId']
        
        boxscore_players = team_to_boxscore.get(t_id)
        if not boxscore_players:
            if any(p.get('teamId') == t_id for p in boxscore_home if isinstance(p, dict)):
                boxscore_players = boxscore_home
            elif any(p.get('teamId') == t_id for p in boxscore_away if isinstance(p, dict)):
                boxscore_players = boxscore_away
                
        if not boxscore_players: continue
        
        sum_g = summary.get('sGoals', 0)
        sum_yc = summary.get('sYellowCards', 0)
        sum_rc = summary.get('sRedCards', 0)
        
        pl_g = sum(p.get('sGoals', 0) for p in boxscore_players if isinstance(p, dict) and p.get('teamId') == t_id)
        pl_yc = sum(p.get('sYellowCards', 0) for p in boxscore_players if isinstance(p, dict) and p.get('teamId') == t_id)
        pl_rc = sum(p.get('sRedCards', 0) for p in boxscore_players if isinstance(p, dict) and p.get('teamId') == t_id)
        
        unreg_cnt = len(unreg_by_team.get(t_id, []))
        
        if sum_g > pl_g:
            actions_valid = False
            actions_issues.append(f"Team {t_id}: {sum_g} goals vs {pl_g} in boxscore" + (" (unregistered)" if unreg_cnt else ""))
        if sum_yc > pl_yc:
            actions_valid = False
            actions_issues.append(f"Team {t_id}: {sum_yc} yellow cards vs {pl_yc} in boxscore" + (" (unregistered)" if unreg_cnt else ""))
        if sum_rc > pl_rc:
            actions_valid = False
            actions_issues.append(f"Team {t_id}: {sum_rc} red cards vs {pl_rc} in boxscore" + (" (unregistered)" if unreg_cnt else ""))
            
    if not comparison_stats:
        actions_valid = True
        
    return actions_valid, actions_issues, boxscore_home, boxscore_away

def _validate_substitutions(boxscore_home, boxscore_away):
    subs_valid = True
    subs_issues = []
    processed_teams = set()
    
    if not isinstance(boxscore_home, list): boxscore_home = []
    if not isinstance(boxscore_away, list): boxscore_away = []
    
    for boxscore in [boxscore_home, boxscore_away]:
        for p in boxscore:
            if not isinstance(p, dict) or not p.get('teamId') or p['teamId'] in processed_teams: continue
            t_id = p['teamId']
            processed_teams.add(t_id)
            
            subs_off = defaultdict(list)
            subs_on = defaultdict(list)
            
            for player in boxscore:
                if not isinstance(player, dict) or player.get('teamId') != t_id: continue
                off_t = str(player.get('sSubstitutionOffTime', '0')).strip()
                on_t = str(player.get('sSubstitutionOnTime', '0')).strip()
                if off_t and off_t != '0': subs_off[off_t].append(player.get('personId'))
                if on_t and on_t != '0': subs_on[on_t].append(player.get('personId'))
                
            all_times = set(subs_off.keys()) | set(subs_on.keys())
            for t in all_times:
                if len(subs_off[t]) != len(subs_on[t]):
                    subs_valid = False
                    subs_issues.append(f"Team {t_id}: At minute {t}, unbalanced subs")
                    
    return subs_valid, subs_issues

def evaluate_end_game_past_match_data(match_id, data, unregistered_players=None):
    """
    After a match is finished, check if all the data is complete and correct.
    Returns "Yes", "No", or "N/A".
    """
    if not data:
        return "N/A"
    
    try:
        has_team_names, has_time, has_scores, team1_id, team2_id = _check_basic_stats(data)
        actions_valid, actions_issues, boxscore_home, boxscore_away = _compare_summary_to_boxscore(data, team1_id, team2_id, unregistered_players)
        subs_valid, subs_issues = _validate_substitutions(boxscore_home, boxscore_away)
        
        all_issues = actions_issues + subs_issues
        overall_valid = actions_valid and subs_valid
        
        if not has_team_names or not has_time or not has_scores or not overall_valid:
            for issue in all_issues:
                logger.debug(f"Match {match_id} HS JSON Issue: {issue}")
            return "No"
            
        return "Yes"
        
    except Exception as e:
        logger.error(f"Error evaluating HS JSON for match {match_id}: {e}")
        return "N/A"

def format_match_data(match):
    """
    Format match data for output display.
    """
    try:
        # Date
        if match.get('matchTime'):
            try:
                date_obj = datetime.strptime(match['matchTime'], '%Y-%m-%d %H:%M:%S')
                match['date_formatted'] = date_obj.strftime('%d/%m/%Y')
                match['time_local_formatted'] = date_obj.strftime('%H:%M')
            except Exception:
                match['date_formatted'] = ''
                match['time_local_formatted'] = ''
        else:
            match['date_formatted'] = ''
            match['time_local_formatted'] = ''
            
        # UTC Time & Offsets
        if match.get('matchTimeUTC'):
            try:
                utc_time_obj = datetime.strptime(match['matchTimeUTC'], '%Y-%m-%d %H:%M:%S')
                match['time_utc_formatted'] = utc_time_obj.strftime('%H:%M')
                
                tallinn_time = utc_time_obj + timedelta(hours=2)
                match['time_tallinn_formatted'] = tallinn_time.strftime('%H:%M')
                
                medellin_time = utc_time_obj - timedelta(hours=5)
                match['time_medellin_formatted'] = medellin_time.strftime('%H:%M')
            except Exception:
                match['time_utc_formatted'] = ''
                match['time_tallinn_formatted'] = ''
                match['time_medellin_formatted'] = ''
        else:
            match['time_utc_formatted'] = ''
            match['time_tallinn_formatted'] = ''
            match['time_medellin_formatted'] = ''
            
        if match.get('competitor1') and match.get('competitor2'):
            match['game'] = f"{match['competitor1']} vs {match['competitor2']}"
        else:
            match['game'] = 'TBD'
            
        return match
        
    except Exception as e:
        logger.error(f"Error formatting match data: {e}")
        return match
