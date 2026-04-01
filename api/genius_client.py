import logging
import aiohttp
import asyncio
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import json
import os

logger = logging.getLogger(__name__)

LEAGUE_CACHE_PATH = os.getenv("LEAGUE_CACHE_PATH", "league_cache.json")

def load_league_cache():
    if os.path.exists(LEAGUE_CACHE_PATH):
        try:
            with open(LEAGUE_CACHE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load league cache from {LEAGUE_CACHE_PATH}: {e}")
    return {}

def save_league_cache(cache_data):
    try:
        # Only save strings, not asyncio.Tasks
        serializable_cache = {k: v for k, v in cache_data.items() if isinstance(v, str)}
        with open(LEAGUE_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump(serializable_cache, f)
    except Exception as e:
        logger.warning(f"Failed to save league cache to {LEAGUE_CACHE_PATH}: {e}")

# Caches to avoid duplicate API calls
_league_details_cache = load_league_cache()
_match_details_cache = {}

# Leagues where the API doesn't return an abbreviation
LEAGUE_ABBREV_OVERRIDE = {
    61: "MFL",
    13: "LFF",
    31: "CPL",
    101: "CRC",
    56: "VPF",
    64: "LDF",
    37: "ESFL",
    43: "LNFPH",
    91: "MNL",
    77: "IFA",
    58: "GOAFA",
    95: "JPL",
    79: "FPNMF",
    42: "CTFA",
    72: "COS",
}

class GeniusAPIError(Exception):
    pass

class GeniusClient:
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = None
        
    async def init_session(self):
        if self.session is None:
            connector = aiohttp.TCPConnector(limit=100)
            self.session = aiohttp.ClientSession(connector=connector)
            
    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        reraise=True
    )
    async def _get(self, url, params=None, timeout=30):
        """Internal method to make GET requests with retry logic."""
        if params is None:
            params = {}
        if 'ak' not in params and self.api_key:
            params['ak'] = self.api_key

        if self.session is None:
            await self.init_session()

        try:
            async with self.session.get(url, params=params, timeout=timeout) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as e:
            status_code = e.status
            if status_code in (429, 500, 502, 503, 504):
                logger.warning(f"Retryable HTTP error {status_code} for {url}: {e}")
                raise  # Let tenacity retry
            else:
                logger.error(f"Non-retryable HTTP error {status_code} for {url}: {e}")
                return None # Return None for 403, 404, etc.
        except aiohttp.ClientError as e:
            logger.warning(f"Request error for {url}: {e}")
            raise  # Let tenacity retry
        except asyncio.TimeoutError as e:
            logger.warning(f"Timeout error for {url}: {e}")
            raise

    async def fetch_league_details(self, league_id):
        """Fetch league abbreviation."""
        if not league_id:
            return None
        
        cache_key = f"{league_id}"
        if cache_key in _league_details_cache:
            task = _league_details_cache[cache_key]
            if isinstance(task, asyncio.Task):
                return await task
            return task
            
        async def _fetch():
            try:
                lid = int(league_id)
                if lid in LEAGUE_ABBREV_OVERRIDE:
                    return LEAGUE_ABBREV_OVERRIDE[lid]
            except (TypeError, ValueError):
                pass
            
            url = f"https://api.wh.geniussports.com/v1/football/leagues/{league_id}"
            try:
                data = await self._get(url)
                if data:
                    league_data = data.get('response', {}).get('data') or data.get('response') or data
                    if not isinstance(league_data, dict):
                        league_data = {}
                    league_abbrev = (
                        league_data.get('leagueAbbrev') or league_data.get('abbreviation') or
                        league_data.get('federationCode') or league_data.get('code') or league_data.get('slug')
                    )
                    if not league_abbrev:
                        try:
                            lid = int(league_id)
                            if lid in LEAGUE_ABBREV_OVERRIDE:
                                league_abbrev = LEAGUE_ABBREV_OVERRIDE[lid]
                        except (TypeError, ValueError):
                            pass
                    return league_abbrev
            except Exception as e:
                logger.error(f"Error fetching league details for {league_id}: {e}")
                
            try:
                lid = int(league_id)
                if lid in LEAGUE_ABBREV_OVERRIDE:
                    return LEAGUE_ABBREV_OVERRIDE[lid]
            except (TypeError, ValueError):
                pass
            return None

        task = asyncio.create_task(_fetch())
        _league_details_cache[cache_key] = task
        result = await task
        _league_details_cache[cache_key] = result
        if result is not None:
            save_league_cache(_league_details_cache)
        return result

    async def fetch_competition_info(self, competition_id):
        """Fetch minimal match data for a competition to resolve its metadata."""
        url = f"https://api.wh.geniussports.com/v1/football/competitions/{competition_id}/matches"
        params = {'limit': 1}
        try:
            data = await self._get(url, params=params)
            if data and 'response' in data and 'data' in data['response']:
                matches = data['response']['data']
                if matches:
                    m = matches[0]
                    return {
                        'id': competition_id,
                        'name': m.get('competitionName', '') or m.get('competitionNameInternational', ''),
                        'league_id': m.get('leagueId', 0),
                        'league_name': m.get('leagueName', '') or m.get('leagueNameInternational', ''),
                    }
            return None
        except Exception as e:
            logger.error(f"Error fetching competition info for {competition_id}: {e}")
            return None

    async def fetch_competitions_for_league(self, league_id):
        url = f"https://api.wh.geniussports.com/v1/football/leagues/{league_id}/competitions"
        params = {'limit': 100}
        try:
            data = await self._get(url, params=params)
            if data and 'response' in data and 'data' in data['response']:
                return data['response']['data']
            return []
        except Exception as e:
            logger.error(f"Error fetching competitions for league {league_id}: {e}")
            return []

    async def fetch_matches_for_competition(self, competition_id, from_date, to_date):
        url = f"https://api.wh.geniussports.com/v1/football/competitions/{competition_id}/matches"
        params = {
            'fromDate': from_date,
            'toDate': to_date,
            'limit': 500
        }
        try:
            data = await self._get(url, params=params)
            if data and 'response' in data and 'data' in data['response']:
                return data['response']['data']
            return []
        except Exception as e:
            logger.error(f"Error fetching matches for competition {competition_id}: {e}")
            return []

    async def fetch_match_details(self, match_id):
        cache_key = f"{match_id}"
        if cache_key in _match_details_cache:
            task = _match_details_cache[cache_key]
            if isinstance(task, asyncio.Task):
                return await task
            return task
        
        async def _fetch():
            url = f"https://api.wh.geniussports.com/v1/football/matches/{match_id}"
            try:
                data = await self._get(url)
                if data:
                    match_data = None
                    if 'response' in data:
                        if 'data' in data['response']:
                            match_data = data['response']['data']
                        else:
                            match_data = data['response']
                    else:
                        match_data = data
                    
                    if match_data:
                        return match_data
                return None
            except Exception as e:
                logger.error(f"Error fetching match details for {match_id}: {e}")
                return None

        task = asyncio.create_task(_fetch())
        _match_details_cache[cache_key] = task
        result = await task
        _match_details_cache[cache_key] = result
        return result

    async def check_publish_connection(self, match_id):
        url = "https://api.wh.geniussports.com/v1/football/connections"
        params = {
            'matchId': match_id,
            'type': 'publish'
        }
        try:
            data = await self._get(url, params=params)
            if data:
                connections_data = []
                if 'response' in data:
                    if 'data' in data['response']:
                        connections_data = data['response']['data']
                    else:
                        connections_data = []
                else:
                    connections_data = []
                
                if isinstance(connections_data, list) and len(connections_data) >= 1:
                    return "Yes"
                else:
                    return "No"
            return "N/A"
        except aiohttp.ClientResponseError as e:
            if e.status in (403, 404):
                logger.debug(f"Publish connection info not accessible/found for match {match_id} (HTTP {e.status})")
            else:
                logger.error(f"Error checking publish connection for {match_id}: {e}")
            return "N/A"
        except Exception as e:
            logger.error(f"Error checking publish connection for {match_id}: {e}")
            return "N/A"

    async def fetch_unregistered_players(self, match_id):
        url = f"https://api.wh.geniussports.com/v1/football/matches/{match_id}/persons/unregistered"
        try:
            data = await self._get(url)
            if data:
                unregistered_players = []
                if 'response' in data and 'data' in data['response']:
                    unregistered_players = data['response']['data']
                elif 'data' in data:
                    unregistered_players = data['data']
                
                return unregistered_players if isinstance(unregistered_players, list) else []
            return []
        except aiohttp.ClientResponseError as e:
            if e.status in (403, 404):
                logger.debug(f"Unregistered players not accessible/found for match {match_id} (HTTP {e.status})")
            else:
                logger.error(f"Error fetching unregistered players for {match_id}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching unregistered players for {match_id}: {e}")
            return []

    # Unauthenticated endpoints
    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    async def fetch_webcast_json(self, match_id):
        url = f"https://livestats.dcd.shared.geniussports.com/data/football/{match_id}/data.json"
        if self.session is None:
            await self.init_session()
        try:
            async with self.session.get(url, timeout=30) as response:
                logger.info(f"Webcast fetch for match {match_id}: HTTP {response.status}")
                if response.status == 403:
                    return None
                elif response.status == 404:
                    return None
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as e:
            logger.info(f"Webcast fetch error for match {match_id}: HTTP {e.status} {e.message}")
            if e.status in (429, 500, 502, 503, 504):
                raise
            return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.info(f"Webcast fetch error for match {match_id}: {type(e).__name__}: {e}")
            raise
        except Exception as e:
            logger.info(f"Webcast fetch error for match {match_id}: {type(e).__name__}: {e}")
            raise

    @retry(
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(3),
        reraise=True
    )
    async def fetch_hs_summary_json(self, fed_slug, match_id):
        url = f"https://hosted.dcd.shared.geniussports.com/{fed_slug}/en/match/{match_id}/summary?json=1"
        if self.session is None:
            await self.init_session()
        try:
            async with self.session.get(url, timeout=30) as response:
                if response.status == 404:
                    logger.debug(f"HS JSON not found for match {match_id} (HTTP 404). Tried: {url}")
                    return None
                elif response.status == 403:
                    logger.debug(f"HS JSON not accessible for match {match_id} (HTTP 403)")
                    return None
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientResponseError as e:
            if e.status in (429, 500, 502, 503, 504):
                raise
            return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug(f"Error fetching HS summary JSON for {match_id}: {e}")
            raise
        except Exception as e:
            logger.debug(f"Error fetching HS summary JSON for {match_id}: {e}")
            raise
