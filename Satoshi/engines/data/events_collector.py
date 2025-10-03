"""
Off-Chain Events Collector Agent

Mission: Unlock calendars, governance proposals, exchange status, GitHub releases.

Outputs: raw_data.events.calendar.

SLO: event→bus lag p95 < 5 minutes (real-time for governance, best-effort for others).
"""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime, timezone
import time
import hashlib
from collections import defaultdict, deque
import os
import json

logger = logging.getLogger(__name__)

# =============================
# HELPERS
# =============================

def _safe_decimal(val):
    if val is None or val == '' or val == 'None':
        return None
    try:
        return Decimal(str(val))
    except Exception:
        return None

def _safe_int(val):
    try:
        return int(val)
    except Exception:
        return 0

def _normalize_timestamp(ts, default_now_us):
    # Accepts int (ms/s/us), float, or ISO string
    if ts is None:
        return default_now_us
    if isinstance(ts, int):
        # Heuristic: ns >=1e18, us >=1e15, ms >=1e12, s >=1e9 else
        if ts >= 1_000_000_000_000_000_000:
            return ts // 1000  # ns to us
        elif ts >= 1_000_000_000_000_000:
            return ts  # already us
        elif ts >= 1_000_000_000_000:
            return ts * 1000  # ms to us
        elif ts >= 1_000_000_000:
            return ts * 1_000_000  # s to us
        else:
            return ts * 1_000_000  # s to us (fallback)
    if isinstance(ts, float):
        return int(ts * 1_000_000)
    if isinstance(ts, str):
        try:
            # Try ISO string - force UTC for naive timestamps
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)
        except Exception:
            try:
                return int(ts)
            except Exception:
                return default_now_us
    return default_now_us

# =============================
# DATA STRUCTURES
# =============================

@dataclass
class CalendarEvent:
    event_type: str  # e.g. 'governance_proposal', 'token_unlock', 'exchange_maintenance', 'github_release'
    title: str
    description: Optional[str] = None
    start_time_utc_us: int = 0
    end_time_utc_us: Optional[int] = None
    source: str = ""  # e.g. 'snapshot', 'compound', 'binance', 'github'
    source_id: str = ""  # external ID from source
    status: str = "active"  # 'active', 'completed', 'cancelled'
    metadata: Optional[Dict[str, Any]] = field(default_factory=lambda: {})
    capture_timestamp_utc_us: int = 0

    def get_hash(self) -> str:
        h = f"{self.event_type}:{self.source}:{self.source_id}:{self.start_time_utc_us}"
        return hashlib.md5(h.encode()).hexdigest()

# =============================
# DUPLICATE DETECTOR
# =============================

class DuplicateDetector:
    def __init__(self, window_size: int = 10000):
        self.window_size = window_size
        # For each stream, keep a dict of key -> (deque, set)
        self.seen: Dict[str, Dict[str, tuple]] = defaultdict(dict)

    def is_duplicate(self, data_type: str, data_hash: str, key: str = "default") -> bool:
        # key can be source, event_type, etc. for per-stream dedup
        if key not in self.seen[data_type]:
            self.seen[data_type][key] = (deque(), set())
        dq, st = self.seen[data_type][key]
        if data_hash in st:
            return True
        dq.append(data_hash)
        st.add(data_hash)
        while len(dq) > self.window_size:
            evicted = dq.popleft()
            st.discard(evicted)
        return False

# =============================
# HTTP CLIENT
# =============================

class HttpClient:
    """Generic HTTP client for various APIs."""
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session: aiohttp.ClientSession | None = None

    async def __aenter__(self):
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(limit_per_host=5)
            timeout = aiohttp.ClientTimeout(total=30)
            headers = {
                'User-Agent': 'EventsCollector/1.0 (aiohttp)'
            }
            if self.api_key:
                headers['Authorization'] = f'Bearer {self.api_key}'
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=headers
            )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def post_json(self, url: str, json_data: dict, headers: Dict[str, str] = {}) -> Optional[dict]:
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.post(url, json=json_data, headers=headers, timeout=timeout) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning(f"HTTP 429, sleeping {retry_after}s for {url}")
                    await asyncio.sleep(retry_after)
                    return None
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error({"event":"http_error","url":url,"status":resp.status,"body":body[:300]})
                    return None
                try:
                    return await resp.json()
                except Exception:
                    body = await resp.text()
                    logger.warning(f"Non-JSON response from POST {url}: {body[:300]}")
                    return None
        except Exception as e:
            logger.warning(f"HTTP error for {url}: {e}")
            return None

    async def get_json_with_headers(self, url: str, headers: Dict[str, str] = {}) -> tuple[Optional[dict], Dict[str, str]]:
        """Returns (json_data, response_headers) for ETag handling."""
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                resp_headers = dict(resp.headers)
                if resp.status == 304:
                    # Not Modified - return None data but headers
                    return None, resp_headers
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning(f"HTTP 429, sleeping {retry_after}s for {url}")
                    await asyncio.sleep(retry_after)
                    return None, {}
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error({"event":"http_error","url":url,"status":resp.status,"body":body[:300]})
                    return None, {}
                try:
                    json_data = await resp.json()
                    return json_data, resp_headers
                except Exception:
                    body = await resp.text()
                    logger.warning(f"Non-JSON response from {url}: {body[:300]}")
                    return None, {}
        except Exception as e:
            logger.warning(f"HTTP error for {url}: {e}")
            return None, {}

    async def get_json(self, url: str, headers: Dict[str, str] = {}) -> Optional[dict]:
        assert self.session is not None, "aiohttp session is not initialized"
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with self.session.get(url, headers=headers, timeout=timeout) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "5"))
                    logger.warning(f"HTTP 429, sleeping {retry_after}s for {url}")
                    await asyncio.sleep(retry_after)
                    return None
                if resp.status >= 400:
                    body = await resp.text()
                    logger.error({"event":"http_error","url":url,"status":resp.status,"body":body[:300]})
                    return None
                return await resp.json()
        except Exception as e:
            logger.warning(f"HTTP error for {url}: {e}")
            return None

# =============================
# MAIN EVENTS COLLECTOR AGENT
# =============================

class EventsCollectorAgent:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.duplicate_detector = DuplicateDetector()
        self.output_queues: Dict[str, asyncio.Queue] = {
            'calendar': asyncio.Queue(maxsize=config.get('calendar_queue_size', 5000)),
        }
        self.running = False
        self.tasks: List[asyncio.Task] = []
        # ETag cache for GitHub API
        self.etag_by_repo: Dict[str, str] = {}
        # Status tracking for event updates
        self.last_status: Dict[tuple, str] = {}  # (source, source_id) -> status

    async def start(self):
        logger.info("Starting Events Collector Agent...")
        self.running = True
        # Start all collection tasks
        if self.config.get('governance_enabled', True):
            self.tasks.append(asyncio.create_task(self._collect_governance()))
        if self.config.get('token_unlocks_enabled', True):
            self.tasks.append(asyncio.create_task(self._collect_token_unlocks()))
        if self.config.get('exchange_status_enabled', True):
            self.tasks.append(asyncio.create_task(self._collect_exchange_status()))
        if self.config.get('github_releases_enabled', True):
            self.tasks.append(asyncio.create_task(self._collect_github_releases()))
        logger.info(f"Started {len(self.tasks)} collection tasks")

    async def stop(self):
        logger.info("Stopping Events Collector Agent...")
        self.running = False
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        logger.info("Events Collector Agent stopped")

    async def _collect_governance(self):
        """Collect governance proposals from Snapshot, Compound, etc."""
        interval_sec = self.config.get('governance_interval_sec', 300)  # 5 min
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                # Snapshot governance
                snapshot_spaces = self.config.get('snapshot_spaces', [])
                async with HttpClient() as client:
                    # Parallelize snapshot queries
                    tasks = []
                    for space in snapshot_spaces:
                        query = {
                            "query": """
                            query Proposals($space: String!, $first: Int!) {
                              proposals(
                                first: $first
                                where: { space: $space }
                                orderBy: "created"
                                orderDirection: desc
                              ) {
                                id
                                title
                                body
                                start
                                end
                                state
                                author
                                space { id name }
                              }
                            }
                            """,
                            "variables": {"space": space, "first": 20}
                        }
                        headers = {"Content-Type": "application/json"}
                        tasks.append(client.post_json("https://hub.snapshot.org/graphql", query, headers))
                    
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    for space, result in zip(snapshot_spaces, results):
                        try:
                            if isinstance(result, Exception):
                                logger.error({"event": "snapshot_fetch_error", "space": space, "error": str(result)})
                                continue
                            
                            if not result or not isinstance(result, dict):
                                continue
                                
                            # Guard for GraphQL error payloads
                            if "errors" in result:
                                logger.error({"event": "graphql_errors", "space": space, "errors": str(result['errors'])[:300]})
                                continue
                            
                            # Guard for partial data shapes under outages
                            data_section = result.get("data")
                            if not data_section or not isinstance(data_section, dict):
                                logger.warning({"event": "partial_data", "space": space, "issue": "missing_data_section"})
                                continue
                                
                            proposals = data_section.get("proposals")
                            if proposals is None:
                                logger.warning({"event": "partial_data", "space": space, "issue": "missing_proposals_field"})
                                continue
                                
                            if not isinstance(proposals, list):
                                logger.warning({"event": "partial_data", "space": space, "issue": "proposals_not_list", "type": str(type(proposals))})
                                continue
                            
                            for proposal in proposals:
                                try:
                                    event = CalendarEvent(
                                        event_type="governance_proposal",
                                        title=proposal["title"],
                                        description=proposal.get("body", "")[:500],  # Truncate
                                        start_time_utc_us=_normalize_timestamp(proposal["start"], now),
                                        end_time_utc_us=_normalize_timestamp(proposal["end"], None),
                                        source="snapshot",
                                        source_id=proposal["id"],
                                        status=proposal["state"],
                                        metadata={
                                            "space": proposal["space"]["name"],
                                            "author": proposal["author"]
                                        },
                                        capture_timestamp_utc_us=capture_now
                                    )
                                    
                                    # Check for status updates
                                    status_key = ("snapshot", proposal["id"])
                                    last_status = self.last_status.get(status_key)
                                    is_status_update = last_status and last_status != proposal["state"]
                                    
                                    if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key=f"snapshot_{space}"):
                                        # Update status tracking
                                        self.last_status[status_key] = proposal["state"]
                                        
                                        q = self.output_queues['calendar']
                                        if q.full():
                                            try: q.get_nowait()
                                            except asyncio.QueueEmpty: pass
                                        try: q.put_nowait(event)
                                        except asyncio.QueueFull: pass
                                except Exception as row_exc:
                                    logger.warning(f"Bad governance proposal: {row_exc} | {str(proposal)[:300]}")
                                    continue
                                    
                        except Exception as e:
                            logger.error(f"Error processing governance from {space}: {e}")
                            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in governance collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _collect_token_unlocks(self):
        """Collect token unlock events from various sources."""
        interval_sec = self.config.get('token_unlocks_interval_sec', 3600)  # 1 hour
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                # Example: Token unlocks from a hypothetical API
                unlock_api_url = self.config.get('token_unlocks_api_url')
                if unlock_api_url:
                    async with HttpClient() as client:
                        data = await client.get_json(unlock_api_url)
                        if data and "unlocks" in data:
                            for unlock in data["unlocks"]:
                                try:
                                    event = CalendarEvent(
                                        event_type="token_unlock",
                                        title=f"{unlock.get('token', 'Token')} Unlock",
                                        description=f"Unlock of {unlock.get('amount', 'unknown')} tokens",
                                        start_time_utc_us=_normalize_timestamp(unlock.get("unlock_date"), now),
                                        source="token_unlocks_api",
                                        source_id=unlock.get("id", ""),
                                        status="active",
                                        metadata={
                                            "token": unlock.get("token"),
                                            "amount": unlock.get("amount"),
                                            "recipient": unlock.get("recipient")
                                        },
                                        capture_timestamp_utc_us=capture_now
                                    )
                                    
                                    if not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="token_unlocks"):
                                        q = self.output_queues['calendar']
                                        if q.full():
                                            try: q.get_nowait()
                                            except asyncio.QueueEmpty: pass
                                        try: q.put_nowait(event)
                                        except asyncio.QueueFull: pass
                                except Exception as row_exc:
                                    logger.warning(f"Bad token unlock: {row_exc} | {str(unlock)[:300]}")
                                    continue
                                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in token unlocks collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _collect_exchange_status(self):
        """Collect exchange maintenance and status events."""
        interval_sec = self.config.get('exchange_status_interval_sec', 600)  # 10 min
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                # Exchange status from multiple sources
                exchanges = []
                
                # Binance system status
                if self.config.get('binance_status_enabled', False):
                    exchanges.append(('binance', 'https://www.binance.com/bapi/composite/v1/public/cms/article/list/query?type=1&pageSize=20'))
                
                # Coinbase status
                if self.config.get('coinbase_status_enabled', False):
                    exchanges.append(('coinbase', 'https://status.coinbase.com/api/v2/incidents.json'))
                
                # Add more exchanges as needed
                # if self.config.get('kraken_status_enabled', False):
                #     exchanges.append(('kraken', 'https://status.kraken.com/api/v2/incidents.json'))
                
                if exchanges:
                    async with HttpClient() as client:
                        # Parallelize exchange status queries
                        tasks = []
                        for exchange, url in exchanges:
                            tasks.append((exchange, client.get_json(url)))
                        
                        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
                        
                        for (exchange, _), result in zip(tasks, results):
                            try:
                                if isinstance(result, Exception):
                                    logger.error(f"Error fetching {exchange} status: {result}")
                                    continue
                                
                                if not result or not isinstance(result, dict):
                                    continue
                                
                                # Process based on exchange type
                                if exchange == 'binance' and "data" in result and "catalogs" in result["data"]:
                                    for article in result["data"]["catalogs"]:
                                        try:
                                            event = CalendarEvent(
                                                event_type="exchange_maintenance",
                                                title=article.get("title", "Binance Update"),
                                                description=article.get("summary", "")[:500],
                                                start_time_utc_us=_normalize_timestamp(article.get("releaseDate"), now),
                                                source="binance",
                                                source_id=str(article.get("id", "")),
                                                status="active",
                                                metadata={
                                                    "exchange": "binance",
                                                    "type": article.get("type")
                                                },
                                                capture_timestamp_utc_us=capture_now
                                            )
                                            
                                            # Check for status updates
                                            status_key = ("binance", str(article.get("id", "")))
                                            last_status = self.last_status.get(status_key)
                                            is_status_update = last_status and last_status != "active"
                                            
                                            if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="binance_status"):
                                                # Update status tracking
                                                self.last_status[status_key] = "active"
                                                
                                                q = self.output_queues['calendar']
                                                if q.full():
                                                    try: q.get_nowait()
                                                    except asyncio.QueueEmpty: pass
                                                try: q.put_nowait(event)
                                                except asyncio.QueueFull: pass
                                        except Exception as row_exc:
                                            logger.warning(f"Bad {exchange} status: {row_exc} | {str(article)[:300]}")
                                            continue
                                
                                elif exchange == 'coinbase' and "incidents" in result:
                                    for incident in result["incidents"]:
                                        try:
                                            event = CalendarEvent(
                                                event_type="exchange_maintenance",
                                                title=incident.get("name", "Coinbase Incident"),
                                                description=incident.get("body", "")[:500],
                                                start_time_utc_us=_normalize_timestamp(incident.get("created_at"), now),
                                                source="coinbase",
                                                source_id=incident.get("id", ""),
                                                status=incident.get("status", "unknown"),
                                                metadata={
                                                    "exchange": "coinbase",
                                                    "impact": incident.get("impact"),
                                                    "shortlink": incident.get("shortlink")
                                                },
                                                capture_timestamp_utc_us=capture_now
                                            )
                                            
                                            # Check for status updates
                                            status_key = ("coinbase", incident.get("id", ""))
                                            last_status = self.last_status.get(status_key)
                                            current_status = incident.get("status", "unknown")
                                            is_status_update = last_status and last_status != current_status
                                            
                                            if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key="coinbase_status"):
                                                # Update status tracking
                                                self.last_status[status_key] = current_status
                                                
                                                q = self.output_queues['calendar']
                                                if q.full():
                                                    try: q.get_nowait()
                                                    except asyncio.QueueEmpty: pass
                                                try: q.put_nowait(event)
                                                except asyncio.QueueFull: pass
                                        except Exception as row_exc:
                                            logger.warning(f"Bad {exchange} incident: {row_exc} | {str(incident)[:300]}")
                                            continue
                                            
                            except Exception as e:
                                logger.error(f"Error processing {exchange} status: {e}")
                                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in exchange status collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def _collect_github_releases(self):
        """Collect GitHub releases for important repos."""
        interval_sec = self.config.get('github_releases_interval_sec', 1800)  # 30 min
        github_token = self.config.get('github_token') or os.environ.get('GITHUB_TOKEN')
        next_tick = time.monotonic()
        while self.running:
            try:
                now = int(time.time() * 1_000_000)
                capture_now = int(time.time() * 1_000_000)
                
                repos = self.config.get('github_repos', [])  # e.g. ['ethereum/go-ethereum', 'compound-finance/compound-protocol']
                
                async with HttpClient() as client:
                    # Parallelize GitHub repo queries
                    tasks = []
                    for repo in repos:
                        url = f"https://api.github.com/repos/{repo}/releases?per_page=10"
                        headers = {
                            'Accept': 'application/vnd.github+json'
                        }
                        if github_token:
                            headers['Authorization'] = f'token {github_token}'
                        
                        # Add ETag for caching
                        etag = self.etag_by_repo.get(repo)
                        if etag:
                            headers['If-None-Match'] = etag
                            
                        tasks.append((repo, client.get_json_with_headers(url, headers)))
                    
                    # Execute all requests in parallel
                    repo_tasks = [(repo, task) for repo, task in tasks]
                    results = await asyncio.gather(*[task for _, task in repo_tasks], return_exceptions=True)
                    
                    for (repo, _), result in zip(repo_tasks, results):
                        try:
                            if isinstance(result, Exception):
                                logger.error(f"Error fetching GitHub repo {repo}: {result}")
                                continue
                                
                            if not result or not isinstance(result, tuple) or len(result) != 2:
                                continue
                                
                            data, resp_headers = result
                            
                            # Update ETag cache (case-insensitive)
                            etag_value = None
                            for key, value in resp_headers.items():
                                if key.lower() == 'etag':
                                    etag_value = value
                                    break
                            if etag_value:
                                self.etag_by_repo[repo] = etag_value
                            
                            if not data:
                                # Could be 304 Not Modified, which is fine
                                continue
                                
                            if not isinstance(data, list):
                                logger.warning(f"Unexpected GitHub API response format for {repo}: {str(data)[:300]}")
                                continue
                                
                            for release in data:
                                try:
                                    event = CalendarEvent(
                                        event_type="github_release",
                                        title=f"{repo}: {release.get('name', release.get('tag_name', 'Release'))}",
                                        description=release.get("body", "")[:500],
                                        start_time_utc_us=_normalize_timestamp(release.get("published_at"), now),
                                        source="github",
                                        source_id=str(release.get("id", "")),
                                        status="completed" if not release.get("draft") else "draft",
                                        metadata={
                                            "repo": repo,
                                            "tag_name": release.get("tag_name"),
                                            "prerelease": release.get("prerelease", False),
                                            "author": release.get("author", {}).get("login")
                                        },
                                        capture_timestamp_utc_us=capture_now
                                    )
                                    
                                    # Check for status updates
                                    status_key = ("github", str(release.get("id", "")))
                                    last_status = self.last_status.get(status_key)
                                    current_status = "completed" if not release.get("draft") else "draft"
                                    is_status_update = last_status and last_status != current_status
                                    
                                    if is_status_update or not self.duplicate_detector.is_duplicate('calendar', event.get_hash(), key=f"github_{repo}"):
                                        # Update status tracking
                                        self.last_status[status_key] = current_status
                                        
                                        q = self.output_queues['calendar']
                                        if q.full():
                                            try: q.get_nowait()
                                            except asyncio.QueueEmpty: pass
                                        try: q.put_nowait(event)
                                        except asyncio.QueueFull: pass
                                except Exception as row_exc:
                                    logger.warning(f"Bad GitHub release: {row_exc} | {str(release)[:300]}")
                                    continue
                                    
                        except Exception as e:
                            logger.error(f"Error processing GitHub releases from {repo}: {e}")
                            
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in GitHub releases collection: {e}")
            
            # Drift-free cadence
            next_tick += interval_sec
            sleep_for = max(0, next_tick - time.monotonic())
            await asyncio.sleep(sleep_for)

    async def get_output_data(self, data_type: str, timeout: float = 1.0) -> Optional[Any]:
        try:
            return await asyncio.wait_for(self.output_queues[data_type].get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

# =============================
# EXAMPLE USAGE
# =============================

async def main():
    config = {
        'governance_interval_sec': 300,
        'token_unlocks_interval_sec': 3600,
        'exchange_status_interval_sec': 600,
        'github_releases_interval_sec': 1800,
        'snapshot_spaces': ['compound', 'aave.eth', 'uniswap', 'ens.eth'],
        'github_repos': ['ethereum/go-ethereum', 'compound-finance/compound-protocol', 'Uniswap/v3-core'],
        'binance_status_enabled': True,
        'calendar_queue_size': 5000
    }
    logging.basicConfig(level=logging.INFO)
    agent = EventsCollectorAgent(config)
    try:
        await agent.start()
        while True:
            event = await agent.get_output_data('calendar', timeout=5.0)
            if event:
                print(f"Event: {event.event_type} | {event.title} | {event.source}")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        await agent.stop()

if __name__ == "__main__":
    asyncio.run(main())
