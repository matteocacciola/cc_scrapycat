import asyncio
import time
from typing import List, Tuple
import urllib.parse
import threading
import requests
from bs4 import BeautifulSoup
from cat.log import log
from cat import StrayCat
from .context import ScrapyCatContext
from ..utils.url_utils import normalize_domain
from ..utils.robots import is_url_allowed_by_robots


# Thread-local storage for session objects
_thread_local = threading.local()


def get_thread_session(user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:55.0) Gecko/20100101 Firefox/55.0") -> requests.Session:
    """Get or create a thread-local requests session for thread-safe parallel requests"""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update({
            "User-Agent": user_agent
        })
    return _thread_local.session


def extract_valid_urls(urls: List[str], page: str, ctx: ScrapyCatContext) -> List[str]:
    """Extract and filter valid URLs from a list of raw URLs"""
    valid_urls: List[str] = []
    for url in urls:
        if "#" in url:
            # Skip anchor links
            continue

        # Handle absolute vs relative URLs correctly
        if url.startswith(("http://", "https://")):
            # URL is already absolute, use it as-is
            new_url: str = url
        else:
            # URL is relative, join with current page
            new_url = urllib.parse.urljoin(page, url)

        parsed_new_url: urllib.parse.ParseResult = urllib.parse.urlparse(new_url)
        new_url_domain: str = normalize_domain(parsed_new_url.netloc)

        # Check if URL is allowed
        is_root_domain: bool = new_url_domain in ctx.root_domains  # Can crawl recursively
        is_allowed_domain: bool = new_url_domain in ctx.allowed_domains  # Single page only
        
        if not (is_root_domain or is_allowed_domain):
            continue

        # Check if URL matches any of the allowed paths (only for root domains)
        if is_root_domain and ctx.allowed_paths:
            path_allowed: bool = any(
                parsed_new_url.path.startswith(allowed_path)
                for allowed_path in ctx.allowed_paths
            )
            if not path_allowed:
                continue
        
        # Skip URLs with GET parameters if configured
        if ctx.skip_get_params and parsed_new_url.query:
            continue

        # Skip URLs with configured file extensions
        if ctx.skip_extensions and new_url.lower().endswith(tuple(ctx.skip_extensions)):
            continue

        # Handle PDFs based on settings
        if new_url.lower().endswith(".pdf"):
            if ctx.ingest_pdf:
                # PDFs should be added to scraped pages but not processed for URL extraction
                should_add_pdf = False
                with ctx.visited_lock:
                    if new_url not in ctx.visited_pages:
                        ctx.visited_pages.add(new_url)
                        should_add_pdf = True
                
                if should_add_pdf:
                    with ctx.scraped_pages_lock:
                        ctx.scraped_pages.append(new_url)
            continue

        valid_urls.append(new_url)
    
    return valid_urls


async def crawl_page(ctx: ScrapyCatContext, cat: StrayCat, page: str, depth: int) -> List[Tuple[str, int]]:
    """Thread-safe page crawling function - now stores content for later sequential ingestion"""
    with ctx.visited_lock:
        if page in ctx.visited_pages:
            return []
        ctx.visited_pages.add(page)
    
    # Check robots.txt compliance for this page
    if not is_url_allowed_by_robots(ctx, page):
        log.info(f"Page blocked by robots.txt, skipping: {page}")
        return []
    
    new_urls: List[Tuple[str, int]] = []
    try:
        # Use thread-local session for fast parallel requests
        session = get_thread_session(ctx.user_agent)
        response = session.get(page, timeout=ctx.page_timeout)
        response_text = response.text

        soup: BeautifulSoup = BeautifulSoup(response_text, "html.parser")
        
        # Store scraped page for later sequential ingestion
        with ctx.scraped_pages_lock:
            ctx.scraped_pages.append(page)
            current_count: int = len(ctx.scraped_pages)
            
        # Send progress update for scraping (only if not scheduled)
        # Done outside the lock to prevent blocking other threads
        # Throttled to avoid flooding the websocket channel
        if not ctx.scheduled:
            should_send = False
            with ctx.update_lock:
                now = time.time()
                if now - ctx.last_update_time > 0.5:  # Update every 0.5 seconds max
                    ctx.last_update_time = now
                    should_send = True
            
            if should_send:
                # Get worker name for debugging
                worker_name = threading.current_thread().name
                # Simplify worker name if it's the standard ThreadPoolExecutor format
                if "ThreadPoolExecutor" in worker_name:
                    try:
                        # Extract just the number if possible, e.g. "ThreadPoolExecutor-0_1" -> "Worker 1"
                        parts = worker_name.split("_")
                        if len(parts) > 1:
                            worker_name = f"Worker {parts[-1]}"
                    except:
                        pass
                
                await cat.notifier.send_ws_message(f"Scraped {current_count} pages - {worker_name} scraping: {page}")
        
        urls: List[str] = [link["href"] for link in soup.select("a[href]")]
        
        # Extract valid URLs using the helper function
        valid_urls: List[str] = extract_valid_urls(urls, page, ctx)

        # Process found URLs: scrape allowed domains immediately, queue root domains for recursion
        recursive_urls: List[str] = []
        for url in valid_urls:
            parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(url)
            url_domain: str = normalize_domain(parsed_url.netloc)
            
            if url_domain in ctx.root_domains:
                # Root domain URL - add for recursive crawling
                recursive_urls.append(url)
            elif url_domain in ctx.allowed_domains:
                # Allowed domain URL - scrape immediately but don't recurse
                should_add = False
                with ctx.visited_lock:
                    if url not in ctx.visited_pages:
                        ctx.visited_pages.add(url)
                        should_add = True
                
                if should_add:
                    # Add to scraped pages for ingestion
                    with ctx.scraped_pages_lock:
                        ctx.scraped_pages.append(url)

        # Batch check for unvisited URLs to reduce lock overhead
        unvisited_urls: List[Tuple[str, int]] = []
        # We don't strictly need the lock here because crawl_page handles the authoritative check
        # This is just a pre-filter to avoid submitting too many duplicate tasks
        for url in recursive_urls:
            if url not in ctx.visited_pages:
                # Only continue recursion if we haven't reached max_depth
                if ctx.max_depth == -1 or (depth + 1) <= ctx.max_depth:
                    unvisited_urls.append((url, depth + 1))
        
        # Add unvisited URLs to new_urls
        new_urls.extend(unvisited_urls)
                            
    except Exception as e:
        log.error(f"Page crawl failed: {page} - {str(e)}")
    
    return new_urls


async def crawler(ctx: ScrapyCatContext, cat: StrayCat, start_urls: list[str]) -> None:
    """Multi-threaded crawler using ThreadPoolExecutor - supporting multiple starting URLs"""
    async def throttled_crawl(url: str, depth: int):
        # Respect max_pages limit before starting
        if ctx.max_pages != -1 and len(ctx.visited_pages) >= ctx.max_pages:
            return
        async with sem:  # Only 'max_workers' can enter this block at once
            try:
                # Add a timeout to the specific page crawl
                new_links = await asyncio.wait_for(
                    crawl_page(ctx, cat, url, depth),
                    timeout=ctx.page_timeout,
                )
                # Process results and schedule new tasks
                for next_url, next_depth in new_links:
                    if (ctx.max_depth == -1 or next_depth <= ctx.max_depth) and next_url not in pending_tasks:
                        pending_tasks.add(next_url)
                        tg.create_task(throttled_crawl(next_url, next_depth))
            except asyncio.TimeoutError:
                log.warning(f"Timeout crawling {url}")
                ctx.failed_pages.append(url)
            except Exception as e:
                log.error(f"Failed to crawl {url}: {e}")
                ctx.failed_pages.append(url)

    # Limit concurrency (replaces max_workers)
    sem = asyncio.Semaphore(ctx.max_workers)

    # 2. Track pending URLs to prevent duplicate tasks
    # (Simplified because we don't need locks in a single-threaded event loop)
    pending_tasks = set()
    try:
        async with asyncio.TaskGroup() as tg:
            for start_url in start_urls:
                pending_tasks.add(start_url)
                tg.create_task(throttled_crawl(start_url, 0))

    except ExceptionGroup as eg:
        # TaskGroup raises ExceptionGroup if multiple tasks fail
        log.error(f"Crawl finished with errors: {eg}")

    log.info(f"Crawl complete. Visited: {len(ctx.visited_pages)}")