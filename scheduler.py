from typing import Dict, Any
from cat.log import log
from cat import hook, CheshireCat, run_sync_or_async
from cat.core_plugins.white_rabbit.white_rabbit import WhiteRabbit, JobStatus

from .core.crawler import crawl4ai_setup_command
from .core.processor import process_scrapycat_command


def _get_job_id(cat: CheshireCat) -> str:
    return f"scrapycat_scheduled_scraping:{cat.agent_key}"


def setup_scrapycat_schedule(cheshire_cat: CheshireCat, job_id: str) -> None:
    """Setup or update the ScrapyCat scheduled job based on settings"""
    # Create wrapper function for scheduled execution
    def scheduled_scrapycat_job(user_message: str) -> str:
        """Wrapper function for scheduled ScrapyCat execution"""
        lock_acquired = white_rabbit.acquire_lock(job_id)
        if not lock_acquired:
            return "Skip execution"  # Skip execution if the lock is not acquired (previous job still running)
        try:
            return run_sync_or_async(
                process_scrapycat_command,
                user_message=user_message,
                cat=cheshire_cat,
                scheduled=True,
            )
        finally:
            # release the lock immediately
            white_rabbit.release_lock(job_id)

    # Load ScrapyCat plugin settings
    settings = cheshire_cat.mad_hatter.get_plugin().load_settings()

    try:
        scheduled_command: str = settings.get("scheduled_command", "").strip()
        schedule_hour: int = settings.get("schedule_hour", 3)
        schedule_minute: int = settings.get("schedule_minute", 0)
        
        # If no command is configured, just remove the job and return
        if not scheduled_command:
            log.debug("No scheduled ScrapyCat command configured, job removed")
            return

        # Check if the job is already scheduled
        white_rabbit = WhiteRabbit()
        if white_rabbit.get_job(job_id):
            log.debug(f"Job '{job_id}' already scheduled for CheshireCat '{cheshire_cat.agent_key}'")
            return

        # Schedule the new job: call the wrapper function
        white_rabbit.schedule_cron_job(
            job=scheduled_scrapycat_job,
            job_id=job_id,
            hour=schedule_hour,
            minute=schedule_minute,
            user_message=scheduled_command,
        )
    except Exception as e:
        log.error(f"Failed to setup scheduled ScrapyCat job: {str(e)}")


@hook(priority=9)
def after_cat_bootstrap(cat: CheshireCat) -> None:
    """Hook called at Cat startup to schedule recurring jobs"""
    log.debug("Setting up ScrapyCat scheduled jobs after BillTheLizard bootstrap")

    settings: Dict[str, Any] = cat.mad_hatter.get_plugin().load_settings()

    # The cat parameter here is a CheshireCat
    crawl4ai_setup_command(settings)
    setup_scrapycat_schedule(cat, _get_job_id(cat))


@hook(priority=0)
def after_plugin_settings_update(plugin_id: str, settings: Dict[str, Any], cat: CheshireCat) -> None:
    """Hook called when the plugin settings are updated"""
    if plugin_id != cat.mad_hatter.get_plugin().id:
        return

    # Job ID for the scheduled task
    job_id = _get_job_id(cat)
    white_rabbit = WhiteRabbit()

    while True:
        job = white_rabbit.get_job(job_id)

        # No job found, exit the loop
        if not job:
            break

        # If the job is not running, remove it
        if job.status != JobStatus.RUNNING:
            white_rabbit.remove_job(job_id)
            break

        # If the job is running, wait and check again
        log.debug(f"ScrapyCat scheduled job '{job_id}' is still running, waiting for it to finish...")
        # Wait for a short period before checking again
        import time
        time.sleep(5)
