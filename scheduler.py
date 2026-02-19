import time
from typing import Dict, Any
from cat.log import log
from cat import hook, CheshireCat, run_sync_or_async, BillTheLizard
from cat.core_plugins.white_rabbit.white_rabbit import JobStatus

from .core.crawler import crawl4ai_setup_command
from .core.processor import process_scrapycat_command


def _get_job_id(cat: CheshireCat) -> str:
    return f"scrapycat_scheduled_scraping:{cat.agent_key}"


# IMPORTANT: This function MUST live at a module level (not inside another function) so that APScheduler + Redis can
# pickle/serialize it by its fully qualified import path.
# All runtime context is passed explicitly via kwargs.
def scheduled_scrapycat_job(user_message: str, agent_key: str, scheduled_job_id: str) -> str:
    """
    Module-level wrapper executed by APScheduler on its cron schedule.

    Args:
        user_message (str): The scraping command to run.
        agent_key (str): Key used to retrieve the correct CheshireCat instance.
        scheduled_job_id (str): The job id, used for distributed locking.
    """
    lizard = BillTheLizard()

    white_rabbit = lizard.white_rabbit
    cheshire_cat = lizard.get_cheshire_cat(agent_key)

    lock_acquired = white_rabbit.acquire_lock(scheduled_job_id)
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
        white_rabbit.release_lock(scheduled_job_id)


def setup_scrapycat_schedule(cheshire_cat: CheshireCat, job_id: str) -> None:
    """Setup or update the ScrapyCat scheduled cron job based on current settings."""
    settings = cheshire_cat.mad_hatter.get_plugin().load_settings()

    try:
        scheduled_command: str = settings.get("scheduled_command", "").strip()
        schedule_hour: int = settings.get("schedule_hour", 3)
        schedule_minute: int = settings.get("schedule_minute", 0)

        # If no command is configured, just remove the job and return
        if not scheduled_command:
            log.debug("No scheduled ScrapyCat command configured, skipping job setup")
            return

        lizard = BillTheLizard()

        # Avoid adding the same job twice
        if lizard.white_rabbit.get_job(job_id):
            log.debug(f"Job '{job_id}' already scheduled for CheshireCat '{cheshire_cat.agent_key}'")
            return

        lizard.white_rabbit.schedule_cron_job(
            job=scheduled_scrapycat_job,
            job_id=job_id,
            hour=schedule_hour,
            minute=schedule_minute,
            user_message=scheduled_command,
            agent_key=cheshire_cat.agent_key,
            scheduled_job_id=job_id,
        )

        log.info(f"ScrapyCat cron job '{job_id}' scheduled at {schedule_hour:02d}:{schedule_minute:02d} UTC")
    except Exception as e:
        log.error(f"Failed to setup scheduled ScrapyCat job: {str(e)}")


@hook(priority=9)
def after_cat_bootstrap(cat: CheshireCat) -> None:
    """Hook called at Cat startup to schedule recurring jobs."""
    log.debug("Setting up ScrapyCat scheduled jobs after BillTheLizard bootstrap")

    settings: Dict[str, Any] = cat.mad_hatter.get_plugin().load_settings()

    crawl4ai_setup_command(settings)
    setup_scrapycat_schedule(cat, _get_job_id(cat))


@hook(priority=0)
def after_plugin_settings_update(plugin_id: str, settings: Dict[str, Any], cat: CheshireCat) -> None:
    """Hook called when plugin settings are updated — replaces the cron job with the new config."""
    if plugin_id != cat.mad_hatter.get_plugin().id:
        return

    job_id = _get_job_id(cat)
    lizard = BillTheLizard()

    # Wait for any currently-running execution to finish before replacing the job
    while True:
        job = lizard.white_rabbit.get_job(job_id)

        if not job:
            break  # No job exists, nothing to remove

        if job.status != JobStatus.RUNNING:
            lizard.white_rabbit.remove_job(job_id)
            break

        log.debug(f"ScrapyCat job '{job_id}' is still running, waiting before replacing...")
        time.sleep(5)

    # Schedule a fresh job with the updated settings
    setup_scrapycat_schedule(cat, job_id)
