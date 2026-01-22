from datetime import datetime, timezone
from typing import Dict, Any, List
from cat.log import log
from cat import hook, CheshireCat, StrayCat, run_sync_or_async
from cat.auth.permissions import AuthUserInfo


def setup_scrapycat_schedule(cheshire_cat: CheshireCat) -> None:
    """Setup or update the ScrapyCat scheduled job based on settings"""
    # Create wrapper function for scheduled execution
    def scheduled_scrapycat_job(user_message: str) -> str:
        """Wrapper function for scheduled ScrapyCat execution"""
        from .scrapycat import process_scrapycat_command
        # Create a proper StrayCat instance for the scheduled job
        # Use a system user for scheduled operations
        system_user = AuthUserInfo(
            id="system",
            name="system"
        )
        stray_cat = StrayCat(cheshire_cat.agent_key, system_user, cheshire_cat.plugin_manager_generator)
        return run_sync_or_async(
            process_scrapycat_command,
            user_message=user_message,
            cat=stray_cat,
            scheduled=True,
        )

    # Load ScrapyCat plugin settings
    settings = cheshire_cat.mad_hatter.get_plugin().load_settings()

    try:
        scheduled_command: str = settings.get("scheduled_command", "").strip()
        schedule_hour: int = settings.get("schedule_hour", 3)
        schedule_minute: int = settings.get("schedule_minute", 0)
        
        # Job ID for the scheduled task
        job_id: str = "scrapycat_scheduled_scraping"
        
        # Always try to remove any existing job first
        try:
            cheshire_cat.white_rabbit.scheduler.remove_job(job_id)
            log.info(f"Removed existing scheduled ScrapyCat job: {job_id}")
        except Exception:
            pass  # Job doesn't exist, which is fine
        
        # If no command is configured, just remove the job and return
        if not scheduled_command:
            log.info("No scheduled ScrapyCat command configured, job removed")
            return

        # Schedule the new job: call the wrapper function
        cheshire_cat.white_rabbit.schedule_cron_job(
            job=scheduled_scrapycat_job,
            job_id=job_id,
            hour=schedule_hour,
            minute=schedule_minute,
            user_message=scheduled_command,
        )
        
        # Get current time for comparison
        current_utc: datetime = datetime.now(timezone.utc)
        
        log.info(f"Scheduled ScrapyCat command '{scheduled_command}' to run daily at {schedule_hour:02d}:{schedule_minute:02d} UTC")
        log.info(f"Current UTC time: {current_utc}")
        
        # Debug: Check scheduler status and all jobs
        try:
            scheduler_running: bool = cheshire_cat.white_rabbit.scheduler.running
            log.info(f"White Rabbit scheduler running: {scheduler_running}")
            
            if not scheduler_running:
                log.warning("White Rabbit scheduler is not running! This may be why jobs don't execute.")
            
            # Get all jobs
            all_jobs: List[Any] = cheshire_cat.white_rabbit.get_jobs()
            log.info(f"Total scheduled jobs: {len(all_jobs)}")
            
            # Check our specific job
            job: Dict[str, Any] | None = cheshire_cat.white_rabbit.get_job(job_id)
            if not job:
                log.error(f"Job not found in scheduler after creation: {job_id}")
                return

            log.info(f"Job successfully added to scheduler: {job}")
            if "next_run" not in job:
                log.warning("Job dictionary has no 'next_run' key")
                return

            # Log next run time - job is a dictionary, not an object
            log.info(f"Next scheduled run: {job['next_run']}")
            time_diff = job["next_run"] - current_utc
            log.info(f"Time until next run: {time_diff}")

            if time_diff.total_seconds() < 0:
                log.warning("Job is scheduled in the past! This might be why it's not executing.")
        except Exception as debug_e:
            log.error(f"Error checking scheduled job: {debug_e}", exc_info=True)
    except Exception as e:
        log.error(f"Failed to setup scheduled ScrapyCat job: {str(e)}")


@hook()
def after_cat_bootstrap(cat) -> None:
    """Hook called at Cat startup to schedule recurring jobs"""
    log.info("Setting up ScrapyCat scheduled jobs after Cat bootstrap")
    # The cat parameter here is CheshireCat during bootstrap
    setup_scrapycat_schedule(cat)
