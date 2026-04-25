import logging
import os
import signal
import traceback

import rollbar
from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from celery.signals import worker_shutting_down

from bots.bot_controller import BotController
from bots.models import Bot, BotEventManager, BotEventSubTypes, BotEventTypes, BotStates

logger = logging.getLogger(__name__)

# Configurable via env vars. Set to 0 to disable the limit entirely.
# Defaults to 8 hours so bots aren't killed mid-meeting.
_BOT_SOFT_TIME_LIMIT = int(os.getenv("BOT_TASK_SOFT_TIME_LIMIT", "28800")) or None
_BOT_TIME_LIMIT = int(os.getenv("BOT_TASK_TIME_LIMIT", "28860")) or None


def get_bot_context_for_rollbar(bot_id):
    """Get bot context for Rollbar error reporting."""
    try:
        bot = Bot.objects.get(id=bot_id)
        return {
            "bot_id": str(bot.id),
            "bot_object_id": bot.object_id,
            "bot_state": BotStates(bot.state).label if bot.state else "Unknown",
            "bot_state_value": bot.state,
            "meeting_url": bot.meeting_url,
            "project_id": str(bot.project_id) if bot.project_id else None,
            "created_at": bot.created_at.isoformat() if bot.created_at else None,
            "first_heartbeat": bot.first_heartbeat_timestamp,
            "last_heartbeat": bot.last_heartbeat_timestamp,
            "bot_duration_seconds": bot.bot_duration_seconds(),
            "session_type": bot.session_type,
            "join_at": bot.join_at.isoformat() if bot.join_at else None,
        }
    except Bot.DoesNotExist:
        return {"bot_id": str(bot_id), "error": "Bot not found in database"}
    except Exception as e:
        return {"bot_id": str(bot_id), "error": f"Failed to fetch bot context: {str(e)}"}


def _cleanup_bot_controller(bot_id, bot_controller, context_label):
    """Call cleanup on bot_controller, swallowing errors so state marking still runs."""
    if not bot_controller:
        return
    try:
        logger.info(f"Attempting graceful cleanup for bot {bot_id} ({context_label})")
        bot_controller.cleanup()
    except Exception as cleanup_error:
        logger.error(f"Error during cleanup for bot {bot_id} ({context_label}): {cleanup_error}")
        rollbar.report_exc_info(extra_data={"bot_id": str(bot_id), "context": f"cleanup_{context_label}"})


def _mark_bot_fatal_error_if_active(bot_id, sub_type):
    """Transition bot to FATAL_ERROR if it is still in an active (non-terminal) state."""
    try:
        bot = Bot.objects.get(id=bot_id)
        if not BotEventManager.event_can_be_created_for_state(BotEventTypes.FATAL_ERROR, bot.state):
            logger.info(f"Bot {bot_id} is already in state {bot.state}, skipping FATAL_ERROR transition")
            return
        BotEventManager.create_event(bot=bot, event_type=BotEventTypes.FATAL_ERROR, event_sub_type=sub_type)
        logger.info(f"Bot {bot_id} transitioned to FATAL_ERROR (sub_type={BotEventSubTypes(sub_type).label})")
    except Bot.DoesNotExist:
        logger.error(f"Bot {bot_id} not found when trying to mark as fatal error")
    except Exception:
        logger.error(f"Failed to mark bot {bot_id} as fatal error")
        rollbar.report_exc_info(extra_data={"bot_id": str(bot_id), "context": "mark_bot_fatal_error"})


@shared_task(bind=True, soft_time_limit=_BOT_SOFT_TIME_LIMIT, time_limit=_BOT_TIME_LIMIT)
def run_bot(self, bot_id):
    logger.info(f"Running bot {bot_id}")
    bot_controller = None
    try:
        bot_controller = BotController(bot_id)
        bot_controller.run()
    except SoftTimeLimitExceeded:
        logger.error(f"Bot {bot_id} exceeded soft time limit ({_BOT_SOFT_TIME_LIMIT} seconds)")

        rollbar.report_message(
            message=f"Bot {bot_id} exceeded soft time limit",
            level="error",
            extra_data={
                "bot_context": get_bot_context_for_rollbar(bot_id),
                "task_id": self.request.id,
                "task_name": self.name,
                "full_traceback": traceback.format_exc(),
                "timeout_type": "soft_time_limit",
                "timeout_seconds": _BOT_SOFT_TIME_LIMIT,
            },
        )

        _mark_bot_fatal_error_if_active(bot_id, BotEventSubTypes.FATAL_ERROR_PROCESS_TERMINATED)
        _cleanup_bot_controller(bot_id, bot_controller, "soft_time_limit")
        raise

    except Exception:
        logger.error(f"Unexpected error in bot {bot_id}", exc_info=True)

        rollbar.report_exc_info(
            extra_data={
                "bot_context": get_bot_context_for_rollbar(bot_id),
                "task_id": self.request.id,
                "task_name": self.name,
            }
        )

        _mark_bot_fatal_error_if_active(bot_id, BotEventSubTypes.FATAL_ERROR_ATTENDEE_INTERNAL_ERROR)
        _cleanup_bot_controller(bot_id, bot_controller, "unexpected_exception")
        raise


def kill_child_processes():
    # Get the process group ID (PGID) of the current process
    pgid = os.getpgid(os.getpid())

    try:
        # Send SIGTERM to all processes in the process group
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # Process group may no longer exist


@worker_shutting_down.connect
def shutting_down_handler(sig, how, exitcode, **kwargs):
    # Just adding this code so we can see how to shut down all the tasks
    # when the main process is terminated.
    # It's likely overkill.
    logger.info("Celery worker shutting down, sending SIGTERM to all child processes")
    kill_child_processes()
