from unittest.mock import MagicMock, patch

from celery.exceptions import SoftTimeLimitExceeded
from django.test import TestCase, override_settings

from accounts.models import Organization
from bots.models import (
    Bot,
    BotEventManager,
    BotEventSubTypes,
    BotEventTypes,
    BotStates,
    Project,
    Recording,
    RecordingStates,
    RecordingTypes,
    TranscriptionTypes,
)
from bots.tasks.run_bot_task import run_bot


def _advance_bot_to_joined_recording(bot):
    """Drive bot through the valid event sequence to reach JOINED_RECORDING."""
    Recording.objects.create(
        bot=bot,
        recording_type=RecordingTypes.AUDIO_ONLY,
        transcription_type=TranscriptionTypes.NON_REALTIME,
        state=RecordingStates.NOT_STARTED,
    )
    BotEventManager.create_event(bot, BotEventTypes.JOIN_REQUESTED)
    BotEventManager.create_event(bot, BotEventTypes.BOT_JOINED_MEETING)
    BotEventManager.create_event(bot, BotEventTypes.BOT_RECORDING_PERMISSION_GRANTED)


def _advance_bot_to_ended(bot):
    """Drive bot through the valid event sequence to reach ENDED."""
    _advance_bot_to_joined_recording(bot)
    BotEventManager.create_event(bot, BotEventTypes.MEETING_ENDED)
    BotEventManager.create_event(bot, BotEventTypes.POST_PROCESSING_COMPLETED)


@override_settings(CELERY_TASK_ALWAYS_EAGER=True, CELERY_TASK_EAGER_PROPAGATES=True)
class RunBotTaskTestCase(TestCase):
    def setUp(self):
        self.organization = Organization.objects.create(
            name="Test Organization",
            centicredits=10000,
        )
        self.project = Project.objects.create(
            name="Test Project",
            organization=self.organization,
        )
        self.bot = Bot.objects.create(
            project=self.project,
            name="Test Bot",
            meeting_url="https://example.zoom.us/j/123456789",
        )
        _advance_bot_to_joined_recording(self.bot)

    def _assert_bot_already_fatal_error(self, bot):
        """Side-effect for cleanup() that proves FATAL_ERROR is set before cleanup runs."""
        bot.refresh_from_db()
        self.assertEqual(
            bot.state,
            BotStates.FATAL_ERROR,
            "Bot must already be in FATAL_ERROR state when cleanup() is called",
        )

    @patch("bots.tasks.run_bot_task.rollbar")
    @patch("bots.tasks.run_bot_task.BotController")
    def test_soft_timeout_marks_fatal_error_before_cleanup(self, mock_controller_cls, mock_rollbar):
        mock_instance = mock_controller_cls.return_value
        mock_instance.run.side_effect = SoftTimeLimitExceeded()
        mock_instance.cleanup.side_effect = lambda: self._assert_bot_already_fatal_error(self.bot)

        with self.assertRaises(SoftTimeLimitExceeded):
            run_bot.apply(args=[self.bot.id])

        mock_instance.cleanup.assert_called_once()
        self.bot.refresh_from_db()
        self.assertEqual(self.bot.state, BotStates.FATAL_ERROR)

        last_event = self.bot.bot_events.order_by("created_at").last()
        self.assertEqual(last_event.event_type, BotEventTypes.FATAL_ERROR)
        self.assertEqual(last_event.event_sub_type, BotEventSubTypes.FATAL_ERROR_PROCESS_TERMINATED)

        mock_rollbar.report_message.assert_called_once()
        extra_data = mock_rollbar.report_message.call_args.kwargs["extra_data"]
        self.assertEqual(extra_data["timeout_type"], "soft_time_limit")

    @patch("bots.tasks.run_bot_task.rollbar")
    @patch("bots.tasks.run_bot_task.BotController")
    def test_unexpected_exception_marks_fatal_error_before_cleanup(self, mock_controller_cls, mock_rollbar):
        mock_instance = mock_controller_cls.return_value
        mock_instance.run.side_effect = RuntimeError("unexpected crash")
        mock_instance.cleanup.side_effect = lambda: self._assert_bot_already_fatal_error(self.bot)

        with self.assertRaises(RuntimeError):
            run_bot.apply(args=[self.bot.id])

        mock_instance.cleanup.assert_called_once()
        self.bot.refresh_from_db()
        self.assertEqual(self.bot.state, BotStates.FATAL_ERROR)

        last_event = self.bot.bot_events.order_by("created_at").last()
        self.assertEqual(last_event.event_type, BotEventTypes.FATAL_ERROR)
        self.assertEqual(last_event.event_sub_type, BotEventSubTypes.FATAL_ERROR_ATTENDEE_INTERNAL_ERROR)

        mock_rollbar.report_exc_info.assert_called()

    @patch("bots.tasks.run_bot_task.rollbar")
    @patch("bots.tasks.run_bot_task.BotController")
    def test_does_not_create_second_fatal_error_for_terminal_bot(self, mock_controller_cls, mock_rollbar):
        ended_bot = Bot.objects.create(
            project=self.project,
            name="Ended Bot",
            meeting_url="https://example.zoom.us/j/999999",
        )
        _advance_bot_to_ended(ended_bot)
        events_before = ended_bot.bot_events.count()

        mock_instance = mock_controller_cls.return_value
        mock_instance.run.side_effect = RuntimeError("crash on already-ended bot")

        with self.assertRaises(RuntimeError):
            run_bot.apply(args=[ended_bot.id])

        ended_bot.refresh_from_db()
        self.assertEqual(ended_bot.state, BotStates.ENDED)
        self.assertEqual(ended_bot.bot_events.count(), events_before)
        mock_instance.cleanup.assert_called_once()

    @patch("bots.tasks.run_bot_task.rollbar")
    @patch("bots.tasks.run_bot_task.BotController")
    def test_cleanup_failure_does_not_prevent_fatal_state(self, mock_controller_cls, mock_rollbar):
        mock_instance = mock_controller_cls.return_value
        mock_instance.run.side_effect = RuntimeError("task crash")
        mock_instance.cleanup.side_effect = Exception("cleanup failed too")

        with self.assertRaises(RuntimeError):
            run_bot.apply(args=[self.bot.id])

        self.bot.refresh_from_db()
        self.assertEqual(self.bot.state, BotStates.FATAL_ERROR)

        cleanup_error_calls = [
            c
            for c in mock_rollbar.report_exc_info.call_args_list
            if "cleanup_" in str(c.kwargs.get("extra_data", {}).get("context", ""))
        ]
        self.assertEqual(len(cleanup_error_calls), 1)
