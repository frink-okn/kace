from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from config import config
from functools import wraps

class SlackCanary:
    def __init__(self, token, channel_id):
        """
        Initialize the Slack Canary.
        :param token: Slack Bot User OAuth Token.
        :param channel_id: Slack channel ID where messages will be sent.
        """
        self.client = WebClient(token=token)
        self.channel_id = channel_id

    def send_message(self, text):
        """
        Send a message to the configured Slack channel.
        :param text: The message to send.
        """
        try:
            response = self.client.chat_postMessage(channel=self.channel_id, text=text)
            print(f"Message sent: {response['ts']}")
        except SlackApiError as e:
            print(f"Error sending message: {e.response['error']}")
            raise e

    def notify_event(self, event_name, **kwargs):
        """
        Notify Slack about a specific event.
        :param event_name: Name of the event.
        :param kwargs: Additional context for the event.
        """
        message = f"*Event Triggered*: {event_name}\n"
        for key, value in kwargs.items():
            message += f"> *{key}*: {value}\n"
        self.send_message(message)

    def slack_notify_on_failure(self, event_name):
        """
        Decorator to send a Slack notification if the decorated function fails.
        :param event_name: Name of the event to notify about.
        """

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    self.notify_event(
                        event_name,
                        function_name=func.__name__,
                        error_message=str(e),
                        **kwargs
                    )
                    raise  # Re-raise the exception after notifying

            return wrapper

        return decorator


slack_canary = SlackCanary(
    config.slack_token,
    config.slack_channel
)
