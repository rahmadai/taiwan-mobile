"""Exception hierarchy. Callers can catch one type per concern."""


class TaskflowError(Exception):
    """Base class for all taskflow errors."""


class InvalidTaskError(TaskflowError):
    """A submitted task definition was rejected; nothing was registered."""


class UnknownUserError(InvalidTaskError):
    """The user has no registered quota."""


class UnknownActionError(InvalidTaskError):
    """No action strategy is registered under the requested name."""


class ActionError(TaskflowError):
    """Raised by an action on a transient failure; the executor retries it."""


class PermanentActionError(ActionError):
    """Raised by an action when retrying cannot help (e.g. target missing)."""
