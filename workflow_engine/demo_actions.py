"""Trusted, bounded actions used only by the local demo."""

from datetime import datetime, timezone
from time import sleep

from pydantic import BaseModel, ConfigDict, Field


class DelayParameters(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    seconds: float = Field(ge=0, le=2)


class RetryUntilParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")
    until: datetime


def _delay(parameters: DelayParameters):
    sleep(parameters.seconds)


def _retry_until(parameters: RetryUntilParameters):
    if datetime.now(timezone.utc) < parameters.until:
        raise RuntimeError("demo retry window is still active")


def register_actions(registry):
    """Register harmless demonstration handlers; no user executable is loaded."""
    registry.register("demo.delay.v1", DelayParameters, _delay)
    registry.register("demo.retry_until.v1", RetryUntilParameters, _retry_until)
