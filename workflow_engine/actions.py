"""Server-owned action types accepted by API-submitted workflows."""

from dataclasses import dataclass
import importlib
import os
from typing import Callable, Dict, Type

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .retries import PermanentTaskError


class NoopParameters(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class RequireNonemptyParameters(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    text: str = Field(max_length=4096)


@dataclass(frozen=True)
class RegisteredAction:
    parameters: Type[BaseModel]
    handler: Callable[[BaseModel], object]


class UnknownActionType(ValueError):
    pass


class ActionRegistry:
    """Maps fixed action names to trusted handlers and typed parameter models."""

    def __init__(self):
        self._actions: Dict[str, RegisteredAction] = {}

    def register(self, name: str, parameters: Type[BaseModel],
                 handler: Callable[[BaseModel], object]) -> None:
        if not name or name in self._actions:
            raise ValueError(f"action type is empty or already registered: {name!r}")
        self._actions[name] = RegisteredAction(parameters, handler)

    def validate(self, name: str, parameters):
        action = self._actions.get(name)
        if action is None:
            raise UnknownActionType(name)
        try:
            return action.parameters.model_validate(parameters)
        except ValidationError:
            raise

    def resolve(self, name: str, parameters):
        action = self._actions.get(name)
        if action is None:
            raise UnknownActionType(name)

        def invoke():
            validated = action.parameters.model_validate(parameters)
            return action.handler(validated)
        return invoke

    def contains(self, name: str) -> bool:
        return name in self._actions

    @property
    def names(self):
        return tuple(sorted(self._actions))


def _noop(_parameters: NoopParameters) -> None:
    return None


def _require_nonempty(parameters: RequireNonemptyParameters) -> None:
    if not parameters.text.strip():
        raise PermanentTaskError("text must not be blank")


def default_action_registry() -> ActionRegistry:
    registry = ActionRegistry()
    registry.register("noop.v1", NoopParameters, _noop)
    registry.register("text.require_nonempty.v1", RequireNonemptyParameters,
                      _require_nonempty)
    return registry


def configured_action_registry() -> ActionRegistry:
    """Load optional trusted server registrations named by deployment config."""
    registry = default_action_registry()
    module_name = os.environ.get("WORKFLOW_ACTIONS_MODULE")
    if module_name:
        module = importlib.import_module(module_name)
        register = getattr(module, "register_actions", None)
        if not callable(register):
            raise RuntimeError("configured action module must define register_actions(registry)")
        register(registry)
    return registry
