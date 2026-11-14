"""Validated HTTP request and response models for WF-004."""

from typing import Annotated, Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from .retries import RetryPolicy


TaskId = Annotated[str, StringConstraints(
    min_length=1, max_length=128,
    pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")]


class ActionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str = Field(min_length=1, max_length=128,
                      pattern=r"^[a-z][a-z0-9_.-]*\.v[1-9][0-9]*$")
    parameters: Dict[str, Any] = Field(default_factory=dict)


class RetryPolicyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_attempts: int = Field(default=1, ge=1, le=100, strict=True)
    initial_delay: float = Field(default=0, ge=0, le=86400, strict=True)
    multiplier: float = Field(default=2, ge=1, le=100, strict=True)
    max_delay: float = Field(default=300, ge=0, le=86400, strict=True)
    jitter: float = Field(default=0, ge=0, le=1, strict=True)

    @model_validator(mode="after")
    def validate_policy(self):
        values = self.model_dump()
        if any(isinstance(value, bool) for value in values.values()):
            raise ValueError("retry values must not be booleans")
        RetryPolicy(**values)
        return self


class TaskSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: TaskId
    dependencies: List[TaskId] = Field(default_factory=list, max_length=500)
    action: ActionInput
    retry_policy: RetryPolicyInput = Field(default_factory=RetryPolicyInput)

    @model_validator(mode="after")
    def unique_dependencies(self):
        if len(set(self.dependencies)) != len(self.dependencies):
            raise ValueError("task dependencies must be unique")
        if self.id in self.dependencies:
            raise ValueError("a task cannot depend on itself")
        return self


class WorkflowSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    tasks: List[TaskSubmission] = Field(min_length=1, max_length=500)


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str


class ErrorResponse(BaseModel):
    error: ErrorBody


class WorkflowCreated(BaseModel):
    workflow_id: str
    state: str
    created_at: str
    status_url: str

