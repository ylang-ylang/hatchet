from typing import Any
from unittest.mock import MagicMock

from pydantic import TypeAdapter

from hatchet_sdk import EmptyModel
from hatchet_sdk.config import ClientConfig
from hatchet_sdk.runnables.types import WorkflowConfig, normalize_validator
from hatchet_sdk.runnables.workflow import Workflow


def test_success_hook_dependencies_survive_repeated_serialization() -> None:
    """Publishing a workflow again must not turn its finalizer into a root."""
    workflow = Workflow(
        config=WorkflowConfig(
            name="stable-success-hook",
            input_validator=TypeAdapter(normalize_validator(EmptyModel)),
        ),
        client=MagicMock(config=ClientConfig.model_construct(namespace="")),
    )

    async def execute(_input: EmptyModel, _ctx: Any) -> None:
        pass

    root = workflow.task(name="root")(execute)
    workflow.task(name="left", parents=[root])(execute)
    middle = workflow.task(name="middle", parents=[root])(execute)
    workflow.durable_task(name="right", parents=[middle])(execute)
    workflow.on_success_task(name="finalize")(execute)

    expected = {
        "root": (),
        "left": ("root",),
        "middle": ("root",),
        "right": ("middle",),
        "finalize-on-success": ("left", "right"),
    }
    for _ in range(3):
        request = workflow.to_proto()
        assert {
            task.readable_id: tuple(task.parents) for task in request.tasks
        } == expected
