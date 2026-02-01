# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess

from celery import signals
from celery.utils.log import get_task_logger
from openrelik_common.logging import Logger
from openrelik_worker_common.file_utils import (
    create_output_file,
    count_file_lines,
    is_disk_image,
)
from openrelik_worker_common.mount_utils import BlockDevice
from openrelik_worker_common.task_utils import create_task_result, get_input_files


import datetime
import time

from .app import celery

TASK_NAME = "openrelik-worker-grep.tasks.grep"

TASK_METADATA = {
    "display_name": "Grep",
    "description": "Search for a regular expression in a file (case insensitive).",
    "task_config": [
        {
            "name": "regex",
            "label": "[a-f][0-9]+",
            "description": "Regular expression to grep for",
            "type": "text",
            "required": True,
        },
        {
            "name": "mount_disk_images",
            "label": "Mount disk images",
            "description": "If checked, the worker will try to mount disk images and grep the files inside the disk image.",
            "type": "checkbox",
            "required": True,
            "default_value": False,
        },
    ],
}

log_root = Logger()
logger = log_root.get_logger(__name__, get_task_logger(__name__))


@signals.task_prerun.connect
def on_task_prerun(sender, task_id, task, args, kwargs, **_):
    log_root.bind(
        task_id=task_id,
        task_name=task.name,
        worker_name=TASK_METADATA.get("display_name"),
    )


@celery.task(bind=True, name=TASK_NAME, metadata=TASK_METADATA)
def command(
    self,
    pipe_result: str = None,
    input_files: list = None,
    output_path: str = None,
    workflow_id: str = None,
    task_config: dict = None,
) -> str:
    """Run grep on input files.

    Args:
        pipe_result: Base64-encoded result from the previous Celery task, if any.
        input_files: List of input file dictionaries (unused if pipe_result exists).
        output_path: Path to the output directory.
        workflow_id: ID of the workflow.
        task_config: User configuration for the task.

    Returns:
        Base64-encoded dictionary containing task results.
    """
    log_root.bind(workflow_id=workflow_id)
    logger.debug(f"Starting {TASK_NAME} for workflow {workflow_id}")

    input_files = get_input_files(pipe_result, input_files or [])
    output_files = []
    base_command = ["grep", "-aEi", task_config.get("regex")]
    base_command_string = " ".join(base_command)

    mount_disk_images = task_config.get("mount_disk_images", False)

    disks_mounted = []
    for input_file in input_files:
        input_file_path = input_file.get("path")
        try:
            if mount_disk_images and is_disk_image(input_file):
                bd = BlockDevice(input_file_path, min_partition_size=1)
                bd.setup()
                mountpoints = bd.mount()
                disks_mounted.append(bd)
                base_command.append("-R")
                for mountpoint in mountpoints:
                    base_command.append(f"{mountpoint}/")
                command = base_command
            else:
                command = base_command + [input_file.get("path")]

            output_file = create_output_file(
                output_path,
                display_name=input_file.get("display_name") + ".grep",
            )

            with open(output_file.path, "w") as fh:
                process = subprocess.Popen(command, stdout=fh)
                start_time = datetime.datetime.now()
                update_interval_s = 3

                while process.poll() is None:
                    grep_matches = count_file_lines(output_file.path)
                    duration = datetime.datetime.now() - start_time
                    rate = (
                        int(grep_matches / duration.total_seconds())
                        if duration.total_seconds() > 0
                        else 0
                    )
                    self.send_event(
                        "task-progress",
                        data={"extracted_strings": grep_matches, "rate": rate},
                    )
                    time.sleep(update_interval_s)

            output_files.append(output_file.to_dict())
        except RuntimeError as e:
            logger.error("Error openrelik-worker-grep encountered: %s", str(e))
        finally:
            for blockdevice in disks_mounted:
                if blockdevice:
                    logger.debug(f"Unmounting image {blockdevice.image_path}")
                    blockdevice.umount()

    if not output_files:
        logger.info("openrelik-worker-grep task yielded no results!")

    return create_task_result(
        output_files=output_files,
        workflow_id=workflow_id,
        command=base_command_string,
        meta={},
    )
