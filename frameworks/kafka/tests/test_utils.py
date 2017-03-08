import pytest

import json
import dcos
import shakedown
import dcos.config
import dcos.http
import sdk_spin as spin

from tests.config import (
    PACKAGE_NAME,
    DEFAULT_BROKER_COUNT
)


def service_cli(cmd_str):
    full_cmd = '{} {}'.format(PACKAGE_NAME, cmd_str)
    ret_str = command.run_cli(full_cmd)
    return json.loads(ret_str)


def check_health():
    def fn():
        try:
            return shakedown.get_service_tasks(PACKAGE_NAME)
        except dcos.errors.DCOSHTTPException:
            return []

    def success_predicate(tasks):
        running_tasks = [t for t in tasks if t['state'] == TASK_RUNNING_STATE]
        print('Waiting for {} healthy tasks, got {}/{}'.format(
            DEFAULT_BROKER_COUNT, len(running_tasks), len(tasks)))
        return (
            len(running_tasks) == DEFAULT_BROKER_COUNT,
            'Service did not become healthy'
        )

    return spin.spin(fn, success_predicate)


def get_running_broker_task(broker_name):
    def fn():
        try:
            tasks = shakedown.get_service_tasks(PACKAGE_NAME)
            return [t for t in tasks if t['state'] == 'TASK_RUNNING' and t['name'] == broker_name]
        except dcos.errors.DCOSHTTPException:
            return []

    def success_predicate(tasks):
        return len(tasks) == 1, 'Failed to get task'

    return spin.spin(fn, success_predicate)


def task_id_changes(broker_name, task_id):
    def fn():
        try:
            tasks = shakedown.get_service_tasks(PACKAGE_NAME)
            return [t for t in tasks if t['state'] == 'TASK_RUNNING' and t['name'] == broker_name]
        except dcos.errors.DCOSHTTPException:
            return []

    def success_predicate(tasks):
        return len(tasks) == 1 and tasks[0]['id'] != task_id, "Task ID didn't change."

    return spin(fn, success_predicate)


def is_suppressed():
    dcos_url = dcos.config.get_config_val('core.dcos_url')
    suppressed_url = urllib.parse.urljoin(dcos_url, 'service/{}/v1/state/properties/suppressed'.format(PACKAGE_NAME))

    def suppress_url_check():
        response = dcos.http.get(suppressed_url)
        response.raise_for_status()
        return response.text

    def success_predicate(result):
        return result == "true", 'Waiting for supressed'

    spin.spin(suppress_url_check, success_predicate)
