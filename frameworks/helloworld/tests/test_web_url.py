import pytest
import shakedown

import sdk_install as install
import sdk_plan as plan

from tests.config import (
    PACKAGE_NAME
)


def setup_module(module):
    shakedown.uninstall_package_and_data(PACKAGE_NAME, PACKAGE_NAME)
    options = {
        "service": {
            "spec_file": "examples/web-url.yml"
        }
    }

    # this config produces 1 hello's + 0 world's:
    install.install(PACKAGE_NAME, 1, additional_options=options)


def teardown_module(module):
    shakedown.uninstall_package_and_data(PACKAGE_NAME, PACKAGE_NAME)


@pytest.mark.sanity
def test_deploy():
    deployment_plan = plan.get_deployment_plan(PACKAGE_NAME).json()
    print("deployment_plan: " + str(deployment_plan))

    assert(len(deployment_plan['phases']) == 1)
    assert(deployment_plan['phases'][0]['name'] == 'hello')
    assert(len(deployment_plan['phases'][0]['steps']) == 1)

