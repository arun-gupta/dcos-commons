import pytest
import shakedown

import sdk_cmd as cmd
import sdk_install as install
import sdk_plan as plan

from tests.config import (
    PACKAGE_NAME,
    DEFAULT_TASK_COUNT
)


def setup_module(module):
    shakedown.uninstall_package_and_data(PACKAGE_NAME, PACKAGE_NAME)
    install.gc_frameworks()
    install.install(PACKAGE_NAME, DEFAULT_TASK_COUNT)


def teardown_module(module):
    shakedown.uninstall_package_and_data(PACKAGE_NAME, PACKAGE_NAME)


@pytest.mark.smoke
def test_install():
    pass # Setup makes sure install has completed


@pytest.mark.sanity
def test_example():
    cmd.request('get', '{}/example'.format(shakedown.dcos_service_url('proxylite')))


@pytest.mark.sanity
def test_google():
    cmd.request('get', '{}/google'.format(shakedown.dcos_service_url('proxylite')))


@pytest.mark.sanity
def test_httpd():
    cmd.request('get', '{}/httpd'.format(shakedown.dcos_service_url('proxylite')))


@pytest.mark.sanity
def test_httpd():
    cmd.request('get', '{}/pyhttpsd'.format(shakedown.dcos_service_url('proxylite')))


@pytest.mark.sanity
def test_plan():
    plan.get_deployment_plan(PACKAGE_NAME)
