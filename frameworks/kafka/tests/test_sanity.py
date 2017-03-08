import pytest

import sdk_install as install
import sdk_tasks as tasks
import sdk_marathon as marathon
import sdk_utils as utils
import sdk_package as package
import sdk_cmd as command
import sdk_spin as spin
import json
import dcos

from tests.config import (
    PACKAGE_NAME,
    DEFAULT_BROKER_COUNT,
    DEFAULT_PARTITION_COUNT,
    DEFAULT_REPLICATION_FACTOR
)

from tests.test_utils import (
    service_cli,
    check_health,
    get_running_broker_task
)


DEFAULT_TOPIC_NAME = 'topic1'
EPHEMERAL_TOPIC_NAME = 'topic_2'


def setup_module(module):
    install.uninstall(PACKAGE_NAME)
    utils.gc_frameworks()
    install.install(PACKAGE_NAME, DEFAULT_BROKER_COUNT)
    check_health()
    default_topic()


def teardown_module(module):
    install.uninstall(PACKAGE_NAME)


# Connection
@pytest.mark.smoke
@pytest.mark.sanity
def test_endpoints_address():
    def fun():
        return service_cli('endpoints broker')

    def ends(result):
        return (
            len(result['native']) == DEFAULT_BROKER_COUNT,
            'waiting for the expected number of brokers to come online'
        )

    address = spin.spin(fun, ends)
    assert len(address) == 3
    assert len(address['direct']) == DEFAULT_BROKER_COUNT


@pytest.mark.smoke
@pytest.mark.sanity
def test_endpoints_zookeeper():
    zookeeper = command.run_cli('{} endpoints zookeeper'.format(PACKAGE_NAME))
    assert zookeeper.rstrip() == (
        'master.mesos:2181/dcos-service-{}'.format(PACKAGE_NAME)
    )


# Broker
@pytest.mark.smoke
@pytest.mark.sanity
def test_broker_list():
    brokers = service_cli('broker list')
    assert set(brokers) == set([str(i) for i in range(DEFAULT_BROKER_COUNT)])


@pytest.mark.smoke
@pytest.mark.sanity
def test_broker_invalid():
    assert not command.run_cli('{} broker get {}'.format(PACKAGE_NAME, DEFAULT_BROKER_COUNT + 1))


# Pod
@pytest.mark.smoke
@pytest.mark.special
def test_pod_restart():
    for i in range(DEFAULT_BROKER_COUNT):
        broker_task = get_running_broker_task('{}-{}-broker'.format(PACKAGE_NAME, i))[0]
        broker_id = broker_task['id']
        assert broker_id.startswith('{}-{}-broker-__'.format(PACKAGE_NAME,i))
        restart_info = service_cli('pods restart {}-{}'.format(PACKAGE_NAME,i))
        task_id_changes('{}-{}-broker'.format(PACKAGE_NAME, i), broker_id)
        assert len(restart_info) == 2
        assert restart_info['tasks'] == '{}-{}-broker'.format(PACKAGE_NAME, i)


@pytest.mark.smoke
@pytest.mark.sanity
def test_pod_replace():
    broker_0_task = get_running_broker_task('{}-0-broker'.format(PACKAGE_NAME))[0]
    broker_0_id = broker_0_task['id']

    service_cli('pods replace {}-0-broker',format(PACKAGE_NAME))
    task_id_changes('{}-0-broker'.format(PACKAGE_NAME), broker_0_id)


# Topic
@pytest.mark.smoke
@pytest.mark.sanity
def test_topic_create():
    create_info = service_cli(
        'topic create {}'.format(EPHEMERAL_TOPIC_NAME)
    )
    print(create_info)
    assert ('Created topic "%s".\n' % EPHEMERAL_TOPIC_NAME in create_info['message'])
    assert ("topics with a period ('.') or underscore ('_') could collide." in create_info['message'])
    topic_list_info = service_cli('topic list')
    assert topic_list_info == [EPHEMERAL_TOPIC_NAME]

    topic_info = service_cli('topic describe {}'.format(EPHEMERAL_TOPIC_NAME))
    assert len(topic_info) == 1
    assert len(topic_info['partitions']) == DEFAULT_PARTITION_COUNT


@pytest.mark.smoke
@pytest.mark.sanity
def test_topic_delete():
    delete_info = service_cli('topic delete {}'.format(EPHEMERAL_TOPIC_NAME))

    assert len(delete_info) == 1
    assert delete_info['message'].startswith('Output: Topic {} is marked for deletion'.format(EPHEMERAL_TOPIC_NAME))

    topic_info = service_cli('topic describe {}'.format(EPHEMERAL_TOPIC_NAME))
    assert len(topic_info) == 1
    assert len(topic_info['partitions']) == DEFAULT_PARTITION_COUNT


@pytest.mark.sanity
def test_topic_partition_count():
    topic_info = service_cli('topic describe {}'.format(DEFAULT_TOPIC_NAME))
    assert len(topic_info['partitions']) == DEFAULT_PARTITION_COUNT


@pytest.mark.sanity
def test_topic_offsets_increase_with_writes():
    offset_info = service_cli('topic offsets --time="-1" {}'.format(DEFAULT_TOPIC_NAME))
    assert len(offset_info) == DEFAULT_PARTITION_COUNT

    offsets = {}
    for o in offset_info:
        assert len(o) == DEFAULT_REPLICATION_FACTOR
        offsets.update(o)

    assert len(offsets) == DEFAULT_PARTITION_COUNT

    num_messages = 10
    write_info = service_cli('topic producer_test {} {}'.format(DEFAULT_TOPIC_NAME, num_messages))
    assert len(write_info) == 1
    assert write_info['message'].startswith('Output: {} records sent'.format(num_messages))

    offset_info = service_cli('topic offsets --time="-1" {}'.format(DEFAULT_TOPIC_NAME))
    assert len(offset_info) == DEFAULT_PARTITION_COUNT

    post_write_offsets = {}
    for offsets in offset_info:
        assert len(o) == DEFAULT_REPLICATION_FACTOR
        post_write_offsets.update(o)

    assert not offsets == post_write_offsets


@pytest.mark.sanity
def test_decreasing_topic_partitions_fails():
    partition_info = service_cli('topic partitions {} {}'.format(DEFAULT_TOPIC_NAME, DEFAULT_PARTITION_COUNT - 1))

    assert len(partition_info) == 1
    assert partition_info['message'].startswith('Output: WARNING: If partitions are increased')
    assert ('The number of partitions for a topic can only be increased' in partition_info['message'])


@pytest.mark.sanity
def test_setting_topic_partitions_to_same_value_fails():
    partition_info = service_cli('topic partitions {} {}'.format(DEFAULT_TOPIC_NAME, DEFAULT_PARTITION_COUNT))

    assert len(partition_info) == 1
    assert partition_info['message'].startswith('Output: WARNING: If partitions are increased')
    assert ('The number of partitions for a topic can only be increased' in partition_info['message'])


@pytest.mark.sanity
def test_increasing_topic_partitions_succeeds():
    partition_info = service_cli('topic partitions {} {}'.format(DEFAULT_TOPIC_NAME, DEFAULT_PARTITION_COUNT + 1))

    assert len(partition_info) == 1
    assert partition_info['message'].startswith('Output: WARNING: If partitions are increased')
    assert ('The number of partitions for a topic can only be increased' not in partition_info['message'])


@pytest.mark.sanity
def test_no_under_replicated_topics_exist():
    partition_info = service_cli('topic under_replicated_partitions')

    assert len(partition_info) == 1
    assert partition_info['message'] == ''


@pytest.mark.sanity
def test_no_unavailable_partitions_exist():
    partition_info = service_cli('topic unavailable_partitions')

    assert len(partition_info) == 1
    assert partition_info['message'] == ''


# Cli
@pytest.mark.sanity
def test_help():
    service_cli('help')


@pytest.mark.sanity
def test_config():
    configs = service_cli('config list')
    assert len(configs) == 1

    assert service_cli('config show {}'.format(configs[0]))
    assert service_cli('config target')
    assert service_cli('config target_id')


@pytest.mark.sanity
def test_plan():
    assert service_cli('plan list')
    assert service_cli('plan show deploy')
    assert service_cli('interrupt deploy Deployment')
    assert service_cli('continue deploy Deployment')


@pytest.mark.sanity
def test_state():
    assert service_cli('state framework_id')
    tasks = service_cli('state tasks')
    assert tasks


# Auxiliary
@pytest.fixture
def default_topic():
    service_cli('topic create {}'.format(DEFAULT_TOPIC_NAME))
