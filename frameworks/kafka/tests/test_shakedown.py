import pytest

import sdk_install as install
import sdk_tasks as tasks
import sdk_marathon as marathon
import sdk_package as package
import sdk_cmd as command
import sdk_plan as plan
import json
import dcos

from tests.utils import (
    DEFAULT_PARTITION_COUNT,
    DEFAULT_REPLICATION_FACTOR,
    PACKAGE_NAME,
    DEFAULT_BROKER_COUNT,
    DEFAULT_TOPIC_NAME,
    EPHEMERAL_TOPIC_NAME,
    service_cli
)


def setup_module(module):
    install.uninstall(PACKAGE_NAME)
    install.install(PACKAGE_NAME, DEFAULT_BROKER_COUNT)


def teardown_module(module):
    install.uninstall(PACKAGE_NAME)


# --------- Endpoints -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_endpoints_address():
    def fun():
        ret=service_cli('endpoints broker')
        if len(result['native']) == DEFAULT_BROKER_COUNT:
            return ret
        return False
    address = shakedown.wait_for(fun)
    assert len(address) == 3
    assert len(address['direct']) == DEFAULT_BROKER_COUNT


@pytest.mark.smoke
@pytest.mark.sanity
def test_endpoints_zookeeper():
    zookeeper = command.run_cli('{} endpoints zookeeper'.format(PACKAGE_NAME))
    assert zookeeper.rstrip() == (
        'master.mesos:2181/dcos-service-{}'.format(PACKAGE_NAME)
    )


# --------- Broker -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_broker_list():
    brokers = service_cli('broker list')
    assert set(brokers) == set([str(i) for i in range(DEFAULT_BROKER_COUNT)])


@pytest.mark.smoke
@pytest.mark.sanity
def test_broker_invalid():
    assert not command.run_cli('{} broker get {}'.format(PACKAGE_NAME, DEFAULT_BROKER_COUNT + 1))


# --------- Pods -------------


@pytest.mark.smoke
@pytest.mark.special
def test_pods_restart():
    for i in range(DEFAULT_BROKER_COUNT):
        broker_id = shakedown.get_task_ids(PACKAGE_NAME,'{}-{}-broker'.format(PACKAGE_NAME, i))
        restart_info = service_cli('pods restart {}-{}'.format(PACKAGE_NAME, i))
        shakedown.check_task_updated(PACKAGE_NAME, '{}-{}-broker'.format(PACKAGE_NAME, i), broker_id)
        assert len(restart_info) == 2
        assert restart_info['tasks'] == '{}-{}-broker'.format(PACKAGE_NAME, i)


@pytest.mark.smoke
@pytest.mark.sanity
def test_pods_replace():
    broker_0_id = shakedown.get_task_ids(PACKAGE_NAME, '{}-0-broker'.format(PACKAGE_NAME))
    service_cli('pods replace {}-0-broker'.format(PACKAGE_NAME))
    shakedown.check_task_updated(PACKAGE_NAME, '{}-0-broker'.format(PACKAGE_NAME), broker_0_id)


# --------- Topics -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_topic_create():
    create_info = service_cli(
        'topic create {}'.format(EPHEMERAL_TOPIC_NAME)
    )
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


@pytest.fixture
def default_topic():
    service_cli('topic create {}'.format(DEFAULT_TOPIC_NAME))


@pytest.mark.sanity
def test_topic_partition_count(default_topic):
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


# --------- Cli -------------


@pytest.mark.smoke
@pytest.mark.sanity
def test_help():
    service_cli('help')


@pytest.mark.smoke
@pytest.mark.sanity
def test_config():
    configs = service_cli('config list')
    assert len(configs) == 1

    assert service_cli('config show {}'.format(configs[0]))
    assert service_cli('config target')
    assert service_cli('config target_id')


@pytest.mark.smoke
@pytest.mark.sanity
def test_plan():
    assert service_cli('plan list')
    assert service_cli('plan show deploy')
    assert service_cli('interrupt deploy Deployment')
    assert service_cli('continue deploy Deployment')


@pytest.mark.smoke
@pytest.mark.sanity
def test_state():
    assert service_cli('state framework_id')
    tasks = service_cli('state tasks')
    assert tasks


# --------- Suppressed -------------


@pytest.mark.smoke
@pytest.mark.smoke
def test_suppress():
    dcos_url = dcos.config.get_config_val('core.dcos_url')
    suppressed_url = urllib.parse.urljoin(dcos_url,
                                          'service/{}/v1/state/properties/suppressed'.format(PACKAGE_NAME))

    def fun():
        response = dcos.http.get(suppressed_url)
        response.raise_for_status()
        return response.text == "true"

    shakedown.wait_for(fun)



# --------- Port -------------


import shakedown

from tests.test_utils import (
    DYNAMIC_PORT_OPTIONS_DICT,
    STATIC_PORT_OPTIONS_DICT,
    check_health,
    get_kafka_config,
    install,
    marathon_api_url,
    spin,
    uninstall,
    update_kafka_config
)


def get_connection_info():
    def fn():
        return shakedown.run_dcos_command('kafka connection')

    def success_predicate(result):
        deployments = dcos.http.get(marathon_api_url('deployments')).json()
        if deployments:
            return False, 'Deployment is ongoing'

        stdout, stderr, rc = result
        try:
            result = json.loads(stdout)
        except Exception:
            return False, 'Command did not return JSON'
        else:
            return (
                not rc and len(result['address']) == 3,
                'Command errored or expected number of brokers are not up',
            )

    return json.loads(spin(fn, success_predicate)[0])


def setup_module(module):
    uninstall()


def teardown_module(module):
    uninstall()


@pytest.yield_fixture
def dynamic_port_config():
    install(DYNAMIC_PORT_OPTIONS_DICT)
    yield
    uninstall()


@pytest.fixture
def static_port_config():
    install(STATIC_PORT_OPTIONS_DICT)


@pytest.mark.sanity
def test_dynamic_port_comes_online(dynamic_port_config):
    check_health()


@pytest.mark.sanity
def test_static_port_comes_online(static_port_config):
    check_health()


@pytest.mark.sanity
def test_can_adjust_config_from_static_to_static_port():
    check_health()

    config = get_kafka_config()
    config['env']['BROKER_PORT'] = '9095'
    update_kafka_config(config)

    check_health()

    result = get_connection_info()
    assert len(result['address']) == 3

    for hostport in result['address']:
        assert hostport.split(':')[-1] == '9095'


@pytest.mark.sanity
def test_can_adjust_config_from_static_to_dynamic_port():
    check_health()

    config = get_kafka_config()
    config['env']['BROKER_PORT'] = '0'
    update_kafka_config(config)

    check_health()

    result = get_connection_info()
    assert len(result['address']) == 3

    for hostport in result['address']:
        assert 9092 != int(hostport.split(':')[-1])


@pytest.mark.sanity
def test_can_adjust_config_from_dynamic_to_dynamic_port():
    check_health()

    connections = get_connection_info()['address']
    config = get_kafka_config()
    brokerCpus = int(config['env']['BROKER_CPUS'])
    config['env']['BROKER_CPUS'] = str(brokerCpus + 0.1)
    update_kafka_config(config)

    check_health()


@pytest.mark.sanity
def test_can_adjust_config_from_dynamic_to_static_port():
    check_health()

    config = get_kafka_config()
    config['env']['BROKER_PORT'] = '9092'
    update_kafka_config(config)

    check_health()

    result = get_connection_info()
    assert len(result['address']) == 3

    for hostport in result['address']:
        assert hostport.split(':')[-1] == '9092'


# --------- HealtCheck -------------


# --------- Recovery -------------


# --------- Deployment Strategy  -------------