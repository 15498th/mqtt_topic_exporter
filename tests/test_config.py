#!/usr/bin/env python3

import unittest
from io import StringIO
from unittest.mock import patch

from configloader import ConfigLoader, ConfigurationError
from exporter import ExporterConfig
from mqtt_topic_exporter import TopicToMetricConfig, parse_config
from mqttcmd import MQTTConfig

load_config = ConfigLoader.load_config

conf_mqtt_minimal = '''
[mqtt]
host = 127.0.0.1
'''
conf_mqtt_full = '''
[mqtt]
host = 127.0.0.1
port = 8883
keepalive = 60
client_id = exporter
loglevel = INFO
'''

conf_exporter_minimal = ''' '''
conf_exporter_full = '''
[exporter]
metrics_path = /metrics
bind_address =
port = 8842
loglevel = INFO
'''

conf_ttm_minimal = '''
[ttm_minimal]
topic = test/cnt1/#
metric_name = mqtt_metrics
metric_type = counter
topic_payload_pattern = test/([^/]+)/[^/ ]+ (\d+\.?\d*)
labels_template = name="\1", id="\1"
value_template = \2
'''

conf_ttm_full = '''
[ttm_full]
topic = test/cnt1/#

metric_name = mqtt_metrics
metric_type = counter
metric_help =

topic_payload_pattern = test/([^/]+)/[^/ ]+ (\d+\.?\d*)
labels_template = name="\1", id="\1"
value_template = \2
topic_payload_separator =

no_activity_timeout = 180

only_float_values =

status_good_topic = test/cnt1/status
status_bad_topic = test/cnt1/status
status_good_payload = online
status_bad_payload = offline
'''


@patch('builtins.open')
def get_config_for(config_text, mock):
    mock.return_value = StringIO(config_text)
    return load_config('-')


class TestConfigLoader(unittest.TestCase):

    def test_no_file(self):
        with self.assertRaises(ConfigurationError):
            load_config('unexistant_file.ini')

    def test_empty_file(self):
        with self.assertRaises(ConfigurationError):
            conf = get_config_for('')
            parse_config(conf)

    def test_minimal(self):
        text = conf_mqtt_minimal + conf_exporter_minimal + conf_ttm_minimal
        conf = get_config_for(text)
        parse_config(conf)

    def test_full(self):
        text = conf_mqtt_full + conf_exporter_full + conf_ttm_full
        conf = get_config_for(text)
        parse_config(conf)


class TestMQTTConfig(unittest.TestCase):

    def setUp(self):
        self._config_text = conf_exporter_minimal + conf_ttm_minimal
        self.base_config_text = conf_mqtt_full + self._config_text

    def test_incomplete(self):
        conf_mqtt = '[mqtt]'
        text = conf_mqtt + self._config_text
        conf = get_config_for(text)
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_bad_port(self):
        conf = get_config_for(self.base_config_text)
        conf['mqtt']['port'] = '-'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_bad_keepalive(self):
        conf = get_config_for(self.base_config_text)
        conf['mqtt']['keepalive'] = '-'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_bad_loglevel(self):
        conf = get_config_for(self.base_config_text)
        conf['mqtt']['loglevel'] = 'NOTIFY'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)


class TestExporterConfig(unittest.TestCase):

    def setUp(self):
        self._config_text = conf_mqtt_minimal + conf_ttm_minimal
        self.base_config_text = conf_exporter_full + self._config_text

    def test_bad_port(self):
        conf = get_config_for(self.base_config_text)
        conf['exporter']['port'] = '-'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_bad_loglevel(self):
        conf = get_config_for(self.base_config_text)
        conf['exporter']['loglevel'] = 'NOTIFY'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_empty_section(self):
        conf_exporter = '[exporter]' + '\n'
        text = conf_exporter + self._config_text
        conf = get_config_for(text)
        parse_config(conf)


class TestTTMConfig(unittest.TestCase):

    def setUp(self):
        self._config_text = conf_exporter_minimal + conf_mqtt_minimal
        self.base_config_text = conf_ttm_full + self._config_text

    def test_incomplete(self):
        conf_ttm = '[sensors]\ntopic = sensors/#\n'
        text = conf_ttm + self._config_text
        conf = get_config_for(text)
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_bad_pattern(self):
        conf = get_config_for(self.base_config_text)
        conf['ttm_full']['topic_payload_pattern'] = '(*_*)'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)

    def test_bad_timeout(self):
        conf = get_config_for(self.base_config_text)
        conf['ttm_full']['no_activity_timeout'] = '*'
        with self.assertRaises(ConfigurationError):
            parse_config(conf)
