#!/usr/bin/env python3

from functools import wraps
import sys
import unittest
from unittest import mock
from unittest.mock import patch, call

from mqtt_topic_exporter import TopicToMetricConfig, TopicToMetric


def get_config_example(name):
    default = 'minimal_counter'
    configs = {
        'minimal_counter': (
        )
    }
    return configs.get(name, configs[default])


class TestTopicToMetric(unittest.TestCase):

    def setUp(self):
        conf = TopicToMetricConfig(
            topic='test/cnt1/#',
            metric_name='mqtt_metrics',
            metric_type='counter',
            metric_help=None,
            topic_payload_pattern=r'test/([^/]+)/[^ ]+ (.*)',
            labels_template=r'name="\1"',
            value_template=r'\2'
        )
        self.ttm = TopicToMetric(conf)
    
    def test_match_to_labels(self):
        pattern=r'test/([^/]+)/[^ ]+ (.*)'
        text='test/cnt1/data 1'
        template=r'name="\1"'
        expected_labels = 'name="cnt1"'
        labels = TopicToMetric.match_to_template(pattern, template, text)
        self.assertEqual(labels, expected_labels)

    def test_match_to_value(self):
        pattern=r'test/([^/]+)/[^ ]+ (.*)'
        text='test/cnt1/data 1'
        template=r'\2'
        expected_value = '1'
        value = TopicToMetric.match_to_template(pattern, template, text)
        self.assertEqual(value, expected_value)

    def test_metric_text(self):
        expected_metric = '#TYPE mqtt_metrics counter\nmqtt_metrics{name="cnt1"} 1\n'
        topic = 'test/cnt1/data'
        payload = '1'
        self.ttm.on_message(topic, payload)
        metric = self.ttm.get_metric_text()
        self.assertEqual(metric, expected_metric) 
