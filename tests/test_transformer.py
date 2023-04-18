#!/usr/bin/env python3

import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from mqtt_topic_exporter import TopicToMetric, TopicToMetricConfig


class TestTopicToMetric(unittest.TestCase):

    def setUp(self):
        conf = TopicToMetricConfig(
            topic='test/cnt1/#',
            metric_name='mqtt_metrics',
            metric_type='counter',
            metric_help=None,
            no_activity_timeout='120',
            topic_payload_pattern=r'test/([^/]+)/[^/ ]+ (\d+\.?\d*)',
            labels_template=r'name="\1"',
            value_template=r'\2'
        )
        self.ttm = TopicToMetric(conf)

    def test_match_to_labels(self):
        pattern = r'test/([^/]+)/[^ ]+ (.*)'
        text = 'test/cnt1/data 1'
        template = r'name="\1"'
        expected_labels = 'name="cnt1"'
        labels = TopicToMetric.match_to_template(pattern, template, text)
        self.assertEqual(labels, expected_labels)

    def test_match_to_value(self):
        pattern = r'test/([^/]+)/[^ ]+ (.*)'
        text = 'test/cnt1/data 1'
        template = r'\2'
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

    def test_mismatching_message(self):
        topic = 'test/cnt1/conf/setting1'
        payload = 'ON'
        expected_metric = ''
        self.ttm.on_message(topic, payload)
        metric = self.ttm.get_metric_text()
        self.assertEqual(metric, expected_metric)

    def test_no_messages(self):
        expected_metric = ''
        metric = self.ttm.get_metric_text()
        self.assertEqual(metric, expected_metric)

    def test_mismatch_wont_overwrite(self):
        expected_metric = '#TYPE mqtt_metrics counter\nmqtt_metrics{name="cnt1"} 1\n'
        topic = 'test/cnt1/data'
        payload1 = '1'
        payload2 = 'ON'
        self.ttm.on_message(topic, payload1)
        self.ttm.on_message(topic, payload2)
        metric = self.ttm.get_metric_text()
        self.assertEqual(metric, expected_metric)

    def test_update_will_overwrite(self):
        expected_metric = '#TYPE mqtt_metrics counter\nmqtt_metrics{name="cnt1"} 2\n'
        topic = 'test/cnt1/data'
        payload1 = '1'
        payload2 = '2'
        self.ttm.on_message(topic, payload1)
        self.ttm.on_message(topic, payload2)
        metric = self.ttm.get_metric_text()
        self.assertEqual(metric, expected_metric)

    def test_two_matching_topics(self):
        expected_metric = '#TYPE mqtt_metrics counter\nmqtt_metrics{name="cnt1"} 1\nmqtt_metrics{name="cnt2"} 2\n'
        topic1 = 'test/cnt1/data'
        topic2 = 'test/cnt2/data'
        payload1 = '1'
        payload2 = '2'
        self.ttm.on_message(topic1, payload1)
        self.ttm.on_message(topic2, payload2)
        metric = self.ttm.get_metric_text()
        self.assertEqual(metric, expected_metric)

    @patch('datetime.datetime')
    def test_no_activity_timeout(self, mock):
        topic = 'test/cnt1/data'
        payload = '1'
        base_date = datetime(2020, 1, 1, 0, 0, 0)
        mock.now.return_value = base_date
        self.ttm.on_message(topic, payload)

        mock.now.return_value = base_date + timedelta(seconds=60)
        metric = self.ttm.get_metric_text()
        expected_metric = '#TYPE mqtt_metrics counter\nmqtt_metrics{name="cnt1"} 1\n'
        self.assertEqual(metric, expected_metric)

        mock.now.return_value = base_date + timedelta(hours=1)
        metric = self.ttm.get_metric_text()
        expected_metric = ''
        self.assertEqual(metric, expected_metric)
