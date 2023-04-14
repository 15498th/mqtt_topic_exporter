#!/usr/bin/env python3

import configparser
from collections import defaultdict
from dataclasses import dataclass
import datetime
import itertools
import logging
import logging.handlers
import re
from socketserver import ThreadingMixIn
from typing import Dict, List, Optional, Tuple
from wsgiref.simple_server import make_server, WSGIServer, WSGIRequestHandler

from paho.mqtt import client as mqtt

from configloader import TryParse, try_parsing_section, load_config, ConfigurationError
from mqttcmd import MQTTClient, MQTTConfig, Action
from exporter import Metric, ExporterConfig, PrometheusExporter, WSGI_LOGGER_NAME


MQTT_SECTION_NAME = 'mqtt'
EXPORTER_SECTION_NAME = 'exporter'

@dataclass
class TopicToMetricConfig:

    # subscription topic, with + and # wildcards
    topic: str

    # static part of metric
    metric_name: str
    metric_type: str
    metric_help: Optional[str]

    # extraction of labels and value
    topic_payload_pattern: str
    labels_template: str
    value_template: str
    topic_payload_separator: str = ' '

    # do not generate metric if last message on topic
    # was more than no_activity_timeout seconds ago
    no_activity_timeout: Optional[int] = None

    # ignore message if value is not a valid float
    only_float_values: Optional[bool] = True

    # generate metric after status_good_payload received on status_good_topic
    # until status_bad_payload received on status_bad_topic
    status_good_topic: Optional[str] = None
    status_good_payload: Optional[str] = None
    status_bad_topic: Optional[str] = None
    status_bad_payload: Optional[str] = None

    def __post_init__(self):
        self.topic_payload_pattern = TryParse.regexp(self.topic_payload_pattern)
        if self.no_activity_timeout is not None:
            self.no_activity_timeout = TryParse.timeout(self.no_activity_timeout)


class TopicToMetric(Action):

    def __init__(self, conf: TopicToMetricConfig):
        self.conf = conf
        self.processed_topics: Dict[str, Metric] = defaultdict(self._metric_factory)

    def _metric_factory(self):
        return Metric(self.conf.metric_name,
                      self.conf.metric_help,
                      self.conf.metric_type,
                      self.conf.no_activity_timeout)

    def get_topic(self):
        return self.conf.topic

    def on_message(self, topic: str, payload: str):
        conf = self.conf
        combined = topic + conf.topic_payload_separator + payload
        metric_labels = self.match_to_template(
            conf.topic_payload_pattern,
            conf.labels_template,
            combined)
        metric_value = self.match_to_template(
            conf.topic_payload_pattern,
            conf.value_template,
            combined)
        if metric_labels is not None and metric_value is not None:
            metric = self.processed_topics[topic]
            metric.update(metric_labels, metric_value)
        logging.debug(f'{topic} {payload}')

    def get_metric_text(self):
        header = self._metric_factory()._render_header()
        text = ''.join([metric.get_metric_text(skip_header=True)
                        for metric in self.processed_topics.values()])
        if text:
            return header + text
        else:
            return ''

    @classmethod
    def match_to_template(cls, pattern: str, template: str, text: str) -> Optional[str]:
        if re.match(pattern, text) is not None:
            return re.sub(pattern, template, text)
        else:
            logging.warning(f'payload "{text}" doesn\'t match pattern "{pattern}"')
            return None


def parse_config(c: configparser.ConfigParser) -> Tuple[
        MQTTConfig, ExporterConfig, List[TopicToMetric]]:
    try:
        mqtt_section = c[MQTT_SECTION_NAME]
        exporter_section = c[EXPORTER_SECTION_NAME] if c.has_section(EXPORTER_SECTION_NAME) else {}
    except KeyError as e:
        raise ConfigurationError(f'config is missing non-arbitrary section {e}') from e
    mqtt_conf = try_parsing_section('mqtt', MQTTConfig, mqtt_section)
    exporter_conf = try_parsing_section('exporter', ExporterConfig, exporter_section)
    ttms = []
    for name, conf in c.items():
        if name in (c.default_section, MQTT_SECTION_NAME, EXPORTER_SECTION_NAME):
            continue
        ttm_config = try_parsing_section(name, TopicToMetricConfig, conf)
        ttm = TopicToMetric(ttm_config)
        ttms.append(ttm)
    return mqtt_conf, exporter_conf, ttms


def set_root_logger(log_level=logging.INFO):
    log_format = '[%(asctime)s] %(name)s %(message)s'
    date_format = '%Y/%m/%d %H:%M:%S'
    logging.basicConfig(level=log_level, format=log_format, datefmt=date_format)


def set_loggers(mqtt_conf, exporter_conf):
    handler = logging.handlers.RotatingFileHandler(
        exporter_conf.log_path, maxBytes=100000, backupCount=3)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    server_logger = logging.getLogger(WSGI_LOGGER_NAME)
    server_logger.addHandler(handler)
    server_logger.setLevel(exporter_conf.loglevel)
    server_logger.propagate = False


def main():
    set_root_logger(logging.DEBUG)
    conf = load_config('config.ini')
    mqtt_conf, exporter_conf, ttms = parse_config(conf)
    set_loggers(mqtt_conf, exporter_conf)
    mqttc = MQTTClient(mqtt_conf, ttms)
    mqttc.run_bg()
    exporter = PrometheusExporter(exporter_conf, ttms)
    exporter.run()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Stopping...')
