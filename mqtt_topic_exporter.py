#!/usr/bin/env python3

import argparse
import configparser
import logging
import logging.handlers
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from paho.mqtt import client as mqtt

from configloader import (ConfigLoader, ConfigurationError, TryParse,
                          set_root_logger)
from exporter import (WSGI_LOGGER_NAME, ExporterConfig, Metric,
                      PrometheusExporter, set_wsgi_logger)
from mqttcmd import Action, MQTTClient, MQTTConfig

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
        logging.debug(f'[MQTT] {topic} {payload}')

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
            logging.debug(f'payload "{text}" doesn\'t match pattern "{pattern}"')
            return None


def parse_config(c: configparser.ConfigParser) -> Tuple[
        MQTTConfig, ExporterConfig, List[TopicToMetricConfig]]:
    configs_factories = {
        MQTT_SECTION_NAME: MQTTConfig,
        EXPORTER_SECTION_NAME: ExporterConfig,
        c.default_section: TopicToMetricConfig
    }
    named_configs, ttms = ConfigLoader.parse_sections(c, configs_factories)
    mqtt_conf = named_configs[MQTT_SECTION_NAME]
    exporter_conf = named_configs[EXPORTER_SECTION_NAME]
    ttm_confs = list(ttms.values())
    return mqtt_conf, exporter_conf, ttm_confs


def main(args):
    log_level = logging.DEBUG if args.verbose else logging.INFO
    set_root_logger(log_level)

    conf = ConfigLoader.load_config(args.config)
    mqtt_conf, exporter_conf, ttm_confs = parse_config(conf)
    ttms = [TopicToMetric(conf) for conf in ttm_confs]

    mqttc = MQTTClient(mqtt_conf, ttms)
    mqttc.run_bg()

    set_wsgi_logger(exporter_conf)
    exporter = PrometheusExporter(exporter_conf, ttms)
    exporter.run()


def args_parse():
    description = 'Serve MQTT messages on specific topics as Prometheus metrics'
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--config', '-c', default='config.ini', metavar='PATH',
                        help='Path to configuration file, default is %(default)s')
    parser.add_argument('--verbose', '-v',
                        action='store_true', default=False,
                        help='Show debug output')
    return parser.parse_args()


if __name__ == '__main__':
    try:
        main(args_parse())
    except ConfigurationError as e:
        logging.error(e)
        sys.exit(1)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
    except KeyboardInterrupt:
        print('Stopping...')
