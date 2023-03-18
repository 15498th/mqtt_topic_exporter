#!/usr/bin/env python3

import asyncio
import configparser
from collections import defaultdict
from dataclasses import dataclass
import datetime
import logging
import re
from socketserver import ThreadingMixIn
from typing import Dict, List, Optional, Callable, Tuple
import uuid
from wsgiref.simple_server import make_server, WSGIServer

from paho.mqtt import client as mqtt


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
    topic_payload_separator = ' '

    # do not generate metric if last message on topic
    # was more than no_activity_timeout seconds ago
    no_activity_timeout: Optional[int] = None

    # generate metric after status_good_payload received on status_good_topic
    # until status_bad_payload received on status_bad_topic
    status_good_topic: Optional[str] = None
    status_good_payload: Optional[str] = None
    status_bad_topic: Optional[str] = None
    status_bad_payload: Optional[str] = None

    def __post_init__(self):
        if self.no_activity_timeout is not None:
            self.no_activity_timeout = int(self.no_activity_timeout)

@dataclass
class Metric:
    metric_labels: str = ''
    metric_value: str = ''
    last_update = datetime.datetime.fromtimestamp(0)
    status_is_good: bool = False
    
class TopicToMetric:

    def __init__(self, conf: TopicToMetricConfig):
        self.conf = conf
        self.processed_topics: Dict[str, Metric] = defaultdict(Metric)

    def get_metric_text(self) -> str:
        conf = self.conf
        metric_text = f'#TYPE {conf.metric_name} {conf.metric_type}\n'
        if conf.metric_help:
            metric_text += f'#HELP {conf.metric_name} {conf.metric_help}\n'
        for topic, metric in self.processed_topics.items():
            if conf.no_activity_timeout is not None:
                timeout = datetime.timedelta(seconds=conf.no_activity_timeout)
                if datetime.datetime.now() - metric.last_update > timeout:
                    logging.info(f'{topic} outdated more than {conf.no_activity_timeout} seconds, skipping')
                    continue
            if not metric.status_is_good:
                logging.info(f'{topic} has bad status, skipping')
                continue
            metric_text += f'{conf.metric_name}{{{metric.metric_labels}}} {metric.metric_value}\n' 
        return metric_text

    def on_message(self, topic: str, payload: str):
        conf = self.conf
        metric = self.processed_topics[topic]
        metric.last_update = datetime.datetime.now()
        if conf.status_good_topic is None:
            metric.status_is_good = True
        combined = topic + conf.topic_payload_separator + payload
        metric.metric_labels = self.match_to_template(
            conf.topic_payload_pattern,
            conf.labels_template,
            combined)
        metric.metric_value = self.match_to_template(
            conf.topic_payload_pattern,
            conf.value_template,
            combined)
        logging.debug(topic, payload)

    @classmethod
    def match_to_template(cls, pattern: str, template: str, text: str) -> str:
        if re.match(pattern, text) is not None:
            return re.sub(pattern, template, text)
        else:
            logging.warning(f'payload "{text}" doesn\'t match pattern "{pattern}"')
            return ''

    def _set_status(self, is_good: bool):
        for topic, metric in self.processed_topics.items():
            if metric.status_is_good != is_good:
                metric.status_is_good = is_good
                status = 'good' if is_good else 'bad'
                logging.info(f'{metric} status changed to {status} by status topic message')

    def on_status_good(self, topic, payload):
        if self.conf.status_good_payload == payload:
            self._set_status(True)

    def on_status_bad(self, topic, payload):
        if self.conf.status_bad_payload == payload:
            self._set_status(False)


class MQTTConfig:

    def __init__(self, host, port=1883, keepalive=0, client_id=None):
        self.host = host
        self.port = int(port)
        self.keepalive = keepalive
        if client_id is None:
            client_id = 'topic_exporter' + str(uuid.getnode())
        self.client_id = client_id


@dataclass
class ExporterConfig:
    metrics_path: str = '/metrics'
    bind_address: str = '0.0.0.0'
    port: int = 8840

    def __post_init__(self):
        self.port = int(self.port)


def load_config(path) -> Tuple[MQTTConfig, ExporterConfig, List[TopicToMetric]]:
    c = configparser.ConfigParser()
    # open explicitly to cause exception on error
    # configparser will silently ignore non-existing file
    with open(path, 'rt') as fp:
        c.read_file(fp)
    mqtt_conf = MQTTConfig(**c[MQTT_SECTION_NAME])
    exporter_section = c[EXPORTER_SECTION_NAME] if c.has_section(EXPORTER_SECTION_NAME) else {}
    exporter_conf = ExporterConfig(**exporter_section)
    ttms = []
    for name, conf in c.items():
        if name in (c.default_section, MQTT_SECTION_NAME, EXPORTER_SECTION_NAME):
            continue
        ttm_config = TopicToMetricConfig(**conf)
        ttm = TopicToMetric(ttm_config)
        ttms.append(ttm)
    return mqtt_conf, exporter_conf, ttms


class MQTTClient:

    def __init__(self, conf: MQTTConfig, ttms: List[TopicToMetric]):
        self.conf = conf
        self.ttms = ttms

    def on_connect(self, mqttc, userdata, flags, rc):
        logging.info(f'connected, rc: {rc}')
        for ttm in self.ttms:
            mqttc.subscribe(ttm.conf.topic)
            on_message = self.wrap_on_message(ttm.on_message)
            mqttc.message_callback_add(ttm.conf.topic, on_message)
            if ttm.conf.status_good_topic is not None:
                mqttc.subscribe(ttm.conf.status_good_topic)
                on_status_good = self.wrap_on_message(ttm.on_status_good)
                mqttc.message_callback_add(ttm.conf.status_good_topic, on_status_good)
            if ttm.conf.status_bad_topic is not None:
                mqttc.subscribe(ttm.conf.status_bad_topic)
                on_status_bad = self.wrap_on_message(ttm.on_status_bad)
                mqttc.message_callback_add(ttm.conf.status_bad_topic, on_status_bad)

    def on_disconnect(self, mqttc, userdata, rc):
        logging.warning(f'disconnected, rc: {rc}')

    def wrap_on_message(self, func):
        def on_message(mqttc, userdata, msg):
            return func(msg.topic, msg.payload.decode())
        return on_message

    def _prepare_client(self):
        config = self.conf

        mqttc = mqtt.Client(config.client_id)
        mqttc.on_connect = self.on_connect
        mqttc.on_disconnect = self.on_disconnect

        mqttc.connect(config.host, config.port, config.keepalive)

        return mqttc

    def run(self):
        mqttc = self._prepare_client()
        mqttc.loop_forever()

    def run_bg(self):
        '''run in separate thread'''
        mqttc = self._prepare_client()
        mqttc.loop_start()


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
        """Thread per request HTTP server."""
        daemon_threads = True


class PrometheusExporter:

    def __init__(self, conf: ExporterConfig, ttms: List[TopicToMetric]):
        self.ttms = ttms
        self.conf = conf

    def metrics_text(self) -> str:
        metrics = []
        for ttm in self.ttms:
            metric_text = ttm.get_metric_text()
            metrics.append(metric_text)
        return ''.join(metrics)

    def app(self, environ, start_response):
        route = environ['PATH_INFO']
        if route == self.conf.metrics_path:
            headers = [("Content-type", "text/plain; charset=utf-8")]
            status = '200 OK'
            page = self.metrics_text()
        elif route == '/favicon.ico':
            status = '200 OK'
            headers = [('', '')]
            page = ''
        else:
            status = '404 Not Found'
            headers = [("Content-Type", "text/plain")]
            page = 'Not Found'
        start_response(status, headers)
        return [page.encode('utf8')]

    def run(self):
        conf = self.conf
        httpd = make_server(conf.bind_address, conf.port, self.app, ThreadingWSGIServer)
        with httpd:
            httpd.serve_forever()


def main():
    set_logger()
    mqtt_conf, exporter_conf, ttms = load_config('config.ini')
    mqttc = MQTTClient(mqtt_conf, ttms)
    mqttc.run_bg()
    exporter = PrometheusExporter(exporter_conf, ttms)
    exporter.run()

def set_logger(log_level=logging.INFO):
    log_format = '[%(asctime)s] %(message)s'
    datefmt = '%Y/%m/%d %H:%M:%S'
    logging.basicConfig(level=log_level, format=log_format, datefmt=datefmt)

if __name__ == '__main__':
    main()
