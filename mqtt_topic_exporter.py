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
import uuid
from wsgiref.simple_server import make_server, WSGIServer, WSGIRequestHandler

from paho.mqtt import client as mqtt


MQTT_SECTION_NAME = 'mqtt'
EXPORTER_SECTION_NAME = 'exporter'
WSGI_LOGGER_NAME = 'web-server'

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

    def _render_metric_header(self) -> str:
        conf = self.conf
        metric_header = f'#TYPE {conf.metric_name} {conf.metric_type}\n'
        if conf.metric_help:
            metric_header += f'#HELP {conf.metric_name} {conf.metric_help}\n'
        return metric_header

    def _render_metric_text(self) -> str:
        conf = self.conf
        metric_text = []
        for topic, metric in self.processed_topics.items():
            if conf.no_activity_timeout is not None:
                timeout = datetime.timedelta(seconds=conf.no_activity_timeout)
                if datetime.datetime.now() - metric.last_update > timeout:
                    logging.info(f'{topic} outdated more than {conf.no_activity_timeout} seconds, skipping')
                    continue
            if not metric.status_is_good:
                logging.info(f'{topic} has bad status, skipping')
                continue
            metric_text.append(f'{conf.metric_name}{{{metric.metric_labels}}} {metric.metric_value}\n')
        return ''.join(metric_text)

    def _valid_metric_value(self, value) -> bool:
        if not self.conf.only_float_values:
            return True
        try:
            float(value)
            return True
        except ValueError:
            return False

    def get_metric_text(self) -> str:
        text = self._render_metric_text()
        if text == '':
            return ''
        else:
            return self._render_metric_header() + text

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
            if not self._valid_metric_value(metric_value):
                logging.debug(f'on topic {topic} ignoring message with non-float value "{payload}"')
            metric = self.processed_topics[topic]
            metric.last_update = datetime.datetime.now()
            if conf.status_good_topic is None:
                metric.status_is_good = True
            metric.metric_labels = metric_labels
            metric.metric_value = metric_value
        logging.debug(f'{topic} {payload}')

    @classmethod
    def match_to_template(cls, pattern: str, template: str, text: str) -> Optional[str]:
        if re.match(pattern, text) is not None:
            return re.sub(pattern, template, text)
        else:
            logging.warning(f'payload "{text}" doesn\'t match pattern "{pattern}"')
            return None

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


class TryParse:

    @staticmethod
    def timeout(seconds):
        try:
            s = int(seconds)
        except ValueError as e:
            raise ValueError(f'non-integer time interval: {seconds}') from e
        if s >= 0:
            return s
        else:
            raise ValueError(f'negative time value {seconds}')

    @staticmethod
    def regexp(text):
        try:
            return re.compile(text)
        except re.error as e:
            raise ValueError(f'invalid regexp "{text}": {e}') from e

    @staticmethod
    def port(port):
        try:
            p = int(port)
        except ValueError as e:
            raise ValueError(f'port number must be an integer, got "{port}"') from e
        if 1 <= p and p <= 0xFFFF:
            return p
        else:
            raise ValueError(f'port value not in valid port range {port}')

    @staticmethod
    def loglevel(level):
        try:
            return getattr(logging, level)
        except AttributeError as e:
            raise ValueError(f'invalid loglevel {level}') from e


class MQTTConfig:

    def __init__(self, host, port=1883, keepalive=0, client_id=None, loglevel='INFO'):
        self.host = host
        self.port = TryParse.port(port)
        self.keepalive = TryParse.timeout(keepalive)
        if client_id is None:
            client_id = 'topic_exporter' + str(uuid.getnode())
        self.client_id = client_id
        self.loglevel = TryParse.loglevel(loglevel)


@dataclass
class ExporterConfig:
    metrics_path: str = '/metrics'
    bind_address: str = '0.0.0.0'
    port: int = 8840
    log_path: str = 'exporter.log'
    loglevel: str = 'INFO'

    def __post_init__(self):
        self.port = TryParse.port(self.port)
        self.loglevel = TryParse.loglevel(self.loglevel)


class ConfigurationError(Exception):
    '''Failure to open or parse configuration file'''


def load_config(path) -> configparser.ConfigParser:
    c = configparser.ConfigParser()
    # open explicitly to cause exception on error
    # configparser will silently ignore non-existing file
    try:
        with open(path, 'rt') as fp:
            c.read_file(fp)
    except OSError as e:
        raise ConfigurationError(f'Failed to load config file: {e}') from e
    return c


def try_parsing_section(name, factory_method, kwargs):
    try:
        return factory_method(**kwargs)
    except (ValueError, TypeError) as e:
        raise ConfigurationError(f'Error in section {name}: {e}') from e


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

        logger = logging.getLogger('paho.mqtt.client')
        logger.setLevel(config.loglevel)
        mqttc.enable_logger(logger)

        try:
            mqttc.connect(config.host, config.port, config.keepalive)
        except OSError as e:
            logging.warning(f'First connection to mqtt broker failed: {e}')

        return mqttc

    def run(self):
        mqttc = self._prepare_client()
        mqttc.loop_forever()

    def run_bg(self):
        '''run in separate thread'''
        mqttc = self._prepare_client()
        mqttc.loop_start()


class ThreadingWSGIServer(WSGIServer, ThreadingMixIn):
    """Thread per request HTTP server."""
    daemon_threads = True


class Handler(WSGIRequestHandler):

    # from https://github.com/python/cpython/blob/main/Lib/http/server.py
    _control_char_table = str.maketrans(
        {c: fr'\x{c:02x}' for c in itertools.chain(range(0x20), range(0x7f, 0xa0))})
    _control_char_table[ord('\\')] = r'\\'
    _logger = logging.getLogger(WSGI_LOGGER_NAME)

    def log_message(self, fmt, *args):
        message = (fmt % args)
        self._logger.info("%s - - [%s] %s\n" %
                          (self.address_string(),
                           self.log_date_time_string(),
                           message.translate(self._control_char_table)))


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
        elif route == '/':
            status = '200 OK'
            headers = [("Content-Type", "text/plain")]
            page = '/metrics'
        elif route == '/500':
            raise Exception('Testing 500 error')
        else:
            status = '404 Not Found'
            headers = [("Content-Type", "text/plain")]
            page = 'Not Found'
        start_response(status, headers)
        return [page.encode('utf8')]

    def run(self):
        conf = self.conf
        httpd = make_server(conf.bind_address, conf.port, self.app, ThreadingWSGIServer, Handler)
        with httpd:
            httpd.serve_forever()


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
