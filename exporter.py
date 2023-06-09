#!/usr/bin/env python3

import datetime
import itertools
import logging
import logging.handlers
from dataclasses import dataclass
from socketserver import ThreadingMixIn
from typing import List
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from configloader import TryParse

WSGI_LOGGER_NAME = 'web-server'


class Metric:

    def __init__(self, metric_name, metric_help='', metric_type='Gauge', timeout=None):
        self.metric_name = metric_name
        self.metric_help = metric_help
        self.metric_type = metric_type
        self.no_activity_timeout = timeout

        self.metric_labels: str = ''
        self.metric_value: str = ''
        self.last_update = datetime.datetime.fromtimestamp(0)

    def _render_header(self) -> str:
        metric_header = f'#TYPE {self.metric_name} {self.metric_type}\n'
        if self.metric_help:
            metric_header += f'#HELP {self.metric_name} {self.metric_help}\n'
        return metric_header

    def _render_body(self) -> str:
        return f'{self.metric_name}{{{self.metric_labels}}} {self.metric_value}'

    def _valid_metric_value(self) -> bool:
        try:
            float(self.metric_value)
            return True
        except ValueError:
            return False

    def get_metric_text(self, skip_header=False) -> str:
        header = self._render_header() if not skip_header else ''
        body = self._render_body()

        if self.no_activity_timeout is not None:
            timeout = datetime.timedelta(seconds=self.no_activity_timeout)
            if datetime.datetime.now() - self.last_update > timeout:
                logging.debug(f'[skip metric: no activity] {body}')
                return ''
        if not self._valid_metric_value():
            logging.debug(f'[skip metric: non-numeric value] {body}')
            return ''

        logging.debug(f'[render metric] {body}')
        return header + body + '\n'

    def update(self, labels: str, value):
        self.metric_labels = labels
        self.metric_value = value
        self.last_update = datetime.datetime.now()
        logging.debug(f'[update metric] {self._render_body()}')


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


def set_wsgi_logger(exporter_conf: ExporterConfig):
    handler = logging.handlers.RotatingFileHandler(
        exporter_conf.log_path, maxBytes=100000, backupCount=3)
    formatter = logging.Formatter('%(message)s')
    handler.setFormatter(formatter)
    server_logger = logging.getLogger(WSGI_LOGGER_NAME)
    server_logger.addHandler(handler)
    server_logger.setLevel(exporter_conf.loglevel)
    server_logger.propagate = False


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

    def __init__(self, conf: ExporterConfig, metrics: List[Metric]):
        self.metrics = metrics
        self.conf = conf

    def metrics_text(self) -> str:
        metrics = []
        for metric in self.metrics:
            metric_text = metric.get_metric_text()
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
            page = f'{self.conf.metrics_path}'
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
        logging.info(f'Starting web-server at {conf.bind_address}:{conf.port}{self.conf.metrics_path}')
        httpd = make_server(conf.bind_address, conf.port, self.app, ThreadingWSGIServer, Handler)
        with httpd:
            httpd.serve_forever()
