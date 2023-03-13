#!/usr/bin/env python3

import asyncio
import configparser
from dataclasses import dataclass
import datetime
import logging
import re
from typing import List, Optional, Callable
import uuid

from paho.mqtt import client as mqtt


MQTT_SECTION_NAME = 'mqtt'

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


class TopicToMetric:

    def __init__(self, conf: TopicToMetricConfig):
        self.conf = conf
        self.last_update = datetime.datetime.fromtimestamp(0)
        self.status_is_good = False
        self.metric_labels = ''
        self.metric_value = ''

    def get_metric_text(self) -> Optional[str]:
        conf = self.conf
        if conf.no_activity_timeout is not None:
            timeout = datetime.timedelta(seconds=conf.no_activity_timeout)
            if datetime.datetime.now() - self.last_update > timeout:
                return None
        if not self.status_is_good:
            return None
        metric_text = f'#TYPE {conf.metric_name} {conf.metric_type}\n'
        if conf.metric_help:
            metric_text += f'#HELP {conf.metric_name} {conf.metric_help}\n'
        metric_text += f'{conf.metric_name}{{{self.metric_labels}}} {self.metric_value}\n' 
        return metric_text

    def on_message(self, topic: str, payload: str):
        conf = self.conf
        self.last_update = datetime.datetime.now()
        if conf.status_good_topic is None:
            self.status_is_good = True
        combined = topic + conf.topic_payload_separator + payload
        self.metric_labels = self.match_to_template(
            conf.topic_payload_pattern,
            conf.labels_template,
            combined)
        self.metric_value = self.match_to_template(
            conf.topic_payload_pattern,
            conf.value_template,
            combined)
        print(self.get_metric_text(), end='')

    @classmethod
    def match_to_template(cls, pattern: str, template: str, text: str) -> str:
        if re.match(pattern, text) is not None:
            return re.sub(pattern, template, text)
        else:
            logging.warning(f'payload "{text}" doesn\'t match pattern "{pattern}"')
            return ''

    def on_status_good(self, topic, payload):
        if self.conf.status_good_payload == payload:
            self.status_is_good = True

    def on_status_bad(self, topic, payload):
        if self.conf.status_bad_payload == payload:
            self.status_is_good = False


class MQTTConfig:

    def __init__(self, host, port=1883, keepalive=0, client_id=None):
        self.host = host
        self.port = int(port)
        self.keepalive = keepalive
        if client_id is None:
            client_id = 'topic_exporter' + str(uuid.getnode())
        self.client_id = client_id


def load_config(path) -> List[TopicToMetric]:
    c = configparser.ConfigParser()
    # open explicitly to cause exception on error
    # configparser will silently ignore non-existing file
    with open(path, 'rt') as fp:
        c.read_file(fp)
    mqtt_conf = MQTTConfig(**c[MQTT_SECTION_NAME])
    ttms = []
    for name, conf in c.items():
        if name in (c.default_section, MQTT_SECTION_NAME):
            continue
        ttm_config = TopicToMetricConfig(**conf)
        ttm = TopicToMetric(ttm_config)
        ttms.append(ttm)
    return mqtt_conf, ttms


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

    def run(self):
        config = self.conf

        mqttc = mqtt.Client(config.client_id)
        mqttc.on_connect = self.on_connect
        mqttc.on_disconnect = self.on_disconnect

        mqttc.connect(config.host, config.port, config.keepalive)
        mqttc.loop_forever()


def main():
    conf, ttms = load_config('config.ini')
    mqttc = MQTTClient(conf, ttms)
    mqttc.run()

if __name__ == '__main__':
    main()
