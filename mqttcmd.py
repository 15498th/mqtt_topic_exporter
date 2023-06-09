#!/usr/bin/env python3

import logging
import logging.handlers
import uuid
from abc import abstractmethod
from typing import List

from paho.mqtt import client as mqtt

from configloader import TryParse

MQTT_LOGGER_NAME = 'paho.mqtt.client'

class MQTTConfig:

    def __init__(self, host, port=1883, keepalive=0, client_id=None, loglevel='INFO', username=None, password=None):
        self.host = host
        self.port = TryParse.port(port)
        self.keepalive = TryParse.timeout(keepalive)
        if client_id is None:
            client_id = 'mqtt-cmd-' + str(uuid.getnode())
        self.client_id = client_id
        self.loglevel = TryParse.loglevel(loglevel)
        self.username = username
        self.password = password


class Action:
    '''unit of work for handling MQTT messages on specific topic'''

    @abstractmethod
    def get_topic(self):
        '''return topic to subscribe for this action'''

    @abstractmethod
    def on_message(self, topic: str, payload: str):
        '''called for each message on specified topic'''


class MQTTClient:

    def __init__(self, conf: MQTTConfig, uows: List[Action]):
        self.conf = conf
        self.uows = uows

    def on_connect(self, mqttc, userdata, flags, rc):
        # no point checkin rc here: on_disconnect will be called on error
        logging.info(f'Connected to MQTT broker {self.conf.host}:{self.conf.port}')
        for uow in self.uows:
            mqttc.subscribe(uow.get_topic())
            on_message = self.wrap_on_message(uow.on_message)
            mqttc.message_callback_add(uow.get_topic(), on_message)

    def on_disconnect(self, mqttc, userdata, rc):
        reason = mqtt.error_string(rc)
        logging.warning(f'Disconnected from MQTT broker {self.conf.host}:{self.conf.port}. {reason}')

    def wrap_on_message(self, func):
        def on_message(mqttc, userdata, msg):
            return func(msg.topic, msg.payload.decode())
        return on_message

    def _prepare_client(self):
        config = self.conf

        mqttc = mqtt.Client(config.client_id)
        mqttc.on_connect = self.on_connect
        mqttc.on_disconnect = self.on_disconnect

        logger = logging.getLogger(MQTT_LOGGER_NAME)
        logger.setLevel(config.loglevel)
        mqttc.enable_logger(logger)

        if self.conf.username is not None:
            mqttc.username_pw_set(self.conf.username, self.conf.password)

        try:
            mqttc.connect(config.host, config.port, config.keepalive)
        except OSError as e:
            logging.warning(f'First connection to MQTT broker {config.host}:{config.port} failed: {e}')
            logging.warning('Check if configuration is correct and server is running')
            logging.warning('Will keep retrying in case this is temporarly error')

        return mqttc

    def run(self):
        '''run blocking'''
        mqttc = self._prepare_client()
        mqttc.loop_forever()

    def run_bg(self):
        '''run in separate thread'''
        mqttc = self._prepare_client()
        mqttc.loop_start()

