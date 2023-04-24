#!/usr/bin/env python3

import argparse
import configparser
import logging
import logging.handlers
import re
import subprocess
import sys
from dataclasses import dataclass
from typing import List, Tuple

from configloader import ConfigLoader, ConfigurationError, set_root_logger
from mqttcmd import Action, MQTTClient, MQTTConfig

MQTT_SECTION_NAME = 'mqtt'

@dataclass
class CommandConfig:

    topic: str
    cmd: str
    payload: str = r'.*'


class Command(Action):

    def __init__(self, topic, cmd, payload):
        self.topic = topic
        self.payload = payload
        self.cmd = cmd
        self.subprocess = None

    def on_message(self, topic: str, payload: str):
        logging.debug(f'[MQTT] {topic} {payload}')
        if re.match(self.payload, payload) is not None:
            logging.debug(f'on topic {topic} run command {self.cmd}')
            self._run_subprocess()
        else:
            logging.debug(f'ignoring command on topic {topic} because payload {payload} doesn\'t match template')

    def get_topic(self):
        return self.topic

    def _run_subprocess(self):
        if self.subprocess is None or self.subprocess.poll() is not None:
            self.subprocess = subprocess.Popen(self.cmd, shell=True)
        else:
            logging.info(f'previous command on topic {self.topic} still running, ignoring new command')


def parse_config(c: configparser.ConfigParser) -> Tuple[
        MQTTConfig, List[Command]]:
    configs_factories = {
        MQTT_SECTION_NAME: MQTTConfig,
        c.default_section: CommandConfig
    }
    named_configs, cmd_configs = ConfigLoader.parse_sections(c, configs_factories)
    mqtt_conf = named_configs[MQTT_SECTION_NAME]
    command_confs = list(cmd_configs.values())
    return mqtt_conf, command_confs


def main(args):
    log_level = logging.DEBUG if args.verbose else logging.INFO
    set_root_logger(log_level)

    conf = ConfigLoader.load_config(args.config)
    mqtt_conf, command_confs = parse_config(conf)
    commands = [Command(**conf.__dict__) for conf in command_confs]

    mqttc = MQTTClient(mqtt_conf, commands)
    mqttc.run()


def args_parse():
    description = 'Run pre-defined command when receiving MQTT messages on specific topics'
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
