#!/usr/bin/env python3

from collections import defaultdict
import configparser
import logging
import re
from typing import Any, Dict, List, Tuple

class ConfigurationError(Exception):
    '''Failure to open or parse configuration file'''


class ConfigLoader:

    @staticmethod
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

    @staticmethod
    def try_getting_section(config: configparser.ConfigParser, name, default=None):
        section = config.get(name, default)
        if section is None:
            msg = f'Missing non-arbitrary section "{name}" in config file'
            raise ConfigurationError(msg)

    @staticmethod
    def try_parsing_section(name, factory_method, kwargs):
        try:
            return factory_method(**kwargs)
        except (ValueError, TypeError) as e:
            raise ConfigurationError(f'Error in section {name}: {e}') from e

    @classmethod
    def parse_sections(cls, config: configparser.ConfigParser, sections_factories: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        '''Make configuration objects from Configparser instance according to `sections_factories` mapping.
        Keys should be section names, corresponding values are factory methods
        to create specific config object from section dictionary.

        Value from `ConfigParser.default_section` key is used as default to create
        config from any section not specified in `sections_factories` directly.

        Keys from `section_factories` that do not have corresponding
        sections in config file are treated the same as having empty sections.

        Return values are two mapping {"section name": specific_config_instance}
        one for sections, defined in `sections_factories` and another for the rest,
        created with factory from `default_section`'''

        named_configs = {}
        normal_configs = {}
        for name, conf in config.items():
            if name == config.default_section:
                continue
            if name in sections_factories:
                factory = sections_factories[name]
                named_configs[name] = cls.try_parsing_section(name, factory, conf)
            else:
                factory = sections_factories[config.default_section]
                normal_configs[name] = cls.try_parsing_section(name, factory, conf)
        for name, factory in sections_factories.items():
            if name == config.default_section or name in named_configs:
                continue
            try:
                named_configs[name] = factory()
            except TypeError as e:
                raise ConfigurationError(f'config is missing non-arbitrary section {name}') from e
        return named_configs, normal_configs


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
