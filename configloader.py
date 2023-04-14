#!/usr/bin/env python3

import configparser
import logging
import re

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
