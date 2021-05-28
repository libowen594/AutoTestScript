#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import logging
import datetime
import os
from Common.ReadConfig import get_config_values


def get_logger(name):
    pushLogToConsole = eval(get_config_values("Script-configuration", "pushLogToConsole"))
    _BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    logfileName = f"{_BASE_DIR}/Log/log_{datetime.datetime.today().strftime('%Y-%m-%d')}.txt"
    logger = logging.getLogger(name)
    logger.setLevel(level=logging.INFO)
    handler = logging.FileHandler(logfileName)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s[line:%(lineno)d]: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if pushLogToConsole:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatter)
        logger.addHandler(console)
    return logger
