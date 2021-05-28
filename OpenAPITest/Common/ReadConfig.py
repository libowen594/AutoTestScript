#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import os
import configparser


def get_config_values(section, option):
    """
    根据传入的section获取对应的value
    :param option:
    :param section: ini配置文件中用[]标识的内容
    :return:
    """
    config = configparser.ConfigParser()
    config.read("../config.ini", encoding="UTF-8")

    return config.get(section=section, option=option)


def get_options(section):
    """
    获取section下所有的options
    :param section:
    :return:
    """
    config = configparser.ConfigParser()
    config.read("../config.ini", encoding="UTF-8")
    options = config.options(section=section)
    return options


def modify_properties(key, value):
    """
    修改推送程序配置文件
    :param key: 需要修改的key
    :param value:
    :return:
    """
    with open("../application-offline.properties", "r", encoding="UTF-8") as fp:
        lines = fp.readlines()
    for i in range(0, len(lines)):
        if not lines[i].startswith("#") and key in lines[i]:
            lines[i] = f"{key}={value}\n"
    os.remove("../application-offline.properties")
    with open("../application-offline.properties", "w", encoding="UTF-8") as fp:
        for line in lines:
            fp.write(line)


def modify_config(section, option, value):
    """
    修改脚本配置文件
    :param section: 
    :param option:
    :param value:
    :return:
    """
    config = configparser.ConfigParser()
    config.read("../config.ini", encoding="UTF-8")
    config.set(section, option, value)
    with open("../config.ini", "r+", encoding="UTF-8") as fd:
        config.write(fd)
    # config.write(open("../config.ini", "r+", encoding="UTF-8", closefd=True))






