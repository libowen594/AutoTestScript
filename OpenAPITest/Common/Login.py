#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'
import requests
from Common.ReadConfig import get_config_values
from Common.logger import get_logger

logger = get_logger(__name__)


def login(platform):
    """
    :param platform: 需要登录的平台中文名称
    :return: s: 带cookies的一个对话session
    """
    host = get_config_values(platform, "host")
    port = get_config_values(platform, "port")
    user = get_config_values(platform, "username")
    password = get_config_values(platform, "password")
    api = get_config_values(platform, "loggingAPI")
    s = requests.session()
    url = "http://" + host + ":" + port + api
    if platform == "DOPFront":
        data = {"email": user,
                "password": password,
                }
    elif platform == "DMPTest" or platform == "DMPOfficial" or platform == "DOPAfter":
        data = {"username": user,
                "password": password,
                }
    else:
        logger.error("传入参数错误")
        raise
    rep = s.get(url=url, params=data)
    if platform == "DOPFront" or platform == "DOPAfter":
        if rep.json()["error"] == 0:
            flag = True
        else:
            flag = False
    elif platform == "DMPTest" or platform == "DMPOfficial":
        if rep.json()["code"] == 0:
            flag = True
        else:
            flag = False
    else:
        raise
    if flag:
        logger.info(f"{platform}登录成功")
        return s
    else:
        logger.info(f"{platform}登录失败: url=http://{host}:{port}{api} 请求参数={data}")
        return None


