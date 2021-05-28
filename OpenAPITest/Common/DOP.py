#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import time
import requests
from Common.Login import login
from Common.ReadConfig import *
from Common.logger import get_logger
import json

logger = get_logger(__name__)


class DOP:
    def __init__(self, configure):
        self.s = login(configure)
        self.host = get_config_values(configure, "host")
        self.port = get_config_values(configure, "port")

    def get_columns(self, pusher_id):
        """
        获取订阅的表名和字段名
        :param pusher_id:
        :return: 返回表名和字段名组成的字典
        """
        try:
            response = self.s.get(url=f"http://{self.host}:{self.port}/openapi/pusher/{pusher_id}/tables.do")
        except Exception as e:
            print(e)
        else:
            data = response.json()["data"]
            return dict(zip([i["tableName"] for i in data], [i["columns"].split(",") for i in data]))

    @staticmethod
    def getConfigureInfo(pusher_id):
        """
        获取线上平台的订阅信息
        :param pusher_id:
        :return: data字典
        """
        s = requests.session()
        url = "http://openapi.bbdservice.com/openapi/admin/login.admin"
        params = {"username": "liqian@bbdservice.com",
                  "password": "iamadmin",
                  }
        rep = s.get(url=url, params=params)
        if rep.json()["error"] == 0:
            table_column = {}
            try:
                response = s.get(
                    f"http://openapi.bbdservice.com/openapi/admin/getPushById.admin?id={pusher_id}")
                pusher_info = response.json()["data"]
                res = s.get(f"http://openapi.bbdservice.com/openapi/pusher/{pusher_id}/tables.admin")
                info = res.json()["data"]
                for i in info:
                    table_column.update({i["tableName"]: i["columns"]})
                data = {
                    "item": 27,
                    "title": pusher_info[0]["title"],
                    "directory": None,
                    "pushmode": pusher_info[0]["pushmode"],
                    "comments": None,
                    "source": pusher_info[0]["source"],
                    "range": pusher_info[0]["range"],
                    "table_column": json.dumps(table_column)
                }
                s.close()
                return data, pusher_info[0]["handlerclassname"]
            except Exception as e:
                s.close()
                print(e)
        else:
            print("数据开放平台线上环境登录失败")
            raise

    @staticmethod
    def setUpPushTask(task_id, handler):
        """
        启用订阅
        :param task_id: pusher_id
        :param handler: handle名称
        :return:
        """
        host = get_config_values("DOPAfter", "host")
        port = get_config_values("DOPAfter", "port")
        data = {"id": task_id,
                "flume_headers_type": "bbd.dp.push.test.datashare.data",
                "handler_class_name": handler,
                "kafka_consumer_topic": None,
                "kafka_consumer_group_id": None,
                "kafka_consumer_auto_offset_reset": None,
                "path": "/user/bbdflume/dataplatform/pusher/test/datashare"}
        s = login("DOPAfter")
        try:
            if s:
                response = s.get(url="http://" + host + ":" + port + "/openapi/admin/setPusher.admin", params=data)
                logger.info(f"{task_id}配置成功，handler={handler}")
                if response.json()["error"] == 0:
                    response = s.get(url="http://" + host + ":" + port + "/openapi/admin/switchPusher.admin",
                                     params={"id": task_id, "isable": 1})
                    if response.json()["error"] == 0:
                        logger.info(f"{task_id}启用成功")
                    else:
                        logger.error(f"{task_id}启用失败")
                        raise
                else:
                    logger.error(f"{task_id}配置失败，handler={handler}")
                    raise
            else:
                raise
        except Exception as e:
            logger.error(f"程序发生异常", e)
            s.close()
            exit()
            print(f"程序发生异常", e)
        finally:
            s.close()

    def creatPushTask(self, table_column, handler):
        """
        创建订阅
        :param table_column: 表名这字段名组成的字典
        :param handler: handle名字
        :return:
        """
        url = f"http://{self.host}:{self.port}/openapi/user/tables.do"
        response = self.s.get(url=url)
        raw_data = response.json()["data"]
        # handler = get_config_values("PusherInfo", "handler")
        # table_column = eval(get_config_values("PusherInfo", "table_column"))
        for table in list(table_column.keys()):
            if table_column[table] is None:
                try:
                    table_id = raw_data[table]
                except Exception:
                    print(f"{table},在数据开放平台不存在,请联系产品")
                    exit()
                    raise
                response = self.s.get(
                    f"http://{self.host}:{self.port}/openapi/user/table/columns.do?ids={table_id}")
                raw_columns = response.json()["data"]
                columns = ",".join([i["column"] for i in eval(raw_columns[str(table_id)])])
                table_column[table] = columns
        data = {
            "item": 27,
            "title": get_config_values("PusherInfo", "title"),
            "directory": None,
            "pushmode": get_config_values("PusherInfo", "pushmode"),
            "comments": None,
            "source": get_config_values("PusherInfo", "source"),
            "range": get_config_values("PusherInfo", "range"),
            "table_column": json.dumps(table_column)
        }
        task_id = get_config_values("PusherInfo", "test_pusher_id")
        if task_id:  # 判断配置文件中是否有配置订阅id，没有配置则创建
            response = self.s.get(url="http://" + self.host + ":" + self.port + "/openapi/getPusherByUser.do",
                                  params={"page_no": 0, "page_size": 1000, "query": ""})
            response = response.json()
            if response["error"] == 0:
                id_list = [i["id"] for i in response["data"]]
            else:
                id_list = []
            self.s.headers.update({"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"})
            if int(task_id) in id_list:  # 判断配置文件中配置的订阅id是否已经存在，存在则更新，不存在则创建
                data.update({"pusherId": int(task_id)})
                self.s.post(url=f"http://{self.host}:{self.port}/openapi/updatePusherAndPusherTable.do", data=data)
                self.setUpPushTask(task_id, handler)
                modify_config("application-spark-offline", "id", task_id)
                modify_properties("offline.push.subid", str(task_id))
            else:
                self.s.headers.update({"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"})
                res = self.s.post(url="http://" + self.host + ":" + self.port + "/openapi/addPusher.do", data=data)
                if res.json()["error"] == 0:
                    response = self.s.get(
                        url="http://" + self.host + ":" + self.port + "/openapi/getPusherByUser.do",
                        params={"page_no": 0, "page_size": 10, "query": data["title"]})
                    if response.json()["error"] == 0:
                        task_id = response.json()["data"][0]["id"]
                        self.setUpPushTask(task_id, handler)
                        modify_config("PusherInfo", "test_pusher_id", task_id)
                        modify_config("application-spark-offline", "id", task_id)
                        modify_properties("offline.push.subid", str(task_id))
        else:
            self.s.headers.update({"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"})
            res = self.s.post(url="http://" + self.host + ":" + self.port + "/openapi/addPusher.do", data=data)
            if res.json()["error"] == 0:
                response = self.s.get(url="http://" + self.host + ":" + self.port + "/openapi/getPusherByUser.do",
                                      params={"page_no": 0, "page_size": 10, "query": data["title"]})
                if response.json()["error"] == 0:
                    task_id = response.json()["data"][0]["id"]
                    self.setUpPushTask(task_id, handler)
                    modify_config("PusherInfo", "test_pusher_id", task_id)  # 修改配置文件中PusherInfo的test_pusher_id
                    modify_config("application-spark-offline", "id", task_id)  # 修改配置文件中application-spark-offline的id
                    modify_properties("offline.push.subid", str(task_id))  # 修改推送程序的配置文件中的offline.push.subid
