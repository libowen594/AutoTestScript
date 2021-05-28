#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import datetime
from dateutil.relativedelta import relativedelta
from Common.Login import login
import random
from Common.ReadConfig import get_config_values
import requests
from Common.logger import get_logger

logger = get_logger(__name__)


class DMP:
    def __init__(self, configure):
        self.s = login(configure)
        self.host = get_config_values(configure, "host")
        self.port = get_config_values(configure, "port")

    def getBbdDataUniqueId(self, crawler_table_name, bbd_type=None):
        """
        获取UniqueId
        :param crawler_table_name: 爬虫表的名称
        :param bbd_type: bbd_type
        :return: UniqueId
        """
        logger.info(f"开始查找{crawler_table_name}的数据")
        date = datetime.datetime.today()
        data = {"page_no": 0,
                "page_size": 100,
                "index": "",
                "table": "dp-crawler-datas",
                "sort": "bbd_dotime=desc",
                "rangequery": "",
                "query": crawler_table_name,
                "fields": "bbd_table",
                "filter": ""}
        while date >= datetime.datetime.strptime("2018-01-01", "%Y-%m-%d"):
            data["index"] = "dp-crawler-datas-{}".format(date.strftime("%Y%m"))
            if bbd_type:
                data["filter"] = f"bbd_type={bbd_type}"
            response = self.s.get(url="http://" + self.host + ":" + self.port + "/es/advancedquery.do", params=data)
            if response.json()["total"] == 0:
                date = date - relativedelta(months=+1)
            else:
                logger.info(f"在爬虫库dp-crawler-datas-{date.strftime('%Y%m')}找到表{crawler_table_name}数据")
                return random.choice(response.json()["data"])["hbase_rowkey"], date.strftime("%Y%m")
            logger.info(f"结束查找{crawler_table_name}的数据")
        else:
            logger.info(f"结束查找{crawler_table_name}的数据")
            return

    def getRowDate(self, bbd_data_unique_id, date):
        """
        通过unique_id获取原始数据
        :param bbd_data_unique_id: unique_id
        :param date: 数据所在的爬虫库，后面的日期
        :return: 转换后的数据，格式为字典
        """
        params = {"dbtype": "hbase",
                  "index": "dp-crawler-datas-{}".format(date),
                  "table": "crawler_data_for_{}".format(date),
                  "rk": bbd_data_unique_id}
        try:
            response = self.s.get(url="http://" + self.host + ":" + self.port + "/es/geteshbaserow.do", params=params)
        except Exception as e:
            print(f"获取rowDate错误：{e}")
            raise
        else:
            row_data = self.__convertToJson(response.json())  # 转换数据
            return row_data

    def searchTableId(self, table_name):
        """
        获取table的id
        :param table_name: table_name
        :return:
        """
        table_id = None
        data = {"page_no": 0,
                "page_size": 100,
                "query": table_name,
                }
        response = self.s.get(url="http://" + self.host + ":" + self.port + "/model/queryModelTable.do", params=data)
        if response.json()["data"]:
            for i in response.json()["data"]:
                if i["model_table_name"] == table_name:
                    table_id = i["id"]
            if table_id:
                logger.info(f"{table_name}的id={table_id}")
                return table_id
            else:
                logger.info(f"在数据管理平台没有找到业务表{table_name}")
                return "在数据管理平台没有找到业务表"
        else:
            logger.info(f"在数据管理平台没有找到业务表{table_name}")
            return

    def getModelGroup(self, table_id):
        """
        根据table_id获取入库分组信息(清洗规则)
        :param table_id: table_id
        :return: 清洗规则列表
        """
        groups = []
        data = {"page_no": 0,
                "page_size": 100,
                "query": "",
                "querysource": "",
                "tbid": table_id}
        response = self.s.get(url="http://" + self.host + ":" + self.port + "/model/queryModelGroup.do", params=data)
        if response.json()["data"]:
            for i in response.json()["data"]:
                logger.info(f"id={table_id}业务表的清洗规则id={i['model_group_id']}")
                groups.append(i["model_group_id"])
            return groups
        else:
            logger.info(f"没有找到id={table_id}业务表的清洗规则")
            return "没有找到业务表的清洗规则"

    def searchCrawlerTable(self, table_id, group_ids):
        """
        获取爬虫表和对应的bbd_type
        :param table_id: 表的id
        :param group_ids: 清晰规则list
        :return: 表名称和对应的bbd_type组成的元组列表
        """
        tables = []
        for group_id in group_ids:
            data = {"page_no": 0,
                    "page_size": 100,
                    "groupid": group_id,
                    "tbid": table_id,
                    "query": ""}
            response = self.s.get(url="http://" + self.host + ":" + self.port + "/model/queryModelGroupSource.do",
                                  params=data)
            if response.json()["data"]:
                for i in response.json()["data"]:
                    if i["selectid"] != "0":
                        tables.append((i["table_name"], i["type_name"]))
            else:
                logger.info(f"id={table_id}的业务表在清洗规则id={group_id}下没有找到对应的爬虫表")
        return tables

    def getCrawlerTableName(self, table_name):
        """
        根据表命获取，爬虫表和bbd_type
        :param table_name: 表名称
        :return: 表名称和对应的bbd_type组成的元组列表
        """
        logger.info(f"开始查找{table_name}对应的爬虫表")
        table_id = self.searchTableId(table_name)
        if table_id:
            groups = self.getModelGroup(table_id)
        else:
            return "在数据管理平台没有找到业务表"
        if isinstance(groups, list):
            tables = self.searchCrawlerTable(table_id, groups)
        else:
            return groups
        if tables:
            return tables
        else:
            return "数据源配置未勾选或未配置数据源"

    def get_bbd_qyxx_id(self, table_name, c_table):
        """
        获取qyxx表的入库数据，进行入库
        :param table_name:
        :param c_table:
        :return: qyxx_id
        """
        if table_name.startswith("qyxx_element"):
            tag = table_name.split("_")[-1] + "s"
        else:
            tag = table_name.split("_")[-1]
        date = datetime.datetime.today()
        page_no = 0
        data = {"page_no": page_no,
                "page_size": 100,
                "index": "",
                "table": "dp-crawler-datas",
                "sort": "bbd_dotime=desc",
                "rangequery": "",
                "query": c_table,
                "fields": "bbd_table",
                "filter": ""}
        while date >= datetime.datetime.strptime("2018-01-01", "%Y-%m-%d"):
            data["index"] = "dp-crawler-datas-{}".format(date.strftime("%Y%m"))
            response = self.s.get(url="http://" + self.host + ":" + self.port + "/es/advancedquery.do",
                                  params=data).json()
            total = response["total"]
            if total == 0:
                date = date - relativedelta(months=+1)
                continue
            hbase_rowkey_list = [i["hbase_rowkey"] for i in response["data"]]
            for row_key in hbase_rowkey_list:
                params = {"dbtype": "hbase",
                          "index": "dp-crawler-datas-{}".format(date.strftime("%Y%m")),
                          "table": "crawler_data_for_{}".format(date.strftime("%Y%m")),
                          "rk": row_key}
                try:
                    res = self.s.get(url="http://" + self.host + ":" + self.port + "/es/geteshbaserow.do",
                                     params=params)
                except Exception as e:
                    print(f"获取rowDate错误：{e}")
                    raise
                else:
                    row_data = self.__convertToJson(res.json())
                    if eval(row_data.get(tag, "None")):
                        res = self.dataStorage(row_data)
                        if res:
                            return res.get("raw_data").get("bbd_qyxx_id")
                        else:
                            continue
            else:
                if (page_no + 1) * 100 > total:
                    date = date - relativedelta(months=+1)
                    continue
                data["page_no"] = page_no + 1
        else:
            return None

    @staticmethod
    def __convertToJson(content):
        try:
            result = {}
            data = content["data"]
            if not isinstance(data, list):
                data = data.get(data)
            for line in data:
                key = line.get("qualifier")
                value = line.get("value")
                family = line.get("family")
                if family == "info":
                    result.update({key: value})
                else:
                    if family in result.keys():
                        result[family][key] = value
                    else:
                        result[family] = {}
            logger.info(f"数据转换为json成功：{data}")
            return result
        except Exception as e:
            print(f"数据转换为json失败，原数据：{content}")
            logger.info(f"数据转换为json失败，原数据：{content}")
            logger.info(f"错误信息：{e}")
            raise

    @staticmethod
    def dataStorage(data):
        """
        输入入库
        :param data:
        :return: 成功返回xgxx_id，失败返回None
        """
        host = get_config_values("StorageData", "host")
        api = get_config_values("StorageData", "api")
        port = get_config_values("StorageData", "port")
        url = "http://{}:{}{}".format(host, port, api)
        headers = {"Content-Type": "application/json"}
        response = requests.post(url=url, headers=headers, json=data)
        if response.json()["statusCode"] == "SUCCESS":
            logger.info(f"{data}入库成功")
            return response.json()["datas"][0]
        else:
            logger.info(f"数据入库失败：{data}")
            print(response.json()["errors"])
            return None


if __name__ == '__main__':
    import json

    print(json.dumps(DMP("DMPOfficial").get_bbd_qyxx_id("qyxx_xzcf", c_table="qyxx"), ensure_ascii=False))
    # print(DMP("DMPOfficial").getBbdDataUniqueId("qyxx_element"))
