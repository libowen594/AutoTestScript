#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import random
import string
import datetime
import pymysql
import time
from Common.logger import get_logger

logger = get_logger(__name__)


class ExecuteSql:
    @staticmethod
    def execute_sql(sql):
        conn = pymysql.connect(host='10.28.100.51', port=3306, user='root', passwd='Dataom123!@#')
        cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            logger.info(f"执行sql：{sql}  ...")
            start = time.time()
            cursor.execute(sql)
            end = time.time()
            logger.info(f"执行成功，用时{str(end - start).split('.')[0]}秒")
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result
        except Exception as e:
            logger.info(f"执行sql：{sql}失败")
            logger.error(str(e))
            cursor.close()
            conn.close()

    @staticmethod
    def get_databases(bbd_table):
        """
        查询业务表所在的数据库
        :param bbd_table: 被查询业务表的名字
        :return: 所在数据库的列表
        """
        sql = f'SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE table_name="{bbd_table}" and (TABLE_SCHEMA="bbd_higgs" OR TABLE_SCHEMA="bbd_dp_datacube" OR TABLE_SCHEMA="bbd_dp_business") AND TABLE_ROWS != 0'
        result = ExecuteSql.execute_sql(sql)
        return [i["TABLE_SCHEMA"] for i in result]

    @staticmethod
    def get_columns(bbd_table, database):
        """
        :param bbd_table: 被查询的业务表名称
        :param database: 被查询业务表所在的库名称
        :return: column list
        """
        sql = f'SELECT TABLE_SCHEMA,table_name,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name="{bbd_table}" AND TABLE_SCHEMA="{database}"'
        result = ExecuteSql.execute_sql(sql)
        return [i["COLUMN_NAME"] for i in result]

    @staticmethod
    def get_relational_data(bbd_table, database):
        """
        获取关系型数据的qyxx_id和company_name
        :param bbd_table: 业务表名称
        :param database: 业务表所在数据库
        :return: 查询出来的数据
        """
        if database == "bbd_higgs":
            relational_table = f"xgxx_relation"  # relational_table关系表的名称
        else:
            relational_table = f"{bbd_table.split('_')[0]}_enterprise_rel"
        sql = f'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name="{relational_table}" and TABLE_SCHEMA="{database}"'
        table_info = ExecuteSql.execute_sql(sql)
        if len(table_info) != 0:
            table_rows = table_info[0]["TABLE_ROWS"]  # 获取表的最大数据条数
            limit = 1000
            if table_rows <= limit:  # 判断limit是否大于最大数据条数
                limit = table_rows
            if table_rows <= limit:
                limit = table_rows
            sql = f'SELECT * FROM {database}.{bbd_table} LEFT JOIN {database}.{relational_table} on {bbd_table}.bbd_xgxx_id={relational_table}.bbd_xgxx_id WHERE {relational_table}.bbd_qyxx_id is not NULL LIMIT {limit}'
            result = ExecuteSql.execute_sql(sql)
            if len(result) != 0:
                data = random.choice(result)
                for key in list(data.keys()):
                    if relational_table in key: data.pop(key)  # 去掉关系表中的字段
                return data
            else:
                logger.info("没有找到符合条件的数据")
                return "没有找到符合条件的数据"
        else:
            logger.info("被测试的数据表不支持离线推送")
            return "被测试的数据表不支持离线推送"

    @staticmethod
    def get_not_relational_data(bbd_table, database):
        """
        获取非关系型数据的qyxx_id和company_name
        :param bbd_table: 业务表名称
        :param database: 业务表所在数据库
        :return: 查询出来的数据
        """
        sql = f'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name="{bbd_table}" and TABLE_SCHEMA="{database}"'
        table_info = ExecuteSql.execute_sql(sql)
        table_rows = table_info[0]["TABLE_ROWS"]
        limit = 1000
        if table_rows <= limit:
            limit = table_rows
        sql = f'SELECT * FROM {database}.{bbd_table} limit {limit}'
        result = ExecuteSql.execute_sql(sql)
        if len(result) != 0:
            return random.choice(result)

    @staticmethod
    def get_count(bbd_table, bbd_qyxx_id, condition=None):
        """
        通过table_name和bbd_qyxx_id获取有多少条数据
        :param bbd_table:
        :param bbd_qyxx_id:
        :param condition:
        :return:
        """
        databases = ExecuteSql.get_databases(bbd_table=bbd_table)
        if "bbd_dp_business" in databases:
            databases = "bbd_dp_business"
        elif "bbd_dp_datacube" in databases:
            databases = "bbd_dp_datacube"
        elif "bbd_higgs" in databases:
            databases = "bbd_higgs"
        else:
            return None
        columns = ExecuteSql.get_columns(bbd_table, databases)
        if 'bbd_qyxx_id' not in columns:
            count = ExecuteSql.get_relational_count(bbd_table, databases, bbd_qyxx_id, condition)
        else:
            count = ExecuteSql.get_not_relational_count(bbd_table, databases, bbd_qyxx_id, condition)
        return count

    @staticmethod
    def get_relational_count(bbd_table, database, bbd_qyxx_id, condition=None):
        if condition != "" and isinstance(condition, list):
            query = 'and ' + ' and '.join([f"{bbd_table}." + i for i in condition])
        else:
            query = ""
        if database == "bbd_higgs":
            relational_table = "xgxx_relation"
        else:
            relational_table = f"{bbd_table.split('_')[0]}_enterprise_rel"
        sql = f'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_name="{relational_table}" and TABLE_SCHEMA="{database}"'
        table_info = ExecuteSql.execute_sql(sql)
        if len(table_info) != 0:
            sql = f'SELECT COUNT(1) FROM {database}.{bbd_table} LEFT JOIN {database}.{relational_table} ON {bbd_table}.bbd_xgxx_id={relational_table}.bbd_xgxx_id WHERE {relational_table}.bbd_qyxx_id="{bbd_qyxx_id}" and {relational_table}.bbd_table="{bbd_table}" {query}'
            result = ExecuteSql.execute_sql(sql)
            return result[0]["COUNT(1)"]
        else:
            logger.info(f"在{database}不存在关系表{relational_table}")
            return f"在{database}不存在关系表{relational_table}"

    @staticmethod
    def get_not_relational_count(bbd_table, database, bbd_qyxx_id, condition=None):
        if condition and isinstance(condition, list):
            query = 'and ' + ' and '.join(condition)
        else:
            query = ""
        sql = f'SELECT COUNT(1) FROM {database}.{bbd_table} WHERE bbd_qyxx_id="{bbd_qyxx_id}" {query}'
        result = ExecuteSql.execute_sql(sql)
        return result[0]["COUNT(1)"]

    @staticmethod
    def get_data(table_name, databases):
        """
        通过table_name和所在数据库获取一条数据库中的数
        :param table_name:
        :param databases:
        :return:
        """
        logger.info(f"开始查找数据，数据库={databases}，table={table_name}")
        columns = ExecuteSql.get_columns(table_name, databases)
        if 'bbd_qyxx_id' not in columns:
            logger.info("this is a Relational table")
            data = ExecuteSql.get_relational_data(table_name, databases)
        else:
            data = ExecuteSql.get_not_relational_data(table_name, databases)
        if isinstance(data, dict):
            logger.info(f"data={data}")
            ctime = data.pop("ctime", "2021-03-24 00:00:00") or data.pop("create_time", "2021-03-24 00:00:00")
            bbd_type = data.pop("bbd_type", "") or data.pop("type", "")
            company_name = data.pop("entity_name", "") or data.pop("company_name", "")
            return {"data": {"bbd_qyxx_id": data["bbd_qyxx_id"], "company_name": company_name, "ctime": str(ctime)},
                    'bbd_type': bbd_type}
        else:
            return {}

    @staticmethod
    def creatData(bbd_table):
        """
        通过table_name造一条数据
        :param bbd_table:
        :return:
        """
        business_database = 'bbd_dp_business'  # 最新业务库
        datacube_database = 'bbd_dp_datacube'  # 新库
        higgs_database = 'bbd_higgs'  # 老库
        databases = ExecuteSql.get_databases(bbd_table=bbd_table)
        if business_database in databases:
            data = ExecuteSql.get_not_relational_data(bbd_table, business_database)
            database = business_database
        elif datacube_database in databases:
            data = ExecuteSql.get_not_relational_data(bbd_table, datacube_database)
            database = datacube_database
        elif higgs_database in databases:
            data = ExecuteSql.get_not_relational_data(bbd_table, higgs_database)
            database = higgs_database
        else:
            logger.info(f"没有在业务库找到{bbd_table}")
            print(f"没有在业务库找到{bbd_table}")
            return {}
        data.pop("id", None)
        data.pop("bbd_source", None)
        for key in list(data.keys()):
            if isinstance(data[key], datetime.date) or isinstance(data[key], datetime.datetime):
                data[key] = str(data.pop(key))
        # data["ctime"] = data.pop("ctime", "2021-03-24 00:00:00") or data.pop("create_time", "2021-03-24 00:00:00")
        # data["bbd_type"] = data.pop("bbd_type", ) or data.pop("type", "")
        bbd_xgxx_id = data.get("bbd_xgxx_id", None)
        company_name = data.pop("company_name", None) or data.pop("entity_name", None)
        bbd_qyxx_id = data.pop("bbd_qyxx_id", None)
        if bbd_xgxx_id:
            bbd_dp_enterprise_rel = ExecuteSql.get_bbd_dp_enterprise_rel(bbd_table, bbd_xgxx_id, database)
        else:
            bbd_dp_enterprise_rel = [
                {"entity_type": None, "entity_name": company_name, "entity_id": bbd_qyxx_id, "source_key": []}]
        rowData = {
            "bbd_table": bbd_table,
            "odata": None,
            "bbd_qyxx_company": None,
            "bbd_qyxx_id": None,
            "ndata": {
                bbd_table: [
                    {
                        "bbd_seed": "",
                        "bbd_data_unique_id": ExecuteSql.creat_string_chinese(32),  # 随机生成bbd_data_unique_id
                        "bbd_version": "1",
                        "bbd_dp_enterprise_rel": bbd_dp_enterprise_rel,
                        "bbd_params": "flume",
                        "bbd_inner_flag_dict": {},
                        "bbd_md5": ExecuteSql.creat_string_chinese(32),  # 随机生成MD5
                        "bbd_dp_org_rel": [],
                        "bbd_dp_person_rel": [],
                        "_id": f"{ExecuteSql.creat_string_chinese(8)}-{ExecuteSql.creat_string_chinese(4)}-{ExecuteSql.creat_string_chinese(4)}-{ExecuteSql.creat_string_chinese(4)}-{ExecuteSql.creat_string_chinese(8)}",
                    }
                ]
            },
            "bbd_xgxx_id": bbd_xgxx_id,
            "ttype": "xgxx",
            "company_name": None,
            "property": None,
            "type": "insert",
            "bbd_qyxx_company_id": None,
            "cdata": None
        }
        rowData["ndata"][bbd_table][0].update(data)
        return rowData

    @staticmethod
    def get_bbd_dp_enterprise_rel(bbd_table, xgxx_id, database):
        """
        或当前相关联的企业
        :param bbd_table:
        :param xgxx_id:
        :param database:
        :return:
        """
        enterprise_rel = []
        if database == "bbd_higgs":
            relational_table = "xgxx_relation"
        else:
            relational_table = f"{bbd_table.split('_')[0]}_enterprise_rel"
        sql = f'SELECT * FROM {database}.{relational_table} WHERE bbd_xgxx_id = "{xgxx_id}" AND bbd_table = "{bbd_table}"'
        data = ExecuteSql.execute_sql(sql)
        if data:
            for i in data:
                entity_type = i.get("entity_type", 0)
                entity_name = i.get("entity_name") or i.get("company_name")
                entity_id = i.get("bbd_qyxx_id")
                source_key = i.get("source_key", "name")
                enterprise_rel.append({"entity_type": entity_type, "entity_name": entity_name, "entity_id": entity_id,
                                       "source_key": [source_key]})
        return enterprise_rel

    @staticmethod
    def creat_string_chinese(bits):
        """随机生成由小写字母和数字组成的字符串"""
        src_lowercase = string.ascii_lowercase  # string_小写字母 'abcdefghijklmnopqrstuvwxyz'
        src_digits = string.digits  # string_数字  '0123456789'
        chinese_list = []
        for i in range(bits):
            s = random.choice(src_lowercase + src_digits)
            chinese_list.append(s)
        return ''.join(chinese_list)


if __name__ == '__main__':
    print(ExecuteSql.creatData("qyxx_basic"))

