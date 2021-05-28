#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import os
import time
import unittest
from kafka import KafkaProducer
from Common.ConnectRemoteServer import ConnectRemoteServer
from Common.Excel import Excel
from Common.DOP import DOP
from Common.ReadConfig import get_config_values
from Common.logger import get_logger
from Common.ExecuteSql import ExecuteSql
from Common.GetTestCase import pushCaseToExcel
import datetime
import ddt
import json

logger = get_logger(__name__)
pushCaseToExcel(sheet_name="AutoPush")
testData = Excel().get_sheet_values(sheet_name="AutoPush")


def Producer(hosts, topic, data):
    try:
        producer = KafkaProducer(bootstrap_servers=hosts, max_request_size=8388608)
        producer.send(topic, data.encode('utf-8'))
        producer.close()
        return True
    except Exception:
        return False


@ddt.ddt
class AutoPushTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.row = 1
        base_dir = get_config_values("TestData", "path")
        cls.testData_dir = os.path.join(base_dir, str(datetime.datetime.today().date()), "AutoPush")
        if not os.path.exists(cls.testData_dir):
            os.makedirs(cls.testData_dir)
        cls.DOPFront = DOP("DOPFront")
        cls.hdfs_server = ConnectRemoteServer(server_name="SparkOffline-server")
        cls.hosts = get_config_values("AutoPushTest", "hosts")
        cls.topic = get_config_values("AutoPushTest", "topic")
        cls.hdfs_path = f'/user/bbdflume/dataplatform/pusher/test/datashare/{datetime.date.today().strftime("%Y%m%d")}/*'
        cls.app_server = ConnectRemoteServer(server_name="AutoPushApp-server")
        app_path = get_config_values("AutoPushApp-server", "rel_path")
        cls.pid = cls.app_server.start_app(
            query_cmd=r"ps -ef | grep autopush-core",
            cmd=fr'sh {app_path}/start-core.sh')

    @classmethod
    def tearDownClass(cls):
        cls.app_server.kill_app(cls.pid)

    def tearDown(self):
        print(f"CaseName:{self.case_name}-{self.case_id}")
        print(f"handle名称：{self.handle}")
        print(f"订阅的表:{'、'.join(list(self.subscribe_table.keys()))}")
        print(f"推送数据的表：{self.table_name}")
        print(f"推送数据：{self.send_data}")
        with open(os.path.join(self.testData_dir, f"{self.table_name}.txt"), "a+", encoding="UTF-8") as fp:
            print("发送数据如下：" + json.dumps(self.send_data, indent=4, ensure_ascii=False) + "\n")
            fp.write("发送数据如下：" + json.dumps(self.send_data, indent=4, ensure_ascii=False) + "\n")
        with open(os.path.join(self.testData_dir, f"{self.table_name}.txt"), "a+", encoding="UTF-8") as fp:
            print(f"收到{self.rel_count}条数据如下:")
            fp.write(f"收到{self.rel_count}条数据如下:")
            for data in self.hdfs_data:
                print(json.dumps(data, indent=4, ensure_ascii=False) + "\n")
                fp.write(json.dumps(data, indent=4, ensure_ascii=False) + "\n")
        Excel().write_one_cell(value=self.result, col=9, row=self.row, sheet_name="AutoPush")
        Excel().write_one_cell(value=self.reason, col=10, row=self.row, sheet_name="AutoPush")
        print(f"测试结果：{self.result}")
        print(f"{self.reason}")
        print("---------------------------------------------------------------------------")

    def setUp(self):
        self.raw_count = self.hdfs_server.getHbaseData(path=self.hdfs_path, timeout=60)

    @ddt.data(*testData)
    def test_AutoPush(self, data):
        self.row += 1
        self.send_data = {}
        self.case_id = data["Caseid"]
        self.case_name = data["CaseName"]
        self.handle = data["handle"]
        self.subscribe_table = eval(data["订阅表名称和字段"])
        self.push_table = eval(data["推送表的名称"])
        try:
            modifyData = eval(data["是否需要修改数据"])
        except Exception:
            modifyData = None
        try:
            condition = eval(data["数据库查询条件"])
        except Exception:
            condition = None
        base_num = int(data["收到数据数量基数"])
        self.table_name = list(self.push_table.keys())[0]
        self.DOPFront.creatPushTask(self.subscribe_table, self.handle)  # 修改订阅配置信息
        st = time.time()
        pusher_id = get_config_values("PusherInfo", "test_pusher_id")
        business_database = 'bbd_dp_business'  # 最新业务库
        datacube_database = 'bbd_dp_datacube'  # 新库
        higgs_database = 'bbd_higgs'  # 老库
        try:
            databases = ExecuteSql.get_databases(bbd_table=self.table_name)
            if business_database in databases:
                self.raw_data = ExecuteSql.get_data(self.table_name, business_database)
            elif datacube_database in databases:
                self.raw_data = ExecuteSql.get_data(self.table_name, datacube_database)
            elif higgs_database in databases:
                self.raw_data = ExecuteSql.get_data(self.table_name, higgs_database)
            else:
                raise AssertionError(f"没有在业务库找到{self.table_name}")
            self.send_data = self.raw_data.get("data", {})
            if modifyData:  # 是否修改数据，如果需要修改，会把推送数据加载到data.json文件，修改后按回车键继续测试
                with open('data.json', "w+", encoding='utf-8') as fd:
                    fd.write(json.dumps(self.send_data, ensure_ascii=False, indent=4))
                input("请修改data.json文件后按回车键继续测试")
                with open('data.json', "r", encoding='utf-8') as fd:
                    self.send_data = json.loads(fd.read())
            if self.send_data:
                self.send_data.update({"id": pusher_id})
                self.bbd_type = self.raw_data.get("bbd_type", "")
                while time.time() - st <= 31:  # 程序执行到这里，没有超过30秒，就会进行等待.因为auto-push程序拿订阅配置信息需要30秒
                    time.sleep(3)
                Producer(self.hosts, self.topic, json.dumps(self.send_data, ensure_ascii=False))  # 推送数据
                self.qyxx_id = self.send_data.get("bbd_qyxx_id")
                self.hdfs_data = self.hdfs_server.getHbaseData(path=self.hdfs_path, count=self.raw_count,
                                                               timeout=60)  # 通过测试前的的数据条数,获取HDFS收到的数据
                self.rel_count = len(self.hdfs_data)  # 收到数据的数量
                if self.rel_count != 0:
                    if self.handle != "UnionHandler":  # 如果handle=UnionHandler 则使用下面的断言方式
                        self.count = 0
                        for i in list(self.subscribe_table.keys()):  # 便利订阅的表，循环取hdfs_data中取这个表的数据.
                            count = ExecuteSql.get_count(bbd_table=i, bbd_qyxx_id=self.qyxx_id,
                                                         condition=condition)  # 通过qyxx_id查找表中有多少条数据
                            self.count += count
                        self.assertEqual(self.count * base_num, self.rel_count,
                                         msg=f"实际应该收到{self.count * base_num}条数据，但是接收到了{self.rel_count}条相同数据")
                        for hdfs in self.hdfs_data:  # 便利HDFS收到的数据，断言
                            var = list(hdfs.get("data").keys())[0]
                            self.assertIn(var, list(self.subscribe_table.keys()),
                                          msg=f"收到的数据不应该包含表{var}")  # 断言收到数据的表在订阅的表中
                            for i in list(self.subscribe_table.keys()):  # 循环便利订阅的表,查找订阅的表的字段
                                if i == var:
                                    try:
                                        columns = self.DOPFront.get_columns(pusher_id)[
                                            i]  # 获取订阅表的字段，如果报错,说明这张表没有在订阅中.字段为空
                                    except Exception:
                                        columns = []
                                    table_data = hdfs.get("data").get(i, [{}])  # 获取收到数据中这张表的内容,进行字段断言
                                    for t in table_data:
                                        hdfs_columns = list(t.keys())
                                        for column in columns:
                                            self.assertIn(column, hdfs_columns,
                                                          msg='%s收到的数据没有包含字段名%s' % (i, column))
                    else:  # handle != UnionHandler时用下面的断言方式
                        r_count = 0
                        for i in list(self.subscribe_table.keys()):  # 便利订阅的表，循环取hdfs_data中取这个表的数据.
                            t_data = self.hdfs_data[0].get("data").get(i, None)
                            count = ExecuteSql.get_count(bbd_table=i, bbd_qyxx_id=self.qyxx_id, condition=condition)
                            if t_data:  # 收到的数据中存在这个表的数据才进行下面的断言，否则不进行下面的断言
                                r_count += 1
                                self.assertEqual(len(t_data), count, msg=f"{i}应该收到了{count}数据，实际收到了{len(t_data)}条数据")
                                try:
                                    columns = self.DOPFront.get_columns(pusher_id)[i]
                                except Exception:
                                    columns = []
                                for t in t_data:
                                    hdfs_columns = list(t.keys())
                                    for column in columns:
                                        self.assertIn(column, hdfs_columns, msg='%s收到的数据没有包含字段名%s' % (i, column))
                        self.assertEqual(r_count, base_num, msg=f"应该收到{base_num}条数据，实际收到了{r_count}条数据")
                else:
                    self.assertEqual(self.rel_count, base_num, msg=f"应该收到{base_num}条数据，实际收到了{self.rel_count}条数据")
            else:
                raise AssertionError(f"没有在业表{self.table_name}找到测试数据")  # send_data 为空会直接失败
        except AssertionError as e:  # 发生AssertionError，断言失败
            self.reason = e.args[-1]
            self.result = "Fail"
            raise e
        else:  # 没有发生AssertionError，断言通过
            self.reason = ""
            self.result = "PASS"


if __name__ == '__main__':
    unittest.main()
