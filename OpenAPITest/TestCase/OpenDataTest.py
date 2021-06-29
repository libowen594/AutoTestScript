#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import os
import time
import unittest
from kafka import KafkaProducer
from Common.ConnectRemoteServer import ConnectRemoteServer
from Common.DMP import DMP
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
pushCaseToExcel(sheet_name="OpenDataReport")
testData = Excel().get_sheet_values(sheet_name="OpenDataReport")
row = 1


def Producer(hosts, topic, data):
    try:
        producer = KafkaProducer(bootstrap_servers=hosts, max_request_size=8388608)
        producer.send(topic, data.encode('utf-8'))
        producer.close()
        return True
    except Exception:
        return False


@ddt.ddt
class OpenDataTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.row = 1
        base_dir = get_config_values("TestData", "path")
        cls.testData_dir = os.path.join(base_dir, str(datetime.datetime.today().date()), "OpenData")
        if not os.path.exists(cls.testData_dir):
            os.makedirs(cls.testData_dir)
        cls.DMPOfficial = DMP(configure="DMPOfficial")
        cls.DMPTest = DMP(configure="DMPTest")
        cls.DOPFront = DOP(configure="DOPFront")
        cls.RemoteServer = ConnectRemoteServer(server_name="hdfs-server")
        cls.hosts = get_config_values("OpenDataTest", "hosts")
        cls.topic = get_config_values("OpenDataTest", "topic")
        cls.hdfs_path = f'/user/bbdflume/dataplatform/pusher/test/datashare/{datetime.date.today().strftime("%Y%m%d")}/*'

    def tearDown(self) -> None:
        print(f"CaseName:{self.case_name}-{self.case_id}")
        print(f"handle名称：{self.handle}")
        print(f"订阅的表:{'、'.join(list(self.subscribe_table.keys()))}")
        print(f"推送数据的表：{self.table_name}")
        print(f"推送数据的bbd_type:{self.bbd_type}")
        with open(os.path.join(self.testData_dir, f"{self.case_name}-{self.case_id}.txt"), "a+",
                  encoding="UTF-8") as fp:
            print("发送数据如下：" + json.dumps(self.send_data, indent=4, ensure_ascii=False) + "\n")
            fp.write("发送数据如下：" + json.dumps(self.send_data, indent=4, ensure_ascii=False) + "\n")
        with open(os.path.join(self.testData_dir, f"{self.case_name}-{self.case_id}.txt"), "a+",
                  encoding="UTF-8") as fp:
            print(f"收到{self.rel_count}条数据如下:")
            fp.write(f"收到{self.rel_count}条数据如下:")
            for i in self.hdfs_data:
                print(json.dumps(i, indent=4, ensure_ascii=False) + "\n")
                fp.write(json.dumps(i, indent=4, ensure_ascii=False) + "\n")
        Excel().write_one_cell(value=self.result, col=9, row=self.row, sheet_name="OpenDataReport")
        Excel().write_one_cell(value=self.reason, col=10, row=self.row, sheet_name="OpenDataReport")
        print(f"测试结果：{self.result}")
        print(f"{self.reason}")
        print("---------------------------------------------------------------------------")

    def setUp(self) -> None:
        self.raw_count = self.RemoteServer.getHbaseData(path=self.hdfs_path,
                                                        timeout=30)

    @ddt.data(*testData)
    def test_OpenData(self, data):
        global row
        row += 1
        self.row = row
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
        base_num = int(data["收到数据数量基数"])
        self.table_name = list(self.push_table.keys())[0]
        self.DOPFront.creatPushTask(self.subscribe_table, self.handle)  # 修改订阅配置
        st = time.time()
        qyxx_tables = eval(get_config_values("qyxx_tables_info", "tables"))
        try:
            if self.table_name in qyxx_tables:  # 如果表是工商表，则使用下面的代码去拿入库数据入库拿到bbd_qyxx_id
                c_table = self.table_name.replace(f"_{self.table_name.split('_').pop(-1)}", "")
                self.bbd_qyxx_id = self.DMPOfficial.get_bbd_qyxx_id(self.table_name, c_table)
                if self.bbd_qyxx_id:
                    self.pushData = self.RemoteServer.getKafkaData(self.bbd_qyxx_id, c_table)  # 去第一个Kafka拿需要推送的数据
                    self.bbd_type = ""
                else:
                    raise AssertionError("没有找到入库数据")  # 如果没有找到入库数据会直接失败
            else:  # 如果不是工商信息表，则使用下面的代码去找入库数据入库，拿到bbd_xgxx_id
                CrawlerTables = self.DMPTest.getCrawlerTableName(self.table_name)  # 获取能入库的爬虫表和对应的bbd_type,的列表
                if isinstance(CrawlerTables, list):  # 便利列表,查找入库数据
                    for i in CrawlerTables:
                        crawler_table_name = i[0]
                        self.bbd_type = i[1]
                        if self.push_table[self.table_name]:  # 判断用例中是否有bbd_type的限制
                            if self.bbd_type != self.push_table[self.table_name]:
                                continue
                        info = self.DMPOfficial.getBbdDataUniqueId(crawler_table_name, self.bbd_type)
                        if info:  # 拿到UniqueId和数据所在爬虫库，找数据入库
                            storageData = self.DMPOfficial.getRowDate(info[0], info[1])
                            self.bbd_xgxx_id = self.DMPOfficial.dataStorage(storageData)["bbd_xgxx_id"]
                            self.pushData = self.RemoteServer.getKafkaData(self.bbd_xgxx_id, self.table_name)
                        else:  # 如果没有UniqueId和数据所在爬虫库，则使用下面的代码造入库数据入库
                            self.pushData = ExecuteSql.creatData(self.table_name)
                            self.bbd_xgxx_id = self.pushData.get("bbd_xgxx_id", None)
                            self.bbd_type = self.pushData["ndata"][self.table_name][0].get("bbd_table", None)
                        if self.push_table[self.table_name] is None:  # 如果为空就停止循环，如果不为空则进行下一次循环
                            break
                else:  # 如果没有找到入库信息，也会自动造推送数据
                    self.pushData = ExecuteSql.creatData(self.table_name)
                    self.bbd_xgxx_id = self.pushData.get("bbd_xgxx_id", None)
                    self.bbd_type = self.pushData["ndata"][self.table_name][0].get("bbd_table", None)
            pusher_id = get_config_values("PusherInfo", "test_pusher_id")
            if self.table_name not in qyxx_tables:  # 如果不是工商信息表，则用下面的代码回去应该收到数据的条数
                if self.pushData.get("odata", None):
                    count = len(self.pushData["odata"][self.table_name][0]["bbd_dp_enterprise_rel"])
                elif self.pushData.get("ndata", None):
                    count = len(self.pushData["ndata"][self.table_name][0]["bbd_dp_enterprise_rel"])
                elif self.pushData.get("cdata", None):
                    count = len(self.pushData["cdata"][self.table_name][0]["bbd_dp_enterprise_rel"])
                else:
                    count = 0
                if count == 0:
                    self.count = 1
                else:
                    self.count = count
            else:  # 如果是工商信息表，则需要去数据库查询应该收到的数据的条数
                if self.pushData.get("odata", None):
                    self.count = len(self.pushData["odata"][self.table_name])
                elif self.pushData.get("ndata", None):
                    self.count = len(self.pushData["ndata"][self.table_name])
                elif self.pushData.get("cdata", None):
                    self.count = len(self.pushData["cdata"][self.table_name])
                else:
                    self.count = 0
            if modifyData:
                with open('data.json', "w+", encoding='utf-8') as fd:
                    fd.write(json.dumps(self.pushData, ensure_ascii=False, indent=4))
                input("请修改data.json文件后按任意键继续测试")
                with open('data.json', "r", encoding='utf-8') as fd:
                    self.send_data = json.loads(fd.read())
            else:
                self.send_data = self.pushData
            while time.time() - st < 61:  # 程序执行到这里，没有超过30秒，就会进行等待.因为openData-push程序拿订阅配置信息需要60秒
                time.sleep(3)
            Producer(self.hosts, self.topic, json.dumps(self.send_data, ensure_ascii=False))  # 推送数据
            self.hdfs_data = self.RemoteServer.getHbaseData(path=self.hdfs_path,
                                                            count=self.raw_count,
                                                            timeout=30)  # 循环获取hdfs储存数据
            self.rel_count = len(self.hdfs_data)
            if self.rel_count != 0:
                if self.handle != "UnionHandler":
                    self.assertEqual(self.count * base_num, self.rel_count,
                                     msg=f"实际应该收到{self.count * base_num}条数据，但是接收到了{self.rel_count}条相同数据")
                    for hdfs in self.hdfs_data:
                        var = list(hdfs.get("data").keys())[0]
                        self.assertIn(var, list(self.subscribe_table.keys()), msg=f"收到的数据不应该包含表{var}")
                        for i in list(self.subscribe_table.keys()):
                            if i == var:
                                try:
                                    columns = self.DOPFront.get_columns(pusher_id)[i]
                                except Exception:
                                    columns = []
                                table_data = hdfs.get("data").get(i, [{}])
                                for t in table_data:
                                    hdfs_columns = list(t.keys())
                                    for column in columns:
                                        self.assertIn(column, hdfs_columns, msg='%s收到的数据没有包含字段名%s' % (i, column))
                else:
                    r_count = 0
                    for i in list(self.subscribe_table.keys()):
                        t_data = self.hdfs_data[0].get("data").get(i, None)
                        if t_data:
                            r_count += 1
                            self.assertEqual(len(t_data), self.count,
                                             msg=f"{i}应该收到了{self.count}数据，实际收到了{len(t_data)}条数据")
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
        except AssertionError as e:
            self.reason = e.args[-1]
            self.result = "Fail"
            raise e
        else:
            self.reason = ""
            self.result = "PASS"


if __name__ == '__main__':
    unittest.main()
