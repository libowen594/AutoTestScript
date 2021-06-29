#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import unittest
from Common.GetTestCase import *
from Common.ConnectRemoteServer import ConnectRemoteServer
from Common.Excel import Excel
from Common.DOP import DOP
from Common.CSV import writer
from Common.ReadConfig import get_config_values
from Common.logger import get_logger
from Common.ExecuteSql import ExecuteSql
import datetime
import ddt
import json
import os

logger = get_logger(__name__)
pushCaseToExcel(sheet_name="OfflineAutoPush")
testData = Excel().get_sheet_values(sheet_name="OfflineAutoPush")
row = 1


@ddt.ddt
class OfflineAutoPushTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.row = 1
        base_dir = get_config_values("TestData", "path")
        cls.testData_dir = os.path.join(base_dir, str(datetime.datetime.today().date()), "OfflineAutoPush")
        if not os.path.exists(cls.testData_dir):
            os.makedirs(cls.testData_dir)
        cls.DOPFront = DOP("DOPFront")
        cls.hdfs_server = ConnectRemoteServer(server_name="SparkOffline-server")
        cls.hdfs_path = f'/user/bbdoffline/dp-hulin/output/*'
        cls.app_server = ConnectRemoteServer(server_name="AutoPushApp-server")

    def tearDown(self) -> None:
        print(f"CaseName:{self.case_name}-{self.case_id}")
        print(f"handle名称：{self.handle}")
        print(f"订阅的表:{'、'.join(list(self.subscribe_table.keys()))}")
        print(f"推送数据的表：{self.table_name}")
        print(f"推送数据：{self.send_data}")
        with open(os.path.join(self.test_dir, f"{self.case_name}-{self.case_id}.txt"), "a+", encoding="UTF-8") as fp:
            print("发送数据如下：" + json.dumps(self.send_data, indent=4, ensure_ascii=False) + "\n")
            fp.write("发送数据如下：" + json.dumps(self.send_data, indent=4, ensure_ascii=False) + "\n")
        with open(os.path.join(self.test_dir, f"{self.case_name}-{self.case_id}.txt"), "a+", encoding="UTF-8") as fp:
            print(f"收到{self.rel_count}条数据如下:")
            fp.write(f"收到{self.rel_count}条数据如下:")
            for data in self.hdfs_data:
                print(json.dumps(data, indent=4, ensure_ascii=False) + "\n")
                fp.write(json.dumps(data, indent=4, ensure_ascii=False) + "\n")
        Excel().write_one_cell(value=self.result, col=9, row=self.row, sheet_name="OfflineAutoPush")
        Excel().write_one_cell(value=self.reason, col=10, row=self.row, sheet_name="OfflineAutoPush")
        print(f"测试结果：{self.result}")
        print(f"{self.reason}")
        print("---------------------------------------------------------------------------")

    @ddt.data(*testData)
    def test_OfflineAutoPush(self, data):
        global row
        row += 1
        self.row = row
        self.pushData = {}
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
        self.test_dir = os.path.join(self.testData_dir, f"{self.case_name}-{self.case_id}")
        if not os.path.exists(self.test_dir):
            os.makedirs(self.test_dir)
        self.table_name = list(self.push_table.keys())[0]
        self.DOPFront.creatPushTask(self.subscribe_table, self.handle)
        pusher_id = get_config_values("PusherInfo", "test_pusher_id")
        business_database = 'bbd_dp_business'  # 最新业务库
        datacube_database = 'bbd_dp_datacube'  # 新库
        higgs_database = 'bbd_higgs'  # 老库
        databases = ExecuteSql.get_databases(bbd_table=self.table_name)
        try:
            if business_database in databases:
                self.raw_data = ExecuteSql.get_data(self.table_name, business_database)
            elif datacube_database in databases:
                self.raw_data = ExecuteSql.get_data(self.table_name, datacube_database)
            elif higgs_database in databases:
                self.raw_data = ExecuteSql.get_data(self.table_name, higgs_database)
            else:
                raise AssertionError(f"没有在业务库找到{self.table_name}")
            self.send_data = self.raw_data.get("data", {})
            if modifyData:
                with open('data.json', "w+", encoding='utf-8') as fd:
                    fd.write(json.dumps(self.send_data, ensure_ascii=False, indent=4))
                input("请修改data.json文件后按任意键继续测试")
                with open('data.json', "r", encoding='utf-8') as fd:
                    self.send_data = json.loads(fd.read())
            if self.send_data:
                self.bbd_type = self.raw_data.get("bbd_type", "")
                self.qyxx_id = self.send_data.get("bbd_qyxx_id")
                offline_path = get_config_values("AutoPushApp-server", "offline_path")
                self.app_server.upload(src_file=r"../application-offline.properties",
                                       dsc_path=rf"{offline_path}/config/")
                csv_data = [(self.send_data.get("bbd_qyxx_id", ""), self.send_data.get("company_name", ""))]
                writer(path=self.test_dir, filename="test_data.csv", values=csv_data)
                self.app_server.upload(src_file=os.path.join(self.test_dir, "test_data.csv"),
                                       dsc_path=rf"{offline_path}/")
                self.hdfs_server.execute_cmd(r"hadoop fs -rm -r -f /user/bbdoffline/dp-hulin/output/")
                app_path = get_config_values("AutoPushApp-server", "offline_path")
                self.app_server.execute_cmd(fr"rm -r {app_path}/logs/*")
                flag = self.app_server.start_app_offline(query_cmd=r"ps -ef | grep autopush-core",
                                                         cmd=fr"sh {app_path}/start-offline.sh")
                if flag: # 判断推送是否完成
                    log = self.app_server.execute_cmd(
                        rf"cat {app_path}/logs/autopush-core.{datetime.datetime.today().date()}.log")
                    line = [line for line in log if "ERROR" in line]
                    if len(line) != 0: # 判断推送过程中是否有报错
                        with open(os.path.join(self.test_dir, "OfflineAutoPush.log"), "w", encoding="utf-8") as f:
                            f.write(log)
                        raise AssertionError("推送出现错误，请检查")
                    else:
                        self.hdfs_data = self.hdfs_server.getHbaseData(path=self.hdfs_path, count=0, timeout=30)
                        self.rel_count = len(self.hdfs_data)
                        if self.rel_count != 0:
                            if self.handle != "UnionHandler":
                                self.count = 0
                                for i in list(self.subscribe_table.keys()):
                                    count = ExecuteSql.get_count(bbd_table=i, bbd_qyxx_id=self.qyxx_id, condition=condition)
                                    self.count += count
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
                                                    self.assertIn(column, hdfs_columns,
                                                                  msg='%s收到的数据没有包含字段名%s' % (i, column))
                            else:
                                r_count = 0
                                for i in list(self.subscribe_table.keys()):
                                    t_data = self.hdfs_data[0].get("data").get(i, None)
                                    count = ExecuteSql.get_count(bbd_table=i, bbd_qyxx_id=self.qyxx_id, condition=condition)
                                    if t_data:
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
        except AssertionError as e:
            self.reason = e.args[-1]
            self.result = "Fail"
            raise e
        else:
            self.reason = ""
            self.result = "PASS"


if __name__ == '__main__':
    unittest.main()
