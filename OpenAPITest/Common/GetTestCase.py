#!/usr/bin/env python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

from Common.ConnectRemoteServer import *
from Common.Excel import *
from Common.DOP import DOP
from Common.ExecuteSql import ExecuteSql
from Common.logger import get_logger
from Common.ReadConfig import *

logger = get_logger(__name__)


def pushCaseToExcel(sheet_name):
    """
    :param sheet_name:
    :return:
    """
    pusher_id = get_config_values("PusherInfo", "Official_pusher_id")
    if pusher_id:
        __creat_case(sheet=sheet_name, pusher_id=pusher_id)
    else:
        Excel().creat_sheet(sheet_name="OpenDataReport")
        with open("../TestCase/Case.txt", "r", encoding="utf-8") as f:
            lines = f.readlines()
            row = 1
            for line in lines:
                res = line.split("\t")
                Excel().write_rows(start_col=1, end_col=10, row=row, values=res, sheet_name="OpenDataReport")
                row += 1


def __creat_case(sheet, pusher_id):
    Excel().creat_sheet(sheet_name="OpenDataReport", header=["Caseid", "CaseName", "handle", "订阅表名称和字段", "推送表的名称",
                                                             "是否需要修改数据", "数据库查询条件", "收到数据数量基数", "测试结果", "备注"])
    data, handle = DOP.getConfigureInfo(pusher_id)
    title = data.get("title")
    table = json.loads(data.get("table_column"))
    row = 2
    for i in table:
        values = [row - 1, title, handle, table, {i: None}, "", "", 1, "", ""]
        Excel().write_rows(start_col=1, end_col=10, row=row, values=values, sheet_name=sheet)
        row += 1
    modify_config("PusherInfo", "title", title)
