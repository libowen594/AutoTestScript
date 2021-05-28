#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

import csv
import os


def writer(path, filename, values):
    if not filename.endswith(".csv"):  # 判断是否为csv文件
        raise Exception(f"{filename}不是一个csv文件")
    else:
        if not os.path.exists(path):
            os.makedirs(path)
    if not isinstance(values, list):  # 判断传入的values是否为list
        raise TypeError("传入的参数不是list类型，类型错误")
    with open(file=os.path.join(path, filename), mode="w", encoding="UTF-8") as fp:
        csv_writer = csv.writer(fp)
        for value in values:
            if isinstance(value, (list, tuple)):  # 判断传入的参数是否为list或者tuple
                csv_writer.writerow(value)
                fp.close()
            else:
                fp.close()
                raise TypeError("传入的参数内包含的不是list或tuple，类型错误")
