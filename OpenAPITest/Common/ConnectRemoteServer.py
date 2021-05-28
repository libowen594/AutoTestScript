#!/usr/bin/python
# -*- coding: UTF-8 -*-
__author__ = 'Bowen.li'

from time import sleep
import paramiko
from Common.logger import get_logger
from Common.ReadConfig import get_config_values
import json
import time
import os

logger = get_logger(__name__)


class ConnectRemoteServer:
    def __init__(self, server_name):
        self.host = get_config_values(server_name, "host")
        self.port = get_config_values(server_name, "port")
        self.user = get_config_values(server_name, "user")
        self.password = get_config_values(server_name, "password")
        self._transport = None

    def connect(self):
        try:
            transport = paramiko.Transport((self.host, int(self.port)))
            transport.connect(username=self.user, password=self.password)
            self._transport = transport
            logger.info(f"{self.host}连接成功")
        except Exception as e:
            logger.error(f"{self.host}连接失败", str(e))

    def close(self):
        self._transport.close()
        logger.info(f"{self.host}连接已断开")

    def getKafkaData(self, bbd_id, bbd_table):
        """
        获取第一个kafka的数据
        :param bbd_id: qyxx_id或xgxx_id
        :param bbd_table: 需要获数据的table_name
        :return:
        """

        logger.info(f'开始消费kafka数据')
        count = 0
        cmd = 'kafka-console-consumer --zookeeper dataom102test6:2181,dataom102test8:2181,dataom102test10:2181/dataom102testkafka --topic bbd_queue_sync_20171121 --from-beginning |grep "{}"'.format(
            bbd_id)
        result = self.execute_cmd(cmd, timeout=5)
        lines = \
            [line for line in result.split("\n") if f'"bbd_table":"{bbd_table}"' in line and bbd_id in line]
        while len(lines) == 0 and count <= 3:
            result = self.execute_cmd(cmd, timeout=5)
            lines = \
                [line for line in result.split("\n") if f'"bbd_table":"{bbd_table}"' in line and bbd_id in line]
            count += 1
        else:
            if lines:
                line = lines[0]
                logger.info(f"kafka消费数据{line}")
                return json.loads(line.replace("[01;31m[K", "").replace("[m[K", ""))
            else:
                return {}

    def getHbaseData(self, path, count=None, timeout=90):  # 循环取数据，超过timeout时间没有取到就返回空
        """
        :param path: hbase路径
        :param count: 上一次查询到的条数，如果不传就返回路径下当前存在数据的条数
        :param timeout: 超时时间
        :return: 新增的数据
        """
        cmd = rf'hadoop fs -cat {path}'
        start_time = time.time()
        result = self.execute_cmd(cmd, timeout=90)
        lines = [json.loads(line.replace("\r", "")) for line in result.split("\n") if '"data"' in line]
        if count is not None:
            while time.time() - start_time <= timeout:
                if len(lines) > count:
                    logger.info(f"查找到Hadoop储存数据,用时{time.time() - start_time}秒")
                    logger.info(f"hadoop存储数据{lines[count::]}")
                    return lines[count::]
                else:
                    time.sleep(3)
                    result = self.execute_cmd(cmd, timeout=90)
                    lines = [json.loads(line.replace("\r", "")) for line in result.split("\n") if '"data"' in line]
            else:
                print("\n")
                logger.info(f"hadoop存储数据{[]}")
                return []
        else:
            return len(lines)

    def upload(self, src_file, dsc_path):
        """
        上传文件
        :param src_file: 文件本地路径
        :param dsc_path: 上传到服务器的路径
        :return:
        """
        self.connect()
        sftp = paramiko.SFTPClient.from_transport(self._transport)
        path, filename = os.path.split(src_file)
        try:
            sftp.put(src_file, os.path.join(dsc_path, filename))
            logger.info(f"{src_file}上传成功")
        except Exception as e:
            logger.info(f"{src_file}文件上传失败")
            logger.error(e)
        sftp.close()
        self.close()

    def execute_cmd(self, cmd, timeout=None):
        """
        执行命令
        :param cmd: 需要执行的命令
        :param timeout: 超时时间，超过时间没有取到数据，则返回当前拿到的数据
        :return: 返回当前命令控制台打印的内容
        """
        self.connect()
        _channel = self._transport.open_session()
        if timeout and isinstance(timeout, int):
            _channel.settimeout(timeout)
        _channel.get_pty()
        _channel.invoke_shell()
        _channel.send(cmd + '\r')
        logger.info(f'执行命令：{cmd}')
        result = ""
        while not result.endswith("$ "):  # 循环获取控制台打印的内容
            try:
                sleep(0.5)
                ret = _channel.recv(65535)
                ret = ret.decode('utf-8', errors="ignore")
            except Exception:  # 发生错误，就停止获取
                _channel.send("\x03")  # 发送ctrl+c停止运行命令
                time.sleep(2)
                _channel.close()
                logger.info(f'channel关闭')
                self.close()
                return result
            else:
                result += ret  # 没有发生错误就把前一次获取的内容拼接到新获取的内容上
        else:
            _channel.close()
            logger.info(f'channel关闭')
            self.close()
            return result

    def start_app(self, query_cmd, cmd):
        """
        启动程序
        :param query_cmd: 查看程序是否启动的命令
        :param cmd: 启动程序的命令
        :return: 程序的pid
        """
        result = self.execute_cmd(query_cmd)
        line = [line.replace("\r", "") for line in result.split("\n") if "java" in line]
        while len(line) == 0:
            self.execute_cmd(cmd)
            time.sleep(1)
            result = self.execute_cmd(query_cmd)
            line = [line.replace("\r", "") for line in result.split("\n") if "java" in line]
        else:
            pid = line[0].split(" ")[3]
            name = line[0].split(" ")[-1]
            logger.info(f"程序已启动，name={name}, pid={pid}")
        return pid

    def start_app_offline(self, query_cmd, cmd):
        """
        启动offline推送程序，推送完成之后，程序会自动关闭
        :param query_cmd: 检查程序是否关闭的cmd命令
        :param cmd: 启动程序的cmd
        :return: 推送完成之后返回True
        """
        logger.info("开始离线推送数据")
        result = self.execute_cmd(query_cmd)
        line = [line.replace("\r", "") for line in result.split("\n") if "java" in line]
        if len(line) != 0:
            pid = line[0].split(" ")[3]
            self.kill_app(pid)
            self.execute_cmd(cmd)
        else:
            self.execute_cmd(cmd)
        num = 1
        while num != 0:
            result = self.execute_cmd(query_cmd)
            num = len([line.replace("\r", "") for line in result.split("\n") if "java" in line])
            time.sleep(2)
        else:
            logger.info("离线推送数据完成")
            return True

    def kill_app(self, pid):
        """
        关闭APP
        :param pid:程序的pid
        :return:
        """
        self.execute_cmd(f"kill -9 {pid}")
        logger.info(f"程序已关闭，pid={pid}")

    def start_app_spark(self, cmd, timeout=None):
        """
        启动spark-offline推送程序
        :param cmd: 启动命令
        :param timeout: 超时时间，多少时间后，控制台没有打印新的内容，则推送完成
        :return: 控制台打印的log信息
        """
        logger.info("开始spark-offline推送")
        app_path = get_config_values("SparkOffline-server", "app_path")
        self.connect()
        _channel = self._transport.open_session()
        if timeout and isinstance(timeout, int):
            _channel.settimeout(timeout)
        _channel.get_pty()
        _channel.invoke_shell()
        _channel.send(f"cd {app_path}" + '\r')
        logger.info(f'进入程序所在路径')
        _channel.send(cmd + "\r")
        logger.info(f"执行命令:{cmd},启动推送程序")
        result = ""
        while not result.endswith("$ "):
            try:
                sleep(0.5)
                ret = _channel.recv(65535)
                ret = ret.decode('utf-8')
            except Exception:
                _channel.send("\x03")  # 发送ctrl+c停止运行命令
                time.sleep(timeout)
                _channel.close()
                logger.info(f'channel关闭')
                self.close()
                return result
            else:
                result += ret
        else:
            _channel.close()
            logger.info(f'channel关闭')
            self.close()
            return result


if __name__ == '__main__':
    ConnectRemoteServer(server_name="AutoPushApp-server").upload(
        src_file=r"D:\pythonProject\OpenAPITest\TestData\2021-04-27\OfflineAutoPush\test_data.csv",
        dsc_path=r"/data1/dataom/dp-huangyujun/autopush/autopush-offline/")
