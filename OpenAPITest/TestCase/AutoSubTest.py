import json
import unittest
import ddt
from kafka import KafkaProducer

from Common.DOP import DOP
from Common.Excel import Excel
from Common.GetTestCase import pushCaseToExcel
from Common.ReadConfig import get_config_values
from Common.logger import get_logger

logger = get_logger(__name__)
pushCaseToExcel(sheet_name="AutoSub")
testData = Excel().get_sheet_values(sheet_name="AutoSub")
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
class AutoSubTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.DOPFront = DOP("DOPFront")
        cls.hosts = get_config_values("AutoSubTest", "hosts")
        cls.topic = get_config_values("AutoSubTest", "topic")

    def tearDown(self):
        print(f"推送数据：{self.send_data}")
        Excel().write_one_cell(value=self.result, col=5, row=self.row, sheet_name="AutoSub")
        print(f"测试结果：{self.result}")
        print("---------------------------------------------------------------------------")

    @ddt.data(*testData)
    def test_AutoSub(self, data):
        global row
        row += 1
        self.row = row
        directory_id = data["名录id"]
        self.send_data = data["推送数据"]
        qyxx_id = eval(self.send_data)["bbd_qyxx_id"]
        count = int(data["收到数据数量"])
        Producer(self.hosts, self.topic, json.dumps(eval(self.send_data), ensure_ascii=False))
        rel_count = self.DOPFront.getDirectory(directory_id, qyxx_id)
        try:
            self.assertEqual(count, rel_count, msg=f"应该收到{count}条数据，实际收到了{rel_count}条数据")
        except AssertionError as e:
            self.result = "Fail"
            raise e
        else:
            self.result = "Pass"


if __name__ == '__main__':
    unittest.main()
