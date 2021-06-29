from Common.DOP import DOP
from Common.ExecuteSql import ExecuteSql


class CreateData(ExecuteSql):
    def makeDirectoriesData(self):
        sql = """SELECT * FROM bbd_dp_biz.qyxx_tag WHERE bbd_qyxx_id = '006734ef2af845c98aacfcd96dd0e56e' LIMIT 0,1000"""
        res = self.execute_sql(sql)
        for i in res:
            data = {"tn": "qyxx_tag", "data": {"qyxx_tag": [
                {"uptime": str(i["uptime"]), "tag": i["tag"], "bbd_qyxx_id": i["bbd_qyxx_id"],
                 "company_name": i["company_name"]}]}, "bbd_qyxx_id": i["bbd_qyxx_id"],
                    "company_name": i["company_name"]}
            print(data)


class CreatePusher(DOP):
    def CreatePusherByOfficial(self, pusher_id):
        data, handle = self.getConfigureInfo(pusher_id)


if __name__ == '__main__':
    cd = CreateData()
    cd.makeDirectoriesData()
