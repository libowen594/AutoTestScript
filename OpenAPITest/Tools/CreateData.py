from Common.ExecuteSql import ExecuteSql


class CreateData(ExecuteSql):
    def makeDirectoriesData(self):
        sql = """SELECT qy.bbd_qyxx_id, qy.tag, qy.company_name, qy.uptime FROM bbd_dp_biz.qyxx_tag qy WHERE qy.tag  NOT  LIKE 'TX智慧楼宇%' 
 GROUP BY qy.bbd_qyxx_id LIMIT 100 """
        res = self.execute_sql(sql)
        for i in res:
            data = {"tn": "qyxx_tag", "data": {"qyxx_tag": [
                {"uptime": str(i["uptime"]), "tag": i["tag"], "bbd_qyxx_id": i["bbd_qyxx_id"],
                 "company_name": i["company_name"]}]}, "bbd_qyxx_id": i["bbd_qyxx_id"],
                    "company_name": i["company_name"]}
            print(data)


if __name__ == '__main__':
    cd = CreateData()
    cd.makeDirectoriesData()
