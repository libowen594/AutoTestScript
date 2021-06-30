from Common.DOP import DOP
from Common.ExecuteSql import ExecuteSql
import datetime


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

    def makeOpenApiPushData(self, tablename, fields):
        databases = ExecuteSql.get_databases(bbd_table=tablename)
        if "bbd_dp_business" in databases:
            database = "bbd_dp_business"
        elif "bbd_dp_datacube" in databases:
            database = "bbd_dp_datacube"
        elif "bbd_higgs" in databases:
            database = "bbd_higgs"
        else:
            return None
        if database == "bbd_higgs":
            relational_table = f"xgxx_relation"  # relational_table关系表的名称
        else:
            relational_table = f"{tablename.split('_')[0]}_enterprise_rel"
        sql = f"""SELECT * FROM {database}.{tablename} LIMIT 0,10"""
        if isinstance(fields, str):
            field_list = fields.replace(' ', '').split(',')
        elif isinstance(fields, list):
            field_list = fields
        else:
            raise Exception("参数错误")
        res = self.execute_sql(sql)
        for i in res:
            data = {
                "data": {
                    tablename: [

                    ]
                },
                "bbd_qyxx_id": None,
                "company_name": None,
                "tn": tablename
            }
            table_data = {}
            for field in field_list:
                table_data.update({field: str(i.get(field, None)) if isinstance(i.get(field, None),
                                                                                datetime.datetime) else i.get(field,
                                                                                                              None)})
            data["data"][tablename].append(table_data)
            data["bbd_qyxx_id"] = i.get("bbd_qyxx_id", None)
            data["company_name"] = i.get("company_name", None) or i.get("entity_name", None)
            if not data["bbd_qyxx_id"]:
                bbd_xgxx_id = i.get("bbd_xgxx_id")
                sql = f"""SELECT * FROM {database}.{relational_table} WHERE bbd_xgxx_id="{bbd_xgxx_id}" AND bbd_table="{tablename}" LIMIT 0,1"""
                xgxx_data = self.execute_sql(sql)
                if xgxx_data:
                    data["bbd_qyxx_id"] = xgxx_data[0].get("bbd_qyxx_id")
                    if not data["company_name"]:
                        data["company_name"] = xgxx_data[0].get("company_name", None) or xgxx_data[0].get("entity_name",
                                                                                                          None)

            print(data)


class CreatePusher(DOP):
    def CreatePusherByOfficial(self, pusher_id):
        data, handle = self.getConfigureInfo(pusher_id)


if __name__ == '__main__':
    cd = CreateData()
    cd.makeOpenApiPushData(tablename="qyxg_xzxk",
                           fields="bbd_dotime,bbd_xgxx_id,create_time,license_start_date")
