from db.DbHelper import DbHelper

class WorkloadDAO:
   __db = None;

   def __init__(self):
       self.__db = DbHelper()

   def getAll(self):
       return self.__db.query("SELECT * FROM workload_type", None).fetchall();

class ExpectedTimeDAO:
    __db = None;

    def getAll(self):
        return self.__db.query("SELECT * FROM expected_time_preset", None).fetchall();

    def __init__(self):
        self.__db = DbHelper()

    def updateExpectedTime(self, workload, time):
        self.__db.query("""
          UPDATE expected_time_preset
          SET expected_time=%s
          WHERE workload=%s
          """, (time, workload))

        self.__db.commit()

class ExpectedEndTimeDAO:
    __db = None;

    def getExpectedEndTime(self, host):
        lists =  self.__db.query("""
            SELECT * FROM expected_end_time
            WHERE host=%s
        """, host).fetchall()
        new_list = []
        for data in lists:
            data['end_time'] = float(data['end_time'])
            new_list.append(data)
        return new_list

    def getExpectedEndTimeByUID(self, uid):
        lists = self.__db.query("""
                    SELECT * FROM expected_end_time
                    WHERE uid=%s
                """, uid).fetchall()
        record = lists[0]
        record['end_time'] = float(record['end_time'])
        return record

    def __init__(self):
        self.__db = DbHelper()

    def insertExpectedEndTime(self, uid, host, workload, start_time, end_time):
        self.__db.query("""
          INSERT INTO expected_end_time
          VALUES(%s, %s, %s, %s, %s)
          """, (uid, host, workload, start_time, end_time))

        self.__db.commit()

    def deleteData(self, uid):
        self.__db.query("""
          DELETE FROM expected_end_time
          WHERE uid=%s
          """, uid)

        self.__db.commit()

    def deleteAllData(self):
        self.__db.query("""
                  DELETE FROM expected_end_time
                  """, None)

        self.__db.commit()

    def updateEndTime(self, uid, updated_value):
        self.__db.query("""
          UPDATE expected_end_time
          SET end_time=%s
          WHERE uid=%s
          """, (updated_value, uid))

        self.__db.commit()

if __name__ == '__main__':
    # ExpectedEndTimeDAO().updateEndTime('0', '11.66', 'img-dnn')
    # print(ExpectedEndTimeDAO().getExpectedEndTime('intel_worker1'))
    # ExpectedEndTimeDAO().insertExpectedEndTime('0', 'intel_worker2', 'img-dnn', '12.1911610472023', '18.979096907398')
    # ExpectedEndTimeDAO().insertExpectedEndTime('4', 'intel_worker2', 'sphinx', '12.1911610472023', '18.979096907398')
    ExpectedEndTimeDAO().deleteAllData()