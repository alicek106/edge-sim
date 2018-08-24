from db.DbHelper import DbHelper


class StatusDAO:
   __db = None;

   def __init__(self):
       self.__db = DbHelper()

   def getStatus(self):
       return self.__db.query("SELECT * FROM status", None).fetchall();

   def updateStatus(self, status, start_time, end_time, host, workload_name):
       self.__db.query("""
          UPDATE status
          SET status=%s, start_time=%s, end_time=%s
          WHERE host=%s and workload=%s
       """, (status, start_time, end_time, host, workload_name))
       self.__db.commit()