from db.DbHelper import DbHelper


class DiscoveryDAO:
   __db = None;

   def __init__(self):
       self.__db = DbHelper()

   def getAll(self):
       return self.__db.query("SELECT * FROM container_discovery", None).fetchall();

   def getAvailableContainer(self, workload,  host):
       return self.__db.query("""
          SELECT * FROM container_discovery 
          WHERE status = %s AND workload = %s AND host = %s
          """, ("stopped", workload, host))

   def updateContainerStatus(self, status, containerId):
       self.__db.query("""
         UPDATE container_discovery 
         SET status=%s
         WHERE id=%s
         """, (status, containerId))

       self.__db.commit()


if __name__ == '__main__':
    print(DiscoveryDAO().updateContainerStatus('running', 'e7055336ac9c34b2c1a2affe79b8f4e774484fbedb46f3d29e11c9f1408e1e28\n'))