from db.DbHelper import DbHelper


class CpuStatusDAO:
    __db = None;

    def __init__(self):
        self.__db = DbHelper()

    def getAll(self):
        return self.__db.query("SELECT * FROM cpu_status", None).fetchall();

    def getAvailableCpuFromHost(self, host):
        return self.__db.query("""
         SELECT * FROM cpu_status
         WHERE status='idle' AND
         host=%s
         """, host).fetchall()

    def getAvailableCpu(self):
        return self.__db.query("SELECT * FROM cpu_status WHERE status = 'idle'", None)

    def updateCpuStatus(self, status, host, cpuNumber):
        self.__db.query("""
         UPDATE cpu_status 
         SET status=%s
         WHERE host=%s AND cpu=%s
         """, (status, host, cpuNumber))

        self.__db.commit()


if __name__ == '__main__':
    print(CpuStatusDAO().getAvailableCpuFromHost('intel_worker2'))
    # print(CpuStatusDAO().updateCpuStatus('idle', 'edge_cloud_master', '1'))
