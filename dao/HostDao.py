from db.DbHelper import DbHelper

class HostDAO:
   __db = None;

   def __init__(self):
       self.__db = DbHelper()

   def getAll(self):
       return self.__db.query("SELECT * FROM host", None).fetchall();

if __name__ is '__main__':
    print(HostDAO().getAvailableCpu())