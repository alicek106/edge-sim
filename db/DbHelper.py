import pymysql.cursors
import pymysql
import os
import json

class DbHelper:
    __config = {}
    __connection = None;
    __cursor = None;

    def __init__(self):
        with open(os.path.dirname(__file__) + '/../config.json') as f:
            config = json.load(f)

        __db_config = config['mysql'];
        self.__connection = pymysql.connect(host=__db_config['host'],
                                            user = __db_config['user'],
                                            password = __db_config['password'],
                                            db = __db_config['db'],
                                            charset = 'utf8mb4',
                                            cursorclass = pymysql.cursors.DictCursor);
        self.__cursor = self.__connection.cursor();

    def query(self, query, params):
        if params is None:
            self.__cursor.execute(query)
        else:
            self.__cursor.execute(query, params)
        return self.__cursor;

    def commit(self):
        self.__connection.commit()

    def close(self):
        self.__connection.close();