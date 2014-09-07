#!/usr/bin/python
# -*- coding: utf-8 -*-
import mysql.connector as mysql


class ForexDb:
    """
        Forexデータを管理するDB
    """
    def __init__(self, host,user,pas,database):
        self._host = host
        self._user = user
        self._pas = pas
        self._database = database

        self._conn = mysql.connect(host=host,user=user,password=pas,database=database)
        self._conn.text_factory = str
        self._cur = self._conn.cursor(buffered=True)

    def __del__(self):
        if self._conn:
            self.close()

    def close(self):
        self._cur.close()
        self._cur = None
        self._conn.close()
        self._conn = None

    def create_db(self,yyyymm=""):
        """
        月ごとのテーブルを作成する
        """
        """mysql create"""

        sql = '''CREATE TABLE IF NOT EXISTS forex_price_''' + str(yyyymm) + '''(
                                 updatetime datetime,
                                 ccy_pair varchar(8),
                                 Bid double,
                                 Mid double,
                                 Ask double,
                                 Datetime datetime,
                                 PRIMARY KEY(updatetime,ccy_pair)
                                 ) ENGINE=InnoDB DEFAULT CHARSET=utf8  ;'''
        self._cur.execute(sql)

    def AppendForex(self, filepath="",yyyymm=""):
        try:
            sql = "Load data INFILE '" + str(filepath) + "' INTO TABLE forex_price_" + str(yyyymm) + " FIELDS TERMINATED BY ',' IGNORE 1 LINES ;"
            self._cur.execute(sql)

        except mysql.IntegrityError:
            print "Error Occurred in load data " % (filepath)

    def InsertForex(self,updatetime,ccy_pair,bid,mid,ask,datetime,yyyymm=""):
        try:
            sql = "REPLACE INTO forex_price_" + str(yyyymm) + "(updatetime, ccy_pair, Bid, Mid,Ask,Datetime) VALUES( %s, %s, %s, %s, %s, %s) ;"
            self._cur.execute(sql,[updatetime,ccy_pair,bid,mid,ask,datetime])

        except mysql.IntegrityError:
            print "Error Occurred in insert " % (updatetime)

    def DeleteForex(self,yyyymmdd=""):
        try:
            yyyymm = str(yyyymmdd)[0:6]
            sql = "delete from forex_price_" + yyyymm + " where DATE(updatetime) = DATE(" + str(yyyymmdd) + ");"
            self._cur.execute(sql)
        except mysql.IntegrityError:
            print "Error Occurred in delete " % (str(yyyymmdd))

    def Commit(self):
        self._conn.commit()

    def Rollback(self):
        self._conn.rollback()
