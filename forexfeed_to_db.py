#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import ConfigParser
import pandas as pd
import os
from forex_db import ForexDb


reload(sys)
sys.setdefaultencoding('utf-8')


def get_files(dir="/"):
    list = os.listdir(dir)
    return list

def main():
    conf = ConfigParser.SafeConfigParser()
    conf.read("forex.ini")

    db = ForexDb(host=conf.get("DB",
                                  "host"),
                    user=conf.get("DB",
                                  "user"),
                    pas=conf.get("DB",
                                 "pass"),
                    database=conf.get("DB",
                                      "database"))

    infiledir = conf.get("FILE","path")

    files = get_files(infiledir)

    for file in files:
        print file
        yyyymmdd = file[0:8]
        yyyymm = file[0:6]

        forex_data = pd.read_csv(infiledir + file,chunksize=10000)

        """テーブルの作成"""
        db.create_db(yyyymm)
        db.Commit()

        """レコードの削除"""
        db.DeleteForex(yyyymmdd)
        db.Commit()

        """Insert"""
        for df in forex_data:
            [db.InsertForex(row["updatetime"],row["ccy_pair"],row["Bid"],row["Mid"],row["Ask"],row["Datetime"],yyyymm) for index,row in df.iterrows()]
            db.Commit()

        # """Load Data"""
        # db.AppendForex(infiledir + file,yyyymm)
        # db.Commit()

    """DB接続のクローズ"""
    db.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
