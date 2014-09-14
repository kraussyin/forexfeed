#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import urllib2
import datetime
import json
import time
import pandas as pd
from forex_db import ForexDb
import ConfigParser

reload(sys)
sys.setdefaultencoding('utf-8')


def main():

    url = "https://query.yahooapis.com/v1/public/yql?q=select%20Date%2CTime%2Cid%2CBid%2CAsk%2CRate%20from%20yahoo.finance.xchange%20where%20pair%20in(%22USDJPY%22%2C%22EURJPY%22%2C%22EURUSD%22)&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback="

    _retlist = []
    append = _retlist.append
    cnt = 0
    is_test = 0

    startday = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")
    endday = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")

    print startday
    outputfullpath = "" + startday + "_rate.csv"

    youbi = datetime.datetime.now().isoweekday()
    starthour = 0
    endhour = 30
    if youbi == 1:
        """月曜の場合は開始時間を遅らす"""
        starthour = 5
    if youbi == 6:
        """土曜の場合は修了時間を早める"""
        endhour = 7

    """開始待ち"""
    while True:
        """開始時刻よりも後になったらbreak"""
        if datetime.datetime.now().hour >= starthour:
            break

    while True:
        if startday != endday:
            break
        if cnt > 5 and is_test == 1:
            break
        if datetime.datetime.now().hour >= endhour:
            break

        try:
            """YQLを用いて現時点での為替レートを取得する。"""
            response = urllib2.urlopen(url)
            response_json = response.read()

            """keyとなる現時点の時刻"""
            nowtime = datetime.datetime.now()

            response_dict = json.loads(response_json)

            """取得結果はlist of dictになっている"""
            ccy_list_of_dict = response_dict["query"]["results"]["rate"]

            """現在時刻を付け足す"""
            map(lambda x:x.__setitem__(u"updatetime",nowtime),ccy_list_of_dict)

            """リストに追加していく"""
            [append(dict) for dict in ccy_list_of_dict]

            cnt += 1
            endday = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")


        except Exception as e:
            print '=== エラー内容 ==='
            print 'type:' + str(type(e))
            print 'args:' + str(e.args)
            print 'message:' + e.message
            print 'e自身:' + str(e)
            """エラーが出たら1分待つ"""
            time.sleep(60)

    """リストをデータフレームにする"""
    df = pd.DataFrame(_retlist)
    df.rename(columns={"id":"ccy_pair","Rate":"Mid"},inplace=True)
    df["Datetime"] = df["Date"] + " " + df["Time"]
    df["Datetime"] = map(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y %I:%M%p").strftime("%Y-%m-%d %H:%M:%S"),df["Datetime"])
    df["updatetime"] = map(lambda x:x.strftime("%Y-%m-%d %H:%M:%S"),df["updatetime"])
    df = df[["updatetime","ccy_pair","Bid","Mid","Ask","Datetime"]]

    """csvに吐き出す"""
    df.to_csv(outputfullpath,index=None)

    print df

    """DBへのInsert"""
    print "Insert into Data Base"
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

    """テーブルの作成"""
    print "Create Database"
    db.create_db(startday[0:6])
    db.Commit()
    
    """レコードの削除"""
    print "Deleate Record"
    db.DeleteForex(startday)
    db.Commit()

    """Insert"""
    print "Convert to List of Dict"
    list_of_dict = [row.to_dict() for index,row in df.iterrows()]
    print "Insert to DB"
    db.InsertForexMulti(list_of_dict,startday[0:6])
    db.Commit()

    """DB接続のクローズ"""
    db.close()


if __name__ == '__main__':

    sys.exit(main())
