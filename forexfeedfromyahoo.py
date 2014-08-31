#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import urllib2
import datetime
import json
import time
import pandas as pd

reload(sys)
sys.setdefaultencoding('utf-8')


def main():

    url = "https://query.yahooapis.com/v1/public/yql?q=select%20Date%2CTime%2Cid%2CBid%2CAsk%2CRate%20from%20yahoo.finance.xchange%20where%20pair%20in(%22USDJPY%22%2C%22EURJPY%22%2C%22EURUSD%22)&format=json&diagnostics=true&env=store%3A%2F%2Fdatatables.org%2Falltableswithkeys&callback="

    _retlist = []
    append = _retlist.append
    cnt = 0

    startday = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")
    endday = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d")

    print startday
    outputfullpath = "" + startday + "_rate.csv"

    while True:
        if cnt >= 5:
            break
        if startday != endday:
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
    df["Datetime"] = map(lambda x:datetime.datetime.strptime(x,"%m/%d/%Y %I:%M%p"),df["Datetime"])
    df = df[["updatetime","ccy_pair","Bid","Mid","Ask","Datetime"]]

    """csvに吐き出す"""
    df.to_csv(outputfullpath,index=None)

    print df


if __name__ == '__main__':

    sys.exit(main())
