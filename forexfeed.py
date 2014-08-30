#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import urllib2
import datetime
import json
import time

reload(sys)
sys.setdefaultencoding('utf-8')


def main():

    url = "http://rate-exchange.appspot.com/currency?from=usd&to=jpy"
    last_rate = 0

    while True:

        nowtime = datetime.datetime.now()
        response = urllib2.urlopen(url)
        response_json = response.read()
        response_dict = json.loads(response_json)

        rates_dict = {"datetime":nowtime.strftime("%Y-%m-%d %H:%M:%S"),
                      "ccypair":response_dict["from"]+response_dict["to"],
                      "rate":response_dict["rate"]
        }

        if last_rate != rates_dict["rate"]:
            print rates_dict

        time.sleep(1.0)
        last_rate = rates_dict["rate"]



if __name__ == '__main__':

    sys.exit(main())
