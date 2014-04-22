#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
import sys
import json

if __name__ == '__main__':
   
    data = {'query': sys.argv[1]}

    req = urllib2.Request('http://10.20.8.31:8089/hive-server/api/task/submit',
                          json.dumps(data),
                          {'Content-Type': 'application/json'})
    f = urllib2.urlopen(req)
    print f.read()
    
