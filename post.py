#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib2
import sys
import json

if __name__ == '__main__':
   
    data = {'query': sys.argv[1]}

    req = urllib2.Request('http://127.0.0.1:8082/hive-server/api/task/submit',
                          json.dumps(data),
                          {'Content-Type': 'application/json'})
    f = urllib2.urlopen(req)
    print f.read()
    
