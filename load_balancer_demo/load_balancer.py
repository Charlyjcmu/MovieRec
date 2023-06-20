import sys
import os
from flask import jsonify
from flask import Flask
import pickle
import json
from flask import request, make_response
import requests
import numpy as np
import random
import time
"""
conda activate dockerDemo
python3 load_balancer.py
Open: http://localhost:8082
With 70% chance you will see Server A and with 30% Server B. This is an example of load balancing.
"""
app = Flask('load-balancer-server')

probability = 0.25

def checkHealth(ip_addr):
    return os.system('nc -vz '+ip_addr) == 0

@app.route('/')
def welcome():
    # add health check
    Server_up = [checkHealth('0.0.0.0 7004'), checkHealth('0.0.0.0 7005'), 
    		  checkHealth('0.0.0.0 7006'),checkHealth('0.0.0.0 7007')]
    response = ','.join(str(i) for i in Server_up)
    return str(response)

@app.route('/recommend/<userid>')
def predict(userid):
    stime = time.time()
    # add health check
    Server_up = [checkHealth('0.0.0.0 7004'), checkHealth('0.0.0.0 7005'), 
    		  checkHealth('0.0.0.0 7006'),checkHealth('0.0.0.0 7007')]
    working = [i for i,x in enumerate(Server_up) if x]
    if(not len(working)):
    	return ''
    randVal = np.random.randint(0,len(working))
    #print("server up: ",Server_up)
    port = 7004+working[randVal]
    response = f"{requests.get(f'http://0.0.0.0:{port}/recommend/{userid}').text}"
    
    etime= time.time()
    print("time per Request:", etime-stime)
    print("response is ",response)
    return str(response)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8082, debug=False)
    welcome()
