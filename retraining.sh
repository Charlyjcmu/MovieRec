#!/bin/bash
echo hi
cd /home/team14/
python3 retraining.py # mention path
echo bye
echo settingdocker
pwd
/usr/local/bin/docker-compose down
/usr/local/bin/docker-compose up --build
#lsof -ti:8082|xargs kill -9
#python3 load_balancer_demo/load_balancer.py
