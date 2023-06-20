# Movie Recommendations pipeline

*Recommends top 10 movies based on user reviews in docker containers*


## Installation

## Running without docker
create & activate virtual env then install dependency:

with venv/virtualenv + pip:
```
$ python -m venv env  # use `virtualenv env` for Python2, use `python3 ...` for Python3 on Linux & macOS
$ source env/bin/activate  # use `env\Scripts\activate` on Windows
$ pip install -r requirements.txt
```
or with Pipenv:
```
$ pipenv install --dev
$ pipenv shell
```
Then run the flask server
```
$cd movies
$ flask run -p 8082
```
Checking movie recommendations:
http://17645-team14.isri.cmu.edu:8082/recommend/userid - This will generate movie recommendations for this user

Example: http://17645-team14.isri.cmu.edu:8082/recommend/679775


##Running in Docker
Need to create a file called Keys.py with the 
```
pd="password of kafka"
ip="ip address for kafka"
```

Run in docker by starting the load balancer and the script
```
$python3 load_balancer_demo/load_balancer.py
```

Then Start the containers
```
$./retraining.sh
```


