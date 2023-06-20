from flask import Flask, render_template
import os

app = Flask(__name__)


from movies import recommend

if __name__=="__main__":
	app.run(host='0.0.0.0')
