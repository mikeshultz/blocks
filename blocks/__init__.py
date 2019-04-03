from flask import Flask

"""
So, the entire point of the flask app is to satisfy ECS health checks.  There's 
no reason this is necessary for the consumer to function.  
TODO: Perhaps this can be done away with somehow?
TODO: Or at least make this health check a little more useful?
"""
app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello, World!'


def api():
    """ Run the debug server """
    app.run(port=8080)
