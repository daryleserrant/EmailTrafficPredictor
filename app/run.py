from flask import Flask
from flask_apscheduler import APScheduler
from gmail_modeling import update_model

class Config(object):
    JOBS = [
        {
            'id': 'job_modelupate',
            'func': '__main__:update_model',
            'trigger': 'interval',
            'seconds': 10
        }
    ]

    SCHEDULER_VIEWS_ENABLED = True

app = Flask(__name__)
app.config.from_object(Config())
app.debug = True

scheduler = APScheduler()

if __name__ == "__main__":
    scheduler.init_app(app)
    scheduler.start()
    
    app.run()