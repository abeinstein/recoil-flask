import recoil_scripts
from apscheduler.scheduler import Scheduler
from flask import Flask

app = Flask(__name__)
app.config.from_envvar('APPLICATION_SETTINGS')
sched = Scheduler()

@sched.cron_schedule(hour=12, minute=0)
def update():
    recoil_scripts.update()

sched.start()
while True:
    pass

