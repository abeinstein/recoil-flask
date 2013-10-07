import recoil_scripts
from apscheduler.scheduler import Scheduler


sched = Scheduler()

@sched.cron_schedule(hour=12, minute=0)
def update():
    recoil_scripts.update()

sched.start()
while True:
    pass

# def reload():
#     recoil_scripts.reload()

# @app.route('/')
# def home():
#     msg = '''
#     <h1>Welcome to Recoil!</h1>
#     <p>To update, add '/update' to the end of the URL<p>
#     <p>To reload the entire database, add '/reload_database' to the end of the URL.<p>

#     </hr>
#     <p>The database will update automatically every 24 hours at noon.</p>
#     '''
#     return msg
