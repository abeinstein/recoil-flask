import recoil_scripts
from flask import Flask

app = Flask(__name__)

@app.route('/update')
def update():
    recoil_scripts.update()

@app.route('/reload_database')
def reload():
    recoil_scripts.reload()

@app.route('/')
def home():
    msg = '''
    <h1>Welcome to Recoil!</h1>
    <p>To update, add '/update' to the end of the URL<p>
    <p>To reload the entire database, add '/reload_database' to the end of the URL.<p>

    </hr>
    <p>The database will update automatically every 24 hours at noon.</p>
    '''
    return msg
