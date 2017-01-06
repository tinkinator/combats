from flask import Flask, jsonify, request, redirect, session, _app_ctx_stack
from datetime import  datetime
import reports
import os
import threading
import sys
import yaml
import time
import os


app = Flask(__name__)
app.secret_key = "MysteriousCapybara"
DATA_DIR = "./data"
host = os.getenv("COMBATS_DB_HOST", "localhost")
user = os.getenv("COMBATS_DB_USER", "root")
passwd = os.getenv("COMBATS_DB_PASS", None)
reports_collection = reports.ReportCollection(host, user, passwd)

def player_update_daemon():
    while True:
        file = open(os.path.join(DATA_DIR, "player_keys.yml"))
        players = yaml.safe_load(file)
        for i in players['players']:
            reports.get_all_reports(i, reports_collection)
        time.sleep(120)

def server_thread():
    app.run(debug=True)

@app.route('/', methods=['GET'])
def get_status():
    return jsonify({'Server running': "True"})

@app.route('/combats/alliance/<alliance_name>', methods=['GET'])
def get_alliance_combats(alliance_name):
    combat_list = reports_collection.last_week_combat(alliance_name)
    return jsonify(combat_list)

@app.route('/combats/totals/<alliance_name>', methods=['GET'])
def get_alliance_totals(alliance_name):
    totals_list = reports_collection.get_alliance_totals(alliance_name)
    return jsonify(totals_list)

if __name__ == '__main__':
    reports_collection.insert_units()
    thread1 = threading.Thread(target = player_update_daemon)
    thread1.setDaemon(True)
    thread1.start()
    app.run(debug=True)

