from flask import Flask, jsonify, request, redirect, session, _app_ctx_stack
from datetime import  datetime
import reports
import os, json
import errors


app = Flask(__name__)
app.secret_key = "MysteriousCapybara"
host = os.getenv("COMBATS_DB_HOST", "localhost")
user = os.getenv("COMBATS_DB_USER", "root")
passwd = os.getenv("COMBATS_DB_PASS", None)
reports_collection = reports.ReportCollection(host, user, passwd)
apphost = os.getenv("APPHOST", "localhost")

@app.errorhandler(errors.DatabaseError)
def handle_database_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response

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

@app.route('/apikey/', methods=['POST'])
def apikey_create():
    data = json.loads(request.data)
    result = reports_collection.add_apikey(data['player'], data['key'])
    if result:
        return ("success", 200)
    else:
        raise errors.DatabaseError("Failed to save key")

@app.route('/player/<id>/apikey', methods=['GET'])
def get_apikey(id):
    key = reports_collection.get_apikey(id)
    return jsonify({'key': key}) if len(key) > 0 else jsonify({'key':"no key exists"})


if __name__ == '__main__':
    print "App running, database host: %s" % host
    app.run(debug=True, host=apphost)

