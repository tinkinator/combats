from flask import Flask, jsonify, request, g, redirect, session, _app_ctx_stack
from datetime import  datetime
import reports
import os, json
import errors


app = Flask(__name__)
app.secret_key = "MysteriousCapybara"
host = os.getenv("COMBATS_DB_HOST", "localhost")
user = os.getenv("COMBATS_DB_USER", "root")
apphost = os.getenv("APPHOST", "localhost")
passwd = os.getenv("COMBATS_DB_PASS", None)


@app.before_request
def connect_db():
    g.db = reports.ReportCollection(host, user, passwd)


@app.after_request
def close_connection(response):
    g.db.close()
    return response


@app.errorhandler(errors.DatabaseError)
def handle_database_error(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


@app.route('/', methods=['GET'])
def get_status():
    return jsonify({'Server running': "True"})

@app.route('/combats/weekly/<alliance_name>', methods=['GET'])
def get_alliance_combats(alliance_name):
    try:
        combat_list = g.db.last_week_combat(alliance_name)
        return jsonify(combat_list)
    except Exception as e:
        return jsonify({'Error': str(e)})


@app.route('/combats/totals/<alliance_name>', methods=['GET'])
def get_alliance_totals(alliance_name):
    try:
        totals_list = g.db.get_alliance_totals(alliance_name)
        return jsonify(totals_list)
    except Exception as e:
        return jsonify({'Error': str(e)})

@app.route('/combats/topten/<alliance_name>', methods=['GET'])
def get_alliance_topten(alliance_name):
    try:
        topten_list = g.db.last_week_topten(alliance_name)
        return jsonify({'list': topten_list})
    except Exception as e:
        return jsonify({'Error': str(e)})

@app.route('/apikey/', methods=['POST'])
def apikey_create():
    data = json.loads(request.data)
    try:
        result = g.db.add_apikey(data['player'], data['key'])
        if result:
            return ("success", 200)
        else:
            raise errors.DatabaseError("Failed to save key")
    except Exception as e:
        return jsonify({'Error': str(e)})


@app.route('/player/<id>/apikey', methods=['GET'])
def get_apikey(id):
    try:
        key = g.db.get_apikey(id)
        return jsonify({'key': key}) if len(key) > 0 else jsonify({'key':"no key exists"})
    except Exception as e:
        return jsonify({'Error': str(e)})


@app.route('/players/', methods=['POST'])
def create_player():
    data = json.loads(request.data)
    try:
        result = g.db.get_or_create_player(data)
        if result:
            return jsonify({'Result': result})
        else:
            raise errors.DatabaseError("Failed to save player")
    except Exception as e:
        return jsonify({'Error': str(e)})


@app.route('/players/', methods=['GET'])
def show_players():
    try:
        result = g.db.get_players()
        if result:
            return jsonify(result)
    except Exception as e:
        return jsonify({'Error': str(e)})


@app.route('/dbupd/', methods=['GET'])
def update():
    try:
        r = g.db.upd_units()
        print "Update success"
        return jsonify({'Update': r})
    except Exception as e:
        print str(e)
        return jsonify({'Error': str(e)})

@app.route('/combats/sieges/', methods=['POST'])
def get_siegecombats():
    try:
        sieges = json.loads(request.data)
        print sieges
        result = g.db.get_siege_data(sieges)
        return jsonify({'Result': result})
    except Exception as e:
        print str(e)
        return jsonify({'Error': str(e)})

if __name__ == '__main__':
    print "App running, database host: %s" % host
    app.run(debug=True, host=apphost)

