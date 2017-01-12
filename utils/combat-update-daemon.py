import reports
import yaml
import time
import os

host = os.getenv("COMBATS_DB_HOST", "localhost")
user = os.getenv("COMBATS_DB_USER", "root")
passwd = os.getenv("COMBATS_DB_PASS", None)
DATA_DIR = "./data"
reports_collection = reports.ReportCollection(host, user, passwd)
pause = os.getenv("UPDATE_TIMER", 120)


def player_update_daemon():
    while True:
        file = open(os.path.join(DATA_DIR, "player_keys.yml"))
        players = yaml.safe_load(file)
        for i in players['players']:
            reports.get_all_reports(i, reports_collection)
        time.sleep(pause)

if __name__ == '__main__':
    player_update_daemon()