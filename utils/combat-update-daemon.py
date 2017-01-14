import reports
import time
import os

host = os.getenv("COMBATS_DB_HOST", "localhost")
user = os.getenv("COMBATS_DB_USER", "root")
passwd = os.getenv("COMBATS_DB_PASS", None)
DATA_DIR = "./data"
pause = os.getenv("UPDATE_TIMER", 120)


def get_players():
    rc = reports.ReportCollection(host, user, passwd)
    players = rc.get_players()
    rc.close()
    return players

def player_update_daemon():
    while True:
        players = get_players()
        print players
        for i in players:
            reports.import_reports(i, host, user, passwd)
        time.sleep(float(pause))

if __name__ == '__main__':
    player_update_daemon()