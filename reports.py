import mysql.connector
from mysql.connector import errorcode
from datetime import datetime


class ReportCollection(object):

    def __init__(self, host='localhost', user='root', password=None):
        print "Connecting to database, user: %s, host: %s..." %(user, host)
        self.cnx = mysql.connector.connect(user=user, host=host, password=password)
        self.cursor = self.cnx.cursor(buffered=True)
        self._create_db()
        self.cnx.database = 'illycombat'

    def _create_db(self):
        try:
            self.cnx.database = 'illycombat'
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.cursor.execute(
                    "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format('illycombat'))
                self.cnx.database = 'illycombat'
                self._create_tables()
                # self.cursor.execute(
                #     "CREATE TRIGGER unit_type BEFORE INSERT on combat_details "
                #     "FOR EACH ROW SET @unit_type = (SELECT id from units WHERE name = NEW.unit_name)"
                # )
                self.insert_units()
            else:
                print err
                exit(1)

    def close(self):
        self.cursor.close()
        self.cnx.close()
        print "Connection to database closed"

    def _create_tables(self):
        TABLES = {}
        TABLES['combats'] = (
            "CREATE TABLE combats ("
            "  com_id varchar(65) NOT NULL,"
            "  com_datetime datetime NOT NULL,"
            "  PRIMARY KEY (com_id)"
            ") ENGINE=InnoDB")

        TABLES['players'] = (
            "CREATE TABLE players ("
            " player_id int(11),"
            " player_name varchar(60),"
            " alliance varchar(30),"
            " api_key varchar(200),"
            " PRIMARY KEY (player_id)"
            ") ENGINE=InnoDB"
        )

        TABLES['units'] = (
            "CREATE TABLE units ("
            " id int(11) AUTO_INCREMENT,"
            " name varchar(60),"
            " type varchar(40),"
            " attack int(11),"
            " defense int(11),"
            " xp int(11),"
            " speed int(11),"
            " PRIMARY KEY (id)"
            ") ENGINE=InnoDB"
        )

        TABLES['combat_details'] = (
            "CREATE TABLE combat_details ("
            "  id int(11) AUTO_INCREMENT,"
            "  com_id varchar(65) NOT NULL,"
            "  com_datetime datetime NOT NULL,"
            "  mapx int(11) NOT NULL,"
            "  mapy int(11) NOT NULL,"
            "  terrain varchar(60),"
            "  player_id int(11),"
            "  player_name varchar(60),"
            "  alliance varchar(30),"
            "  role varchar(20),"
            "  town_id int(11),"
            "  town_name varchar(100),"
            "  div_id MEDIUMINT,"
            "  div_name varchar(100),"
            "  unit_id TINYINT NOT NULL,"
            "  unit_name varchar(100),"
            "  unit_type varchar(40),"
            "  unit_quantity int(11),"
            "  unit_casualties int(11),"
            "  PRIMARY KEY (id),"
            "  FOREIGN KEY (com_id) REFERENCES combats (com_id)"
            "  ON UPDATE CASCADE ON DELETE CASCADE,"
            "  FOREIGN KEY (player_id) REFERENCES players (player_id)"
            "  ON UPDATE CASCADE ON DELETE CASCADE"
            ") ENGINE=InnoDB")


        for name, ddl in TABLES.iteritems():
            try:
                print "Creating table {}: ".format(name)
                self.cursor.execute("SET foreign_key_checks = 0")
                self.cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

        self.cnx.commit()


    def insert_units(self):
        from units import UNITS
        for k, v in UNITS.items():
            for i in v:
                self.cursor.execute(
                    "INSERT IGNORE INTO units (name, type) "
                    "VALUES (%s, %s)", (i, k)
            )

    def add_report(
        self,
        com_id,
        com_datetime,
        mapx,
        mapy,
        terrain,
        player_id,
        player_name,
        player_alliance,
        role,
        town_id,
        town_name,
        div_id,
        div_name,
        unit_id,
        unit_name,
        unit_quantity,
        unit_casualties,
        stratagem,
        duration
    ):
        self.cursor.execute(
            "SELECT COUNT(*) FROM combat_details WHERE com_id = %s and div_id = %s and unit_id = %s", (com_id, div_id, unit_id))
        result = self.cursor.fetchone()
        found = result[0]
        # insert new record
        if found == 0:
            self.cursor.execute(
                "SELECT type FROM units WHERE REPLACE(units.name, ' ', '') LIKE %s", (unit_name.replace(' ', ''),)
            )
            try:
                unit_type = self.cursor.fetchone()[0]
            except:
                unit_type = "Unknown"
            self.cursor.execute("SET foreign_key_checks = 0")
            update_report_set = [

                (
                    "INSERT IGNORE INTO combats (com_id, com_datetime) "
                    "VALUES (%s, %s)", (com_id, com_datetime)
                ),
                (
                    "INSERT IGNORE INTO combat_details (com_id, "
                    "com_datetime, mapx, mapy, terrain, player_id, player_name, alliance, role, "
                    "town_id, town_name, div_id, div_name, "
                    "unit_id, unit_name, unit_type, unit_quantity, unit_casualties, stratagem, duration) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                        com_id,
                        com_datetime,
                        mapx,
                        mapy,
                        terrain,
                        player_id,
                        player_name,
                        player_alliance,
                        role,
                        town_id,
                        town_name,
                        div_id,
                        div_name,
                        unit_id,
                        unit_name,
                        unit_type,
                        unit_quantity,
                        unit_casualties,
                        stratagem,
                        duration
                    )
                ),

            ]
            for (cmd, args) in update_report_set:
                self.cursor.execute(cmd, args)
                self.cnx.commit()


    def get_or_create_player(self, player):
        self.cursor.execute(
            "SELECT player_id, player_name, alliance, api_key FROM players "
            "WHERE player_id = %s",
            (player['id'],)
        )
        result = self.cursor.fetchall()
        if len(result) == 0:
            self.cursor.execute("SET foreign_key_checks = 0")
            self.cursor.execute(
                "INSERT IGNORE INTO players (player_id, player_name, alliance, api_key) "
                "VALUES (%s, %s, %s, %s)", (player['id'], player['name'], player['alliance'], player['api_key'])
            )
            self.cnx.commit()
            self.cursor.execute(
                "SELECT player_id, player_name, alliance, api_key FROM players "
                "WHERE player_id = %s",
                (player['id'],)
            )
            result = self.cursor.fetchall()
        return result[0]

# Methods for managing players and their API keys
    def get_players(self):
        self.cursor.execute(
            "SELECT * FROM players"
        )
        result = self.cursor.fetchall()
        result_dict = {'Players': []}
        for i in result:
            newdict = {'Id': i[0], 'Name': i[1], 'Alliance': i[2], 'Key': i[3]}
            result_dict['Players'].append(newdict)
        return result_dict


    def add_apikey(self, player_id, key):
        try:
            self.cursor.execute(
                "UPDATE players SET api_key = %s WHERE player_id = %s", (key, player_id)
            )
            self.cnx.commit()
            return True
        except:
            return False


    def get_apikey(self, player_id):
        self.cursor.execute(
            "SELECT api_key FROM players WHERE player_id = %s", (player_id,)
        )
        return self.cursor.fetchone()[0]


    def get_last_comdate(self, id):
        self.cursor.execute(
            "SELECT com_datetime from combat_details "
            "WHERE com_datetime = (SELECT MAX(com_datetime) from combat_details "
            "WHERE player_id = %s) ",
            (id,)
        )
        result = self.cursor.fetchone()
        if result is not None:
            return result[0]
        return result


# Combat data queries
    def get_hunt_results(self, player_name, start_date):
        self.cursor.execute(
            "SELECT com_datetime, unit_name, "
            "SUM(unit_casualties) FROM combat_details "
            "WHERE com_id in "
            "(SELECT com_id FROM combat_details "
            "WHERE player_name = %s "
            "AND role = 'Attacker' "
            "AND com_datetime >= %s) "
            "AND role = 'Defender' "
            "GROUP BY com_datetime, unit_name",
            (player_name, start_date)
        )
        results = self.cursor.fetchall()
        return results


    def get_alliance_casualties_detailed(self, alliance, start_date, end_date):
        self.cursor.execute(
            "SELECT com_datetime, player_name, role, town_name, unit_name, "
            "SUM(unit_casualties) "
            "FROM combat_details WHERE alliance = %s "
            "AND com_datetime >= %s AND com_datetime <= %s "
            "GROUP BY com_datetime, town_name, unit_name",
            (alliance, start_date, end_date)
        )
        result = self.cursor.fetchall()
        return result


    def last_week_combat(self, alliance):
        self.cursor.execute(
            "SELECT com_id, com_datetime, mapx, mapy, player_name, role, unit_type, SUM(unit_quantity), SUM(unit_casualties) "
            "FROM combat_details WHERE com_datetime >= NOW() - INTERVAL 7 DAY "
            "AND com_id IN (SELECT com_id FROM combat_details WHERE alliance = %s) "
            "GROUP BY com_id, player_name, unit_type",
            (alliance,)
        )
        result = self.cursor.fetchall()
        result_dict = {}
        for i in range(0, len(result)):
            if result[i][0] not in result_dict:
                newdict = {
                    'Timestamp': result[i][1],
                    'X': result[i][2],
                    'Y': result[i][3],
                    'Participants': [{'Player': result[i][4], 'Role': result[i][5], 'Unit type': result[i][6], 'Quantity': str(result[i][7]), 'Casualties': str(result[i][8])}]
                }
                result_dict[result[i][0]] = newdict
            else:
                participant = {'Player': result[i][4], 'Role': result[i][5], 'Unit type': result[i][6], 'Quantity': str(result[i][7]), 'Casualties': str(result[i][8])}
                result_dict[result[i][0]]['Participants'].append(participant)
        return result_dict


    def last_week_topten(self, alliance):
        sql = '''
                DROP TABLE IF EXISTS table1;
                CREATE TABLE table1 AS (
                SELECT DISTINCT com.com_id, com.player_name FROM combat_details AS com
                RIGHT JOIN (
                SELECT player_name FROM players WHERE alliance = "%s")
                AS pl ON com.player_name = pl.player_name
                WHERE YEAR(com.com_datetime) = YEAR(CURDATE()) AND MONTH(com.com_datetime) = MONTH(CURDATE()));
                DROP TABLE IF EXISTS table2;
                CREATE TABLE table2 AS(
                SELECT coms.com_id, table1.player_name, SUM(coms.unit_casualties) AS casualties
                FROM combat_details as coms
                RIGHT JOIN table1 ON table1.com_id = coms.com_id
                GROUP BY coms.com_id ORDER BY casualties DESC LIMIT 10
                );
                SELECT c.com_id, c.com_datetime, c.mapx, c.mapy, c.player_name AS enemy,
                c.role, c.unit_type, SUM(c.unit_quantity), SUM(c.unit_casualties), table2.casualties
                FROM combat_details AS c
                JOIN table2 ON c.com_id = table2.com_id
                GROUP BY com_id, enemy, unit_type
                ORDER BY casualties DESC;''' % alliance
        print sql
        res = self.cursor.execute(sql, multi=True)
        for cur in res:
            if cur.with_rows:
                res = cur.fetchall()
        result = []
        for row in res:
            print row
            key = row[0]
            player = row[4]
            utype = row[6]
            details = {
                        'Count': int(row[7]),
                        'Casualties': int(row[8])
                    }
            idx = len(result) - 1
            if len(result) == 0 or result[idx].keys()[0] != key:
                newDict= {
                    'Date': datetime.strftime(row[1], '%m-%d-%Y %H:%M:%S'),
                    'Total': int(row[9]),
                    'Coords': {'X': row[2], 'Y': row[3]},
                    'Players': {
                        player: {'Units':{utype: details}, 'Role': row[5]}}}
                result.append({key:newDict})
            else:
                if key == result[idx].keys()[0]:
                    if player in result[idx][key]['Players']:
                        if utype in result[idx][key]['Players'][player]['Units']:
                            result[idx][key]['Players'][player]['Units'][utype]['Count'] += int(row[6])
                            result[idx][key]['Players'][player]['Units'][utype]['Casualties'] += int(row[7])
                        else:
                            result[idx][key]['Players'][player]['Units'][utype] = details
                    else:
                        result[idx][key]['Players'][player] = {
                            'Units': {utype: details}, 'Role': row[5]
                        }
        return result

    def get_alliance_totals(self, alliance):
        sql = '''
                DROP TABLE IF EXISTS table1;
                CREATE TEMPORARY TABLE table1 AS
                (SELECT DISTINCT pl.player_name as player, com.com_id FROM players as pl JOIN combat_details as com
                ON pl.player_name=com.player_name WHERE pl.alliance = "%s"
                );
                SELECT table1.player, c.player_name as enemy, c.unit_type, SUM(c.unit_casualties) FROM combat_details
                AS c RIGHT JOIN table1 ON c.com_id = table1.com_id WHERE c.player_name != table1.player AND
                YEARWEEK(c.com_datetime) = YEARWEEK(NOW())
                GROUP BY player, enemy, c.unit_type''' % alliance
        print sql
        try:
            res = self.cursor.execute(sql, multi=True)
            for cur in res:
                if cur.with_rows:
                    results = self.cursor.fetchall()
            print results
            result_dict = {'Players': {}}
            for i in range(0, len(results)):
                ally = results[i][0]
                enemy = results[i][1]
                unit_type = results[i][2] if results[i][2] != '' else 'Unknown'
                casualties = int(results[i][3])
                if ally not in result_dict['Players'].keys():
                    result_dict['Players'][ally] = {'Enemies': {enemy: {'Units': {unit_type: casualties}}}, 'Total': casualties}
                else:
                    result_dict['Players'][ally]['Total'] += casualties
                    if enemy not in result_dict['Players'][ally]['Enemies'].keys():
                        result_dict['Players'][ally]['Enemies'][enemy] = {'Units': {unit_type: casualties}}
                    else:
                        if unit_type not in result_dict['Players'][ally]['Enemies'][enemy]['Units'].keys():
                            result_dict['Players'][ally]['Enemies'][enemy]['Units'][unit_type] = casualties
            return result_dict
            self.cnx.commit()
        except:
            print self.cursor.statement

    def get_siege_data(self, sieges):
        print "Getting siege data from %s" % sieges
        start = sieges['start']
        print "Start: %s" % start
        combats = {}
        for coords in sieges['coordinates']:
            print coords
            squarename = coords[2]
            sql = '''
                    SET @dt = "%s";
                    SELECT com_id, com_datetime, player_name, alliance, role, unit_name, unit_type,
                    SUM(unit_quantity), SUM(unit_casualties) 
                    FROM combat_details WHERE com_datetime >= @dt AND mapx = %s AND mapy = %s 
                    GROUP BY com_id, player_name, unit_name''' % (start, coords[0], coords[1])
            print sql
            try:
                res = self.cursor.execute(sql, multi=True)
                for cur in res:
                    if cur.with_rows:
                        res = self.cursor.fetchall()
                if len(res) > 0:
                    result = self.process_query_result(res)
                    print "Result: %s" %result
                    if squarename not in combats:
                        combats[squarename] = result
            except Exception as e:
                print "Error: %s" %e
        if len(combats.items()) > 0:
            return combats
        else:
            return "No combats were found at these coordinates"

    def get_player_kills(self, player_Id, start_date, end_date):
        self.cursor.execute(
            "SELECT `unit_name`, SUM(`unit_casualties`) "
            "FROM `combat_details` WHERE `com_id` in "
            "(SELECT `com_id` FROM `combat_details` WHERE `player_id` = %s) "
            "AND `player_id` !=%s AND com_datetime >= %s "
            "AND alliance not in (SELECT alliance FROM combat_details WHERE player_id = %s) "
            "AND com_datetime <= %s GROUP BY `unit_name`",

            (player_Id, player_Id, start_date, player_Id, end_date)
        )
        result = self.cursor.fetchall()
        return result


# Helper methods
    def check_combat_record(self, combatguid):
        self.cursor.execute(
            "SELECT * FROM combat_details "
            "WHERE com_id = %s",
            (combatguid,)
        )
        results = self.cursor.fetchall()
        if len(results) == 0:
            return True

    def upd_units(self):
        try:
            self.cursor.execute(
                "LOAD DATA LOCAL INFILE 'illy_units.csv' REPLACE INTO TABLE units "
                "COLUMNS TERMINATED BY ',' OPTIONALLY ENCLOSED BY \'\"\' "
                "LINES TERMINATED BY '\n' IGNORE 1 LINES "
                "(name, type, attack, xp, speed, def_spear, def_ranged, def_inf, def_cav, build_time);"
            )
            self.cnx.commit()
            return "success"
        except mysql.connector.Error as err:
            raise err

    def process_query_result(self, result):
        print "processing query result"
        print result
        coms = {}
        for i in range(0, len(result)):
            id = result[i][0]
            if id not in coms:
                print "Id %s, datetime: %s" % (id, result[i][1])
                coms[id] = {"Datetime":result[i][1], "Defenders": {}, "Attacker": {}}
            player = result[i][2]
            alliance = result[i][3]
            role = result[i][4]
            unitname = result[i][5]
            unittype = result[i][6]
            unitstotal = int(result[i][7])
            casualties = int(result[i][8])
            print player, alliance, role, unitname, unitstotal
            if role == 'Defender':
                if player not in coms[id]['Defenders'].keys():
                    coms[id]['Defenders'][player] = {'alliance': alliance, "units": {unitname: [unittype, unitstotal, casualties]}}
                else:
                    coms[id]['Defenders'][player]["units"][unitname] = [unittype, unitstotal, casualties]
            elif role == 'Attacker':
                coms[id]['Attacker'][player] = {'alliance': alliance, "units": {unitname: [unitstotal, casualties]}}
        return coms



