from flask import Flask, request
import logging
import os.path
import psycopg2
import pandas as pd
import json
import csv
import sys

app = Flask(__name__)
app.logger.setLevel(logging.INFO)

formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')

@app.post("/events")
def events_post():

    app.logger.info('started events_post')

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port = '5432'
    )

    cur = con.cursor()
    try:
        file = request.files["file"]
        res = pd.read_csv(file).values.tolist()
        x = 0
        for i in range(len(res)):
            cur.execute('insert INTO events values ( {} ) ON CONFLICT DO NOTHING RETURNING 1'.format(", ".join("'"+str(item)+"'" for item in res[i])))
            a = cur.fetchone()
            if a != None:
                x += a[0]
    except Exception as e:
        print(e)
        app.logger.warning('Encountered error: ', e)
        return str(e),403

    con.commit()

    con.close()

    app.logger.info('Inserted {} amount of rows'.format(x))

    return 'Inserted {} amount of rows'.format(x),200

@app.get('/clear')
def clear_base():
    app.logger.info('started clear_base')

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port='5432'
    )

    cur = con.cursor()

    cur.execute('delete from events')
    con.commit()

    con.close()

    app.logger.info('Cleared DB')

    return 'Cleared DB',200

@app.get('/all')
def stats_all():

    app.logger.info('started stats_all')

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port='5432'
    )

    cur = con.cursor()

    cur.execute('select count(*) from events t')

    res = cur.fetchall()

    con.close()

    app.logger.info('There are a total of {} rows in events table'.format(res[0][0]))

    return res

@app.get('/stats/dau')
def stats_dau():

    app.logger.info('started stats_dau')

    start_date = request.args.get("from")
    end_date = request.args.get("to")

    if start_date == None or end_date == None:
        app.logger.warning('Not enough attributes')
        return 'Not enough attributes', 403

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port='5432'
    )

    cur = con.cursor()

    sql = '''
                with cte as (
                select distinct date(t.occurred_at) occurred_at,
                                t.user_id
                     from events t
                where t.occurred_at between date'{}' and date'{}'
                )

                select t.occurred_at,
                   count(t.user_id) user_qty
            from cte t
            group by t.occurred_at
                '''.format(start_date, end_date)
    try:
        cur.execute(sql)
        res = cur.fetchall()
    except Exception as e:
        print(e)
        app.logger.warning('Encountered error: ', e)
        return str(e), 403

    con.close()

    return res

@app.get('/stats/top-events')
def stats_top():

    app.logger.info('started stats_top')

    limit = request.args.get("limit")
    start_date = request.args.get("from")
    end_date = request.args.get("to")

    if (limit == None) or (start_date == None) or (end_date == None):
        app.logger.warning('Not enough attributes')
        return 'Not enough attributes', 403

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port='5432'
    )

    cur = con.cursor()

    sql = '''
                with cte as (
                select distinct t.event_type,
                                t.event_id
                     from events t
                where t.occurred_at between date'{}' and date'{}'
                )

                select t.event_type,
                        count(t.event_id) event_qty
            from cte t
            group by t.event_type
            order by count(t.event_id) desc
            LIMIT {}
                '''.format(start_date, end_date, limit)

    try:
        cur.execute(sql)
        res = cur.fetchall()
    except Exception as e:
        print(e)
        app.logger.warning('Encountered error: ', e)
        return str(e), 403


    con.close()

    return res

@app.get('/stats/retention')
def stats_retention():

    app.logger.info('started stats_retention')

    start_date = request.args.get("start_date")
    windows = request.args.get("windows")

    if (start_date == None) or (windows == None):
        return 'Not enough attributes', 403

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port='5432'
    )

    cur = con.cursor()

    sql = '''with cte as (
                select distinct date(t.occurred_at) occurred_at, 
                                t.user_id
                     from events t
                where date(t.occurred_at) = date'{}'
                ),
                
                cte1 as (
                select distinct date(t.occurred_at) occurred_at, 
                                t.user_id
                     from events t
                where date(t.occurred_at) = date'{}' + {}
                ),
        res as (
            select distinct count(t1.user_id) new,
                                count(t.user_id) old
            from cte t
            left join cte1 t1 on t1.user_id = t.user_id)
        
        select cast(t.new as REAL)/greatest(cast(t.old as real),1) from res t
                '''.format(start_date, start_date, windows)

    try:
        cur.execute(sql)
        res = cur.fetchall()
    except Exception as e:
        print(e)
        app.logger.warning('Encountered error: ', e)
        return str(e), 403

    con.close()

    return res

def import_history(csv_path):

    con = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="ag424S9+",
        host="db",
        port='5432'
    )

    cur = con.cursor()

    if not os.path.isfile(csv_path):
        sys.stdout.write('No file')
        return 1

    x = 0

    try:
        res = pd.read_csv(csv_path).values.tolist()
        for i in range(len(res)):
            cur.execute('insert INTO events values ( {} ) ON CONFLICT DO NOTHING RETURNING 1'.format(
                ", ".join("'" + str(item) + "'" for item in res[i])))
            a = cur.fetchone()
            if a != None:
                x += a[0]
    except Exception as e:
        sys.stdout.write(str(e))
        sys.stdout.write('UNIQUE constraint failed')
        return 1

    con.commit()

    con.close()

    sys.stdout.write('Inserted {} amount of rows'.format(x))

    return 0

def cli_import():
    if len(sys.argv) == 2:
        import_history(sys.argv[1])
    else:
        sys.stdout.write('Wrong attributes')
        return 1
    return 0


if __name__ == "__main__":
    cli_import()

