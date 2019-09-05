'''
CS 521 - Project

Create .py file for creating a database
Imported and used in Project script
'''

import sqlite3, os
from contextlib import closing
from collections import defaultdict


def create_db(db_name):

    path = os.getcwd()
    db_file = os.path.join(path, db_name)

# 2) Create Baseball Datebase
    if db_name == 'baseball.db':
        with closing(sqlite3.connect(db_file)) as connection:
            c = connection.cursor()
            try:
                c.execute("CREATE TABLE baseball_stats (player_name text, salary integer, games_played integer, average real)")
                connection.commit()
                connection.close()

            except Exception as exe:
                print(exe)

# 3) Create Stocks Database
    elif db_name == 'stocks.db':
        with closing(sqlite3.connect(db_file)) as connection:
            c = connection.cursor()
            try:
                c.execute("CREATE TABLE stock_stats\
                          (company_name text, ticker text, exchange_country text, price real,\
                          exchange_rate real, shares_outstanding real, net_income real,\
                          market_value_usd real, pe_ratio real)")
                connection.commit()
                connection.close()

            except Exception as exe:
                print(exe)

def get_col_names(db_name):
    path = os.getcwd()
    db_file = os.path.join(path, db_name)
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    if db_name == 'baseball.db':
        c.execute("PRAGMA TABLE_INFO('baseball_stats')")
        names = c.fetchall()
        print(names)
    else:
        c.execute("PRAGMA TABLE_INFO('stock_stats')")
        names = c.fetchall()
        print(names)


def insert(records):
    '''
    Insert each record in a list of ``records`` into the database.
    '''


def select():
    '''
    Select all the records from the database.
    Return them as a list of tuples.
    '''


if __name__ == '__main__':
    create_db('baseball.db')
    create_db('stocks.db')

