'''
ETL Project: Part 1 (Extract)
By: Alex Geronimo
Last Modified:

Extract, Transform, Log
'''

# Part 1: Extract

import csv, os
from datetime import datetime
import sqlite3
from contextlib import closing
import collections
from collections import Counter
import create_dbs

class AbstractRecord:
    def __init__(self,name):
        self.name = name



class BaseballStatRecord (AbstractRecord):

    def __init__(self, name, salary=None, g=None, ave=None):

        super().__init__(name)
        self.name = name
        self.salary = salary
        self.g = g
        self.ave = ave

    def __str__(self):
        return "<{0}>({1}: Salary: ${2}, Games Played: {3}, Batting Average: {4})"\
            .format(str(self.__class__.__name__), str(self.name), str(self.salary), str(self.g), str(self.ave))



class StockStatRecord (AbstractRecord):

    def __init__(self, company_name, ticker, exchange_country, price, exchange_rate,
                 shares_outstanding, net_income, market_value_usd=None, pe_ratio=None):

        super().__init__(ticker)
        self.company_name = company_name
        self.exchange_country = exchange_country
        self.price = price
        self.exchange_rate = exchange_rate
        self.shares_outstanding = shares_outstanding
        self.net_income = net_income
        self.market_value_usd = market_value_usd
        self.pe_ratio = pe_ratio

    def __str__(self):
        return "<{0}>({1}, {2}, {3}, Price: ${4}, {5}, {6}, ${7}, ${8}, {9})"\
            .format(str(self.__class__.__name__), str(self.name), str(self.company_name), str(self.exchange_country),
                    str(round(float(self.price),2)), str(round(float(self.exchange_rate),2)), str(round(float(self.shares_outstanding),2)),
                    str(round(float(self.net_income),2)), str(round(self.market_value_usd,2)), str(round(self.pe_ratio,2)))



class AbstractCSVReader:

    def __init__(self, path):
        self.path = path

    def row_to_record (self, row):
        try:
            return row
        except NotImplementedError:
            raise NotImplementedError

    def load (self):
        # Validate file exists
            if os.path.isfile(self.path) == True:
                pass
            else:
                raise FileNotFoundError

            record_list = []

            with open(self.path) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        dict_row = self.row_to_record(row)
                        if dict_row == '':
                            continue
                        else:
                            record_list.append(dict_row)
                    except NotImplementedError:
                        continue

            return record_list



class BaseballCSVReader (AbstractCSVReader):

    def __init__(self, path):
        super().__init__(path)

    def row_to_record(self, dict):
        self.unval_data = dict
        val_data = [None,None,None,None]
        str_keys = ['PLAYER']
        float_keys = ['SALARY','G','AVG']

        for k, v in self.unval_data.items():
            try:
                if k in float_keys:
                    if v == '':
                        raise BadData("Bad data in " + k + " for " + dict['PLAYER'])
                    elif float(v) < 0:
                        raise BadData("Bad data in " + k + " for " + dict['PLAYER'])
                    else:
                        if k == 'SALARY':
                            val_data[1] = int(v)
                        elif k == 'G':
                            val_data[2] = int(v)
                        elif k == 'AVG':
                            val_data[3] = float(v)
                        else:
                            raise BadData("Bad data in " + k + " for " + dict['PLAYER'])

                if k in str_keys:
                    if v == '':
                        raise BadData("Bad data in " + k + " for " + dict['PLAYER'])
                    else:
                        if k == 'PLAYER':
                            val_data[0] = str(v)

            except BadData as bd:
                print(bd)
                return ''

        return val_data



class StocksCSVReader (AbstractCSVReader):

    def __init__(self, path):
        super().__init__(path)

    def row_to_record(self, dict):
        self.unval_data = dict
        val_data = [None,None,None,None,None,None,None,None,None]
        str_keys = ['ticker', 'exchange_country', 'company_name']
        float_keys = ['price', 'exchange_rate', 'shares_outstanding', 'net_income']

        for k, v in self.unval_data.items():
            try:
                if k in float_keys:
                    if v == '':
                        raise BadData("Bad data in " + k + " for " + dict['company_name'])
                    elif v == '#DIV/0!':
                        raise BadData("Bad data in " + k + " for " + dict['company_name'])
                    elif float(v) <= 0:
                        raise BadData("Bad data in " + k + " for " + dict['company_name'])
                    else:
                        if k == 'price':
                            val_data[3] = float(v)
                        elif k == 'exchange_rate':
                            val_data[4] = float(v)
                        elif k == 'shares_outstanding':
                            val_data[5] = float(v)
                        elif k == 'net_income':
                            val_data[6] = float(v)

                if k in str_keys:
                    if v == '':
                        raise BadData("Bad data in " + k + " for " + dict['company_name'])
                    else:
                        if k == 'ticker':
                            val_data[1] = str(v)
                        elif k == 'exchange_country':
                            val_data[2] = str(v)
                        elif k == 'company_name':
                            val_data[0] = str(v)

            except BadData as bd:
                print(bd)
                return ''

        val_data[7] = val_data[3] * val_data[4] * val_data[5]
        val_data[8] = val_data[3] * (val_data[5]/val_data[6])

        return val_data


class BadData (Exception):

    def __init__(self, message):
        super().__init__(message)

# Part 2: Transform (Create Database)
# 4) Create Abstract DAO

class AbstractDAO:


    def __init__(self,db_name):
        self.db_name = db_name

    def insert_records(self,records):
        raise NotImplementedError

    def select_all(self):
        raise NotImplementedError

    def connect(self):
        import sqlite3
        conn = sqlite3.connect(self.db_name)
        return conn


# 5) Create Baseball DAO
class BaseballStatsDAO (AbstractDAO):

    def insert_records(self,records):
        conn = self.connect()
        c = conn.cursor()
        for record in records:
            baseball_record = (str(record[0]), record[1], record[2], record[3])
            c.execute("insert into baseball_stats VALUES (?, ?, ?, ?);", baseball_record)


        conn.commit()
        conn.close()

    def select_all(self):
        conn = self.connect()
        c = conn.cursor()
        deque = collections.deque([])

        c.execute('SELECT * FROM {tn}'.\
        format(tn='baseball_stats'))
        all_rows = c.fetchall()
        for row in all_rows:
            new = BaseballStatRecord(row[0], row[1], row[2], row[3])
            deque.append(new)

        conn.close()
        return deque

# 6) Create Stocks DAO
class StockStatsDAO (AbstractDAO):
    '''
    company_name 0 , ticker 1, exchange_country 2, price 3, exchange_rate 4, shares_outstanding 5, net_income 6, market_value_usd 7, pe_ratio 8
    '''
    def insert_records(self,records):
        conn = self.connect()
        c = conn.cursor()
        for record in records:
            stock_record = (record[0], record[1], record[2], record[3],record[4], record[5], record[6], record[7], record[8])
            c.execute("insert into stock_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", stock_record)


        conn.commit()
        conn.close()

    def select_all(self):
        conn = self.connect()
        c = conn.cursor()
        deque = collections.deque([])

        c.execute('SELECT * FROM {tn}'.\
        format(tn='stock_stats'))
        all_rows = c.fetchall()
        for row in all_rows:
            new = StockStatRecord(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8])
            deque.append(new)

        conn.close()
        return deque

if __name__ in '__main__':


    cwd = os.getcwd()
    filepath1 = os.path.join(cwd, 'MLB2008.csv')
    filepath2 = os.path.join(cwd, 'StockValuations.csv')

    baseball_db_path = os.path.join(cwd,'baseball.db')
    stock_db_path = os.path.join(cwd, 'stocks.db')

    # CHECK IS DB EXISTS
    if os.path.exists(baseball_db_path) == False:
        create_dbs.create_db('baseball.db')
    if os.path.exists(stock_db_path) == False:
        create_dbs.create_db('stocks.db')

    # CHECK IF DATA IN DB
    for db in [baseball_db_path,stock_db_path]:
        con = sqlite3.connect(db)
        cursor = con.cursor()
        if db == stock_db_path:
            cursor.execute("SELECT * FROM stock_stats")
        elif db == baseball_db_path:
            cursor.execute("SELECT * FROM baseball_stats")

        if len(cursor.fetchall()) == 0:

            # 7) Load MLB2008
            mlb = BaseballCSVReader(filepath1).load()
            # 8) Load Stock
            stock = StocksCSVReader(filepath2).load()

            # 9) Baseball DAO
            b_stats = BaseballStatsDAO(baseball_db_path)

            # 10) Stock DAO
            s_stats = StockStatsDAO(stock_db_path)

            # 11) Baseball insert to DB
            b_stats.insert_records(mlb)
            # 12) Stock insert to DB
            s_stats.insert_records(stock)

        else:
            b_stats = BaseballStatsDAO(baseball_db_path)
            s_stats = StockStatsDAO(stock_db_path)
    print("Stocks (Number of stock tickers by country)\n")
# 13) Calculate # of Tickers by Exchange Country
    s_select = s_stats.select_all()
    stock_list = []
    for comp in s_select:
        stock_dict = {'ticker': comp.name, 'exchange_country': comp.exchange_country}
        stock_list.append(stock_dict)

    country_count = Counter(dic['exchange_country'] for dic in stock_list)
    for k,v in country_count.most_common():
        print(str(k), ": " + str(v))

    print("")
    print("Baseball (Average salary by Batting Average):\n")
# 14) Calculate average salary by batting_average
    b_select = b_stats.select_all()
    baseball_list = []
    for player in b_select:
        baseball_dict = {'salary': player.salary, 'BA': player.ave}
        baseball_list.append(baseball_dict)

    sortlist = sorted(baseball_list, key=lambda k: k['BA'], reverse=True)

    newlist = []
    for i in range(0, len(sortlist)):
        if i == len(sortlist)-1:
            newlist.append(sortlist[i])
        else:
            sals = []
            if any(d['BA'] == sortlist[i]["BA"] for d in newlist):
                continue
            elif sortlist[i]["BA"] == sortlist[i+1]["BA"]:
                common_ba = sortlist[i]["BA"]
                if common_ba in newlist:
                    continue

                while sortlist[i]["BA"] == sortlist[i+1]["BA"]:
                    sals.append(sortlist[i]["salary"])
                    i+=1
                    if sortlist[i]["BA"] != sortlist[i+1]["BA"]:
                        sals.append(sortlist[i]["salary"])
                        break

                avg_sal = sum(sals)/ float(len(sals))
                newdict = {"salary": avg_sal, "BA": common_ba}
                newlist.append(newdict)

            else:
                newlist.append(sortlist[i])
    for i in newlist:
        print(i['BA'], ": ",i['salary'])

