#!/usr/bin/env python3

import os
import pandas as pd
import jaydebeapi
import datetime

path = '/home/demipt2/ojdbc8.jar'

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:****:1521/deoracle',
['****', '****'],
path
)

conn.jconn.setAutoCommit(False)
curs = conn.cursor()

curs.execute ("delete from demipt2.rozh_dwh_fact_pssprt_blcklst")
curs.execute ("delete from demipt2.rozh_stg_terminals")


perday_path = '/home/demipt2/rozh/perday/'
archive_path = '/home/demipt2/rozh/archive/'
warning_path = '/home/demipt2/rozh/warning/'
log_path = '/home/demipt2/rozh/log.txt'

def load_to_oracle(df, sql_com, f_name):
    try:
        curs.executemany(sql_com, df.values.tolist())
        os.rename(perday_path + f_name, archive_path + f_name + '.backup')
    except Exception as err:
        f = open(log_path, 'a')
        f.write('### \n' + str(datetime.datetime.now()) + '\n' + sql_com + '\n' + str(err) + '\n')
        f.close()
        os.rename(perday_path + f_name, warning_path + f_name)


def passport_blacklist(f_name):
    df = pd.read_excel(perday_path + f_name)
    df['date'] = df['date'].astype(str)
    sql_com = "insert into demipt2.rozh_dwh_fact_pssprt_blcklst (entry_dt, passport) values(to_date(?, 'YYYY-MM-DD'),?)"
    load_to_oracle(df, sql_com, f_name)


def transactions(f_name):
    df = pd.read_csv(perday_path + f_name, delimiter=';')
    df['amount'] = df['amount'].str.replace(',', '.')
    df['transaction_date'] = df['transaction_date'].astype(str)
    sql_com = "insert into demipt2.rozh_dwh_fact_transactions (transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal) values(?,to_date(?, 'YYYY-MM-DD HH24:MI:SS'),cast(? as decimal(10,2)),?,?,?,?)"
    load_to_oracle(df, sql_com, x)


def terminals(f_name):
    df = pd.read_excel(perday_path+f_name)
    up_dt = str(x[x.rfind('_') + 1:]).replace('.xlsx', '')
    df.insert(4, 'update_dt', up_dt)
    sql_com = "insert into demipt2.rozh_stg_terminals (terminal_id, terminal_type, terminal_city, terminal_address, update_dt) values(?,?,?,?,to_date(?, 'DDMMYYYY'))"
    load_to_oracle(df, sql_com, f_name)




for f_name in os.listdir(perday_path):
    try:
        locals()[f_name[:f_name.rfind('_')]](f_name)
    except Exception as inst:
        print(inst)


conn.commit()
curs.close()
conn.close()
