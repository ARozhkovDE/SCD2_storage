#!/usr/bin/env python3

import os
import shutil
import jaydebeapi


all_path = '/home/demipt2/rozh/all/'
perday_path = '/home/demipt2/rozh/perday/'
archive_path = '/home/demipt2/rozh/archive/'
warning_path = '/home/demipt2/rozh/warning/'
zero_path = '/home/demipt2/rozh/zero/'
log_path = '/home/demipt2/rozh/log.txt'

# Clear all files and logs

for f_name in os.listdir(all_path):
    os.remove(all_path + f_name)

for f_name in os.listdir(perday_path):
    os.remove(perday_path + f_name)

for f_name in os.listdir(archive_path):
    os.remove(archive_path + f_name)

for f_name in os.listdir(warning_path):
    os.remove(warning_path + f_name)

for f_name in os.listdir(zero_path):
    shutil.copyfile(zero_path + f_name, all_path + f_name)

file = open(log_path, 'w').close()

# Clear oracle tables

path = '/home/demipt2/ojdbc8.jar'

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:****:1521/deoracle',
['****', '****'],
path
)

conn.jconn.setAutoCommit(False)
curs = conn.cursor()

# Drop section
#    Dwh_fact
curs.execute("""drop table demipt2.rozh_dwh_fact_transactions purge""")
curs.execute("""drop table demipt2.rozh_dwh_fact_pssprt_blcklst purge""")

#    Dwh_dim_hist
curs.execute("""drop table demipt2.rozh_dwh_dim_terminals_hist purge""")
curs.execute("""drop table demipt2.rozh_dwh_dim_accounts_hist purge""")
curs.execute("""drop table demipt2.rozh_dwh_dim_cards_hist purge""")
curs.execute("""drop table demipt2.rozh_dwh_dim_clients_hist purge""")


#    Stg
curs.execute("""drop table demipt2.rozh_stg_terminals purge""")
curs.execute("""drop table demipt2.rozh_stg_accounts purge""")
curs.execute("""drop table demipt2.rozh_stg_cards purge""")
curs.execute("""drop table demipt2.rozh_stg_clients purge""")

#    Stg del
curs.execute("""drop table demipt2.rozh_stg_del_tables purge""")

#   Report
curs.execute("""drop table demipt2.rozh_rep_fraud purge""")

#    Meta
curs.execute("""drop table demipt2.rozh_meta purge""")



# Create dwh_fact
curs.execute("""
create table demipt2.rozh_dwh_fact_transactions (
    transaction_id varchar(30) primary key,
    transaction_date date,
    amount decimal(10,2),
    card_num varchar(30),
    oper_type varchar(30),
    oper_result varchar(30),
    terminal varchar(30)
)
""")

curs.execute("""
create table demipt2.rozh_dwh_fact_pssprt_blcklst(
    entry_dt date,
    passport varchar(50)
)
""")

# create history tables
curs.execute("""
create table demipt2.rozh_dwh_dim_terminals_hist(
    terminal_id varchar(20),
    terminal_type varchar(10),
    terminal_city varchar(50),
    terminal_address varchar(200),
    effective_from date,
    effective_to date,
    deleted_flg char(1)
)
""")

curs.execute("""
create table demipt2.rozh_dwh_dim_accounts_hist(
    account char(20 byte),
    valid_to date,
    client varchar2(20 byte),
    create_dt date,
    effective_from date,
    effective_to date,
    deleted_flg char(1)
)
""")

curs.execute("""
create table demipt2.rozh_dwh_dim_cards_hist(
    card_num char(20),
    account char(20),
    create_dt date, 
    effective_from date,
    effective_to date,
    deleted_flg char(1)
)
""")

curs.execute("""
create table demipt2.rozh_dwh_dim_clients_hist(
    client_id	varchar2(20),
    last_name	varchar2(100),
    first_name	varchar2(100),
    patronymic	varchar2(100),
    date_of_birth	date,
    passport_num	varchar2(15),
    passport_valid_to	date,
    phone	varchar2(20),
    create_dt	date,
    effective_from date,
    effective_to date,
    deleted_flg char(1)
)
""")



# Create stages

curs.execute("""
create table demipt2.rozh_stg_terminals(
    terminal_id varchar(20),
    terminal_type varchar2(10),
    terminal_city varchar2(50),
    terminal_address varchar2(200),
    update_dt date
)
""")

curs.execute("""create table demipt2.rozh_stg_accounts as (select account, valid_to, CLIENT, create_dt, update_dt from bank.accounts where 1=2)""")
curs.execute("""create table demipt2.rozh_stg_cards as (select card_num, account, create_dt, update_dt from bank.cards where 1=2)""")
curs.execute("""create table demipt2.rozh_stg_clients as (select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt from bank.clients where 1=2)""")




# Create del stages
curs.execute("""
create table demipt2.rozh_stg_del_tables(
    id varchar(30),
    table_name varchar(25)
)
""")

# Create report fraud
curs.execute("""
create table demipt2.rozh_rep_fraud(
    event_dt date,
    passport varchar(25),
    fio varchar2(90),
    phone varchar(25),
    event_type int ,
    report_dt date,
    tr_id varchar(30)
)
""")


# Create & fill meta
curs.execute("""
create table demipt2.rozh_meta(
    table_db varchar2(30),
    table_name varchar2(30),
    last_update_dt date
)
""")

curs.execute("""insert into demipt2.rozh_meta(table_db, table_name, last_update_dt) values ( 'DEMIPT2', 'ACCOUNTS',  to_date( '1900-01-01', 'YYYY-MM-DD'))""")
curs.execute("""insert into demipt2.rozh_meta(table_db, table_name, last_update_dt) values ( 'DEMIPT2', 'CARDS',  to_date( '1900-01-01', 'YYYY-MM-DD') )""")
curs.execute("""insert into demipt2.rozh_meta(table_db, table_name, last_update_dt) values ( 'DEMIPT2', 'CLIENTS',  to_date( '1900-01-01', 'YYYY-MM-DD'))""")
curs.execute("""insert into demipt2.rozh_meta(table_db, table_name, last_update_dt) values ( 'DEMIPT2', 'TERMINALS',  to_date( '1900-01-01', 'YYYY-MM-DD') )""")





conn.commit()
curs.close()
conn.close()




print('Done')

