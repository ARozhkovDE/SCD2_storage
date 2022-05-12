#!/usr/bin/env python3

import os
import pandas as pd
import jaydebeapi
import datetime
from sys import argv

cur_time = (argv[1],)

path = '/home/demipt2/ojdbc8.jar'

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:****:1521/deoracle',
['****', '****'],
path
)

conn.jconn.setAutoCommit(False)
curs = conn.cursor()


# 01. Del stages

curs.execute("delete from demipt2.rozh_stg_clients")

curs.execute("""
delete from demipt2.rozh_stg_del_tables
where table_name = 'CLIENTS'
""")

# 02. Fill stages 
curs.execute("""
insert into demipt2.rozh_stg_clients (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt)
select client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, update_dt from bank.clients
where coalesce(update_dt, create_dt) >= ( 
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') ) 
    from demipt2.rozh_meta where table_db = 'DEMIPT2' and table_name = 'CLIENTS' )
""")

 

#  03. Fill del stage
curs.execute("""
insert into demipt2.rozh_stg_del_tables ( id , table_name) select client_id, 'CLIENTS' from bank.clients
""")

# 04. First insert / Merge target tables
curs.execute("""
merge into demipt2.rozh_dwh_dim_clients_hist tgt
using (
    select
	s.client_id, 
        s.last_name,
        s.first_name,
        s.patronymic,
        s.date_of_birth,
        s.passport_num,
        s.passport_valid_to,
        s.phone,
        s.create_dt,
        s.update_dt
    from demipt2.rozh_stg_clients s
    left join demipt2.rozh_dwh_dim_clients_hist t
    on s.client_id = t.client_id and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where 
        t.client_id is not null and ( 1=0
          or s.last_name <> t.last_name or ( s.last_name is null and t.last_name is not null ) or ( s.last_name is not null and t.last_name is null )
          or s.first_name <> t.first_name or ( s.first_name is null and t.first_name is not null ) or ( s.first_name is not null and t.first_name is null )
          or s.patronymic <> t.patronymic or ( s.patronymic is null and t.patronymic is not null ) or ( s.patronymic is not null and t.patronymic is null )
          or s.date_of_birth <> t.date_of_birth or ( s.date_of_birth is null and t.date_of_birth is not null ) or ( s.date_of_birth is not null and t.date_of_birth is null )
          or s.passport_num <> t.passport_num or ( s.passport_num is null and t.passport_num is not null ) or ( s.passport_num is not null and t.passport_num is null )
          or s.passport_valid_to <> t.passport_valid_to or ( s.passport_valid_to is null and t.passport_valid_to is not null ) or ( s.passport_valid_to is not null and t.passport_valid_to is null )
          or s.phone <> t.phone or ( s.phone is null and t.phone is not null ) or ( s.phone is not null and t.phone is null )
          or s.create_dt <> t.create_dt or ( s.create_dt is null and t.create_dt is not null ) or ( s.create_dt is not null and t.create_dt is null )

        )
) stg
on ( tgt.client_id = stg.client_id )
when matched then update set effective_to = stg.update_dt - interval '1' second where tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
""")

curs.execute("""
insert into demipt2.rozh_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, effective_from, effective_to, deleted_flg )
select 
    client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
    coalesce(update_dt, create_dt) as effective_from, 
    to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to, 
    'N' as deleted_flg
from demipt2.rozh_stg_clients
""")


# 05. Del flag
curs.execute("""
insert into demipt2.rozh_dwh_dim_clients_hist (client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, create_dt, effective_from, effective_to, deleted_flg )
select 
   client_id,
    last_name,
    first_name,
    patronymic,
    date_of_birth,
    passport_num,
    passport_valid_to,
    phone,
    create_dt,
  to_date( ?, 'YYYY-MM-DD') as effective_from, 
  to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to, 
  'Y' as deleted_flg
from 
(
       select
        t.client_id,
        t.last_name,
        t.first_name,
        t.patronymic,
        t.date_of_birth,
        t.passport_num,
        t.passport_valid_to,
        t.phone,
        t.create_dt
    from demipt2.rozh_dwh_dim_clients_hist t
    left join demipt2.rozh_stg_del_tables s
    on t.client_id = s.id and s.table_name = 'CLIENTS'
  where s.id is null and t.effective_to = to_date('9999/12/31', 'yyyy/mm/dd') and deleted_flg = 'N'
)
""", cur_time)

curs.execute("""
update demipt2.rozh_dwh_dim_clients_hist tgt
set tgt.effective_to = to_date( ?, 'YYYY-MM-DD') - interval '1' second
where tgt.client_id in (
    select
        t.client_id
    from demipt2.rozh_dwh_dim_clients_hist t
    left join demipt2.rozh_stg_del_tables s
    on t.client_id = s.id and s.table_name = 'CLIENTS'
  where s.id is null and t.effective_to = to_date('9999/12/31', 'yyyy/mm/dd') and deleted_flg = 'N'
)
and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) 
and tgt.deleted_flg = 'N'
""", cur_time)


# 06. Meta
curs.execute("""
update demipt2.rozh_meta
set last_update_dt = to_date( ?, 'YYYY-MM-DD')
where table_db = 'DEMIPT2' and table_name = 'CLIENTS'
""", cur_time)



conn.commit()
curs.close()
conn.close()
