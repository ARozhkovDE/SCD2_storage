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
curs.execute("delete from demipt2.rozh_stg_accounts")

curs.execute("""
delete from demipt2.rozh_stg_del_tables
where table_name = 'ACCOUNTS'
""")

# 02. Fill stages 
curs.execute("""
insert into demipt2.rozh_stg_accounts ( account, valid_to, CLIENT, create_dt, update_dt )
select account, valid_to, CLIENT, create_dt, update_dt from bank.accounts
where coalesce(update_dt, create_dt) >= ( 
    select coalesce( last_update_dt, to_date( '1900-01-01', 'YYYY-MM-DD') ) 
    from demipt2.rozh_meta where table_db = 'DEMIPT2' and table_name = 'ACCOUNTS' )
""")



# 03. Fill del stage
curs.execute("""
insert into demipt2.rozh_stg_del_tables ( id , table_name) select account, 'ACCOUNTS' from bank.accounts
""")

# 04. First insert / Merge target tables
curs.execute("""
merge into demipt2.rozh_dwh_dim_accounts_hist tgt
using (
    select 
        s.account,
        s.valid_to,
        s.client,
        s.create_dt,
        s.update_dt
    from demipt2.rozh_stg_accounts s
    left join demipt2.rozh_dwh_dim_accounts_hist t
    on s.account = t.account and t.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) and deleted_flg = 'N'
    where 
        t.account is not null and ( 1=0
          or s.valid_to <> t.valid_to or ( s.valid_to is null and t.valid_to is not null ) or ( s.valid_to is not null and t.valid_to is null )
          or s.client <> t.client or ( s.client is null and t.client is not null ) or ( s.client is not null and t.client is null )
          or s.create_dt <> t.create_dt or ( s.create_dt is null and t.create_dt is not null ) or ( s.create_dt is not null and t.create_dt is null )

        )
) stg
on ( tgt.account = stg.account )
when matched then update set effective_to = stg.update_dt - interval '1' second where tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' )
""")

curs.execute("""
insert into demipt2.rozh_dwh_dim_accounts_hist ( account, valid_to, client, create_dt, effective_from, effective_to, deleted_flg )
select 
    account,
    valid_to,
    client,
    create_dt,
    coalesce(update_dt, create_dt) as effective_from, 
    to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to, 
    'N' as deleted_flg
from demipt2.rozh_stg_accounts
""")


# 05. Del flag
curs.execute("""
insert into demipt2.rozh_dwh_dim_accounts_hist ( account, valid_to, client, create_dt, effective_from, effective_to, deleted_flg )
select 
    account,
    valid_to,
    client,
    create_dt,
    to_date( ? ,'YYYY-MM-DD')  as effective_from, 
    to_date( '9999-12-31', 'YYYY-MM-DD' ) as effective_to, 
    'Y' as deleted_flg
from 
(
       select
            t.account,
            t.valid_to,
            t.client,
            t.create_dt
    from demipt2.rozh_dwh_dim_accounts_hist t
    left join demipt2.rozh_stg_del_tables s
    on t.account = s.id and s.table_name = 'ACCOUNTS'
  where s.id is null and t.effective_to = to_date('9999/12/31', 'yyyy/mm/dd') and deleted_flg = 'N'
)
""", cur_time)

curs.execute("""
update demipt2.rozh_dwh_dim_accounts_hist tgt
set tgt.effective_to =  to_date( ? , 'YYYY-MM-DD')  - interval '1' second
where tgt.account in (
    select
        t.account
    from demipt2.rozh_dwh_dim_accounts_hist t
    left join demipt2.rozh_stg_del_tables s
    on t.account = s.id and s.table_name = 'ACCOUNTS'
  where s.id is null and t.effective_to = to_date('9999/12/31', 'yyyy/mm/dd') and deleted_flg = 'N'
)
and tgt.effective_to = to_date( '9999-12-31', 'YYYY-MM-DD' ) 
and tgt.deleted_flg = 'N'
""", cur_time)


# 06. Meta
curs.execute("""
update demipt2.rozh_meta
set last_update_dt = to_date( ? , 'YYYY-MM-DD' ) 
where table_db = 'DEMIPT2' and table_name = 'ACCOUNTS'
""", cur_time)





conn.commit()
curs.close()
conn.close()
