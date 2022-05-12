#!/usr/bin/env python3

import os
import pandas as pd
import jaydebeapi
import datetime
from sys import argv

cur_time = (argv[1], argv[1])

path = '/home/demipt2/ojdbc8.jar'

conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:****:1521/deoracle',
['****', '****'],
path
)

conn.jconn.setAutoCommit(False)
curs = conn.cursor()

# Type 1
curs.execute("""
insert into demipt2.rozh_rep_fraud (event_dt, passport,fio, phone, event_type ,report_dt, tr_id)
select
    tr.transaction_date as event_dt,
    clients.passport_num as passport,
    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
    clients.phone,
    1 as event_type,
    to_date( ? , 'YYYY-MM-DD') as report_date,
    tr.transaction_id as tr_id
from demipt2.rozh_dwh_fact_transactions tr
left join demipt2.rozh_dwh_dim_cards_hist cards
    on tr.card_num = trim(cards.card_num) and cards.deleted_flg = 'N' and tr.transaction_date between cards.effective_from and cards.effective_to
left join demipt2.rozh_dwh_dim_accounts_hist acc
    on cards.account = acc.account and acc.deleted_flg = 'N' and tr.transaction_date between acc.effective_from and acc.effective_to
left join demipt2.rozh_dwh_dim_clients_hist clients
    on acc.client = clients.client_id and clients.deleted_flg = 'N' and tr.transaction_date between clients.effective_from and clients.effective_to
left join demipt2.rozh_dwh_fact_pssprt_blcklst blck_list
    on clients.passport_num = blck_list.passport
where 
(
coalesce(blck_list.entry_dt, to_date('9999-01-01', 'YYYY-MM-DD')) <= tr.transaction_date 
or
coalesce(clients.passport_valid_to, to_date('9999-01-01', 'YYYY-MM-DD')) <= tr.transaction_date
)
and 
tr.oper_result = 'SUCCESS' and to_char(tr.transaction_date, 'YYYY-MM-DD') = to_char(to_date(?,'YYYY-MM-DD') , 'YYYY-MM-DD')

""", cur_time)





#Type 2
curs.execute("""
insert into demipt2.rozh_rep_fraud (event_dt, passport,fio, phone, event_type ,report_dt, tr_id)
select
    tr.transaction_date as event_dt,
    clients.passport_num as passport,
    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
    clients.phone,
    2 as event_type,
    to_date( ? , 'YYYY-MM-DD') as report_date,
    tr.transaction_id as tr_id
from demipt2.rozh_dwh_fact_transactions tr
left join demipt2.rozh_dwh_dim_cards_hist cards
    on tr.card_num = trim(cards.card_num) and cards.deleted_flg = 'N' and tr.transaction_date between cards.effective_from and cards.effective_to
left join demipt2.rozh_dwh_dim_accounts_hist acc
    on cards.account = acc.account and acc.deleted_flg = 'N' and tr.transaction_date between acc.effective_from and acc.effective_to
left join demipt2.rozh_dwh_dim_clients_hist clients
    on acc.client = clients.client_id and clients.deleted_flg = 'N' and tr.transaction_date between clients.effective_from and clients.effective_to
where 
(
acc.valid_to < tr.transaction_date
)
and 
tr.oper_result = 'SUCCESS' and to_char(tr.transaction_date, 'YYYY-MM-DD') = to_char(to_date(?,'YYYY-MM-DD') , 'YYYY-MM-DD')

""", cur_time)



#Type 3
curs.execute("""
insert into demipt2.rozh_rep_fraud (event_dt, passport,fio, phone, event_type ,report_dt, tr_id)
with source as (
select
      tr.transaction_date as event_dt,
      tr.transaction_id as tr_id, 
      clients.passport_num as passport,
      terminals.terminal_city as city, 
      clients.phone as phone,
      clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio,
      lag(tr.transaction_date) over (partition by clients.passport_num order by clients.passport_num, tr.transaction_date) as test1,
      lag(terminals.terminal_city) over (partition by clients.passport_num order by clients.passport_num, tr.transaction_date) as test2
  from demipt2.rozh_dwh_fact_transactions tr
left join demipt2.rozh_dwh_dim_cards_hist cards
    on tr.card_num = trim(cards.card_num) and cards.deleted_flg = 'N' and tr.transaction_date between cards.effective_from and cards.effective_to
left join demipt2.rozh_dwh_dim_accounts_hist acc
    on cards.account = acc.account and acc.deleted_flg = 'N' and tr.transaction_date between acc.effective_from and acc.effective_to
left join demipt2.rozh_dwh_dim_clients_hist clients
    on acc.client = clients.client_id and clients.deleted_flg = 'N' and tr.transaction_date between clients.effective_from and clients.effective_to
left join demipt2.rozh_dwh_dim_terminals_hist terminals
    on tr.terminal = terminals.terminal_id and terminals.deleted_flg = 'N' and tr.transaction_date between terminals.effective_from and terminals.effective_to 
where 
tr.oper_result = 'SUCCESS' and to_char(tr.transaction_date, 'YYYY-MM-DD') = to_char(to_date(?,'YYYY-MM-DD') , 'YYYY-MM-DD')
order by  clients.passport_num, tr.transaction_date
)

select   
    event_dt,    
    passport,
    fio, 
    phone,
    3 as event_type,
    to_date(?, 'YYYY-MM-DD') as report_dt,
    tr_id
from source
where city <> test2 and event_dt between test1 - interval '1' hour and test1 + interval '1' hour 

""", cur_time)


#Type4
curs.execute("""
insert into demipt2.rozh_rep_fraud (event_dt, passport,fio, phone, event_type ,report_dt, tr_id)
select
     event_dt,
     passport,
     fio,
     phone,
     4 as event_type,
     to_date( ? , 'YYYY-MM-DD') as report_date,
     tr_id
from
(
select
     tr.transaction_date as timeee, tr.card_num, tr.oper_result,tr.amount,
     lead(case when tr.oper_result = 'SUCCESS' then 0 else 1 end , 1 , 0) over (partition by tr.card_num order by tr.transaction_date desc) as count_status1,
     (lead(case when tr.oper_result = 'SUCCESS' then 0 else 1 end , 1 , 0) over (partition by tr.card_num order by tr.transaction_date desc) +
     lead(case when tr.oper_result = 'SUCCESS' then 0 else 1 end , 2 , 0) over (partition by tr.card_num order by tr.transaction_date desc) +
     lead(case when tr.oper_result = 'SUCCESS' then 0 else 1 end , 3 , 0) over (partition by tr.card_num order by tr.transaction_date desc) +
     lead(case when tr.oper_result = 'SUCCESS' then 0 else 1 end , 4 , 0) over (partition by tr.card_num order by tr.transaction_date desc)) as count_status2,
     lead(tr.transaction_date,3,to_date('9999-01-01', 'YYYY-MM-DD')) over (partition by tr.card_num order by tr.transaction_date desc) + interval '20' minute as ch_time,
     lead(tr.oper_result, 2 , 0) over (partition by tr.card_num order by tr.transaction_date desc) as count_status3,
     lead(tr.amount , 1 , 0) over (partition by tr.card_num order by tr.transaction_date desc) as ch_am1,
     lead(tr.amount, 2 , 0) over (partition by tr.card_num order by tr.transaction_date desc) as ch_am2,
     lead(tr.amount , 3 , 0) over (partition by tr.card_num order by tr.transaction_date desc) as ch_am3,
    tr.transaction_date as event_dt,
    tr.transaction_id as tr_id, 
    clients.passport_num as passport,
    terminals.terminal_city as city, 
    clients.phone as phone,
    clients.last_name || ' ' || clients.first_name || ' ' || clients.patronymic as fio

from demipt2.rozh_dwh_fact_transactions tr
left join demipt2.rozh_dwh_dim_cards_hist cards
    on tr.card_num = trim(cards.card_num) and cards.deleted_flg = 'N' and tr.transaction_date between cards.effective_from and cards.effective_to
left join demipt2.rozh_dwh_dim_accounts_hist acc
    on cards.account = acc.account and acc.deleted_flg = 'N' and tr.transaction_date between acc.effective_from and acc.effective_to
left join demipt2.rozh_dwh_dim_clients_hist clients
    on acc.client = clients.client_id and clients.deleted_flg = 'N' and tr.transaction_date between clients.effective_from and clients.effective_to
left join demipt2.rozh_dwh_dim_terminals_hist terminals
    on tr.terminal = terminals.terminal_id and terminals.deleted_flg = 'N' and tr.transaction_date between terminals.effective_from and terminals.effective_to 

order by tr.card_num, tr.transaction_date desc
)

where 
(case when count_status1 = 0 or oper_result='REJECT' then 0 else (count_status2) end) >=3 
and timeee < ch_time 
and count_status3='REJECT' 
and amount < ch_am1 and ch_am1 < ch_am2  and ch_am2 < ch_am3
and to_char(event_dt, 'YYYY-MM-DD') = to_char(to_date(?,'YYYY-MM-DD') , 'YYYY-MM-DD')




""", cur_time)








conn.commit()
curs.close()
conn.close()
