#!/usr/bin/env python3

import os
import datetime

all_path = '/home/demipt2/rozh/all/'
perday_path = '/home/demipt2/rozh/perday/'
archive_path = '/home/demipt2/rozh/archive/'
warning_path = '/home/demipt2/rozh/warning/'
scripts_path = '/home/demipt2/rozh/py_scripts/'
sql_path = '/home/demipt2/rozh/sql_scripts/'

if len((os.listdir(perday_path))) == 0:
    source = {}
    for f_name in os.listdir(all_path):
        if (f_name + '.backup') in os.listdir(archive_path):
            os.rename(all_path + f_name, warning_path + f_name)
            continue
        l_str = f_name[:f_name.rfind('_')]
        r_str = f_name[f_name.rfind('_') + 1:]
        if source.get(l_str) is None:
            source.setdefault(l_str, [r_str])
        else:
            source.get(l_str).append(r_str)
    for key in source.keys():
        d = datetime.date(9999, 1, 1)
        for value in source[key]:
            date_f = min((datetime.datetime.strptime(value[:value.rfind('.')], '%d%m%Y').date()), d)
            if date_f < d:
                d = date_f
                format_f = value[value.rfind('.'):]
        name_f = key + '_' + datetime.datetime.strftime(date_f, '%d%m%Y') + format_f
        os.rename(all_path + name_f, perday_path + name_f)
        tr_time = date_f

cur_time = datetime.datetime.now().date()

if len((os.listdir(perday_path))) > 0:
    os.system('python ' + scripts_path + 'conn.py')


os.system('python ' + sql_path + '02*' + ' ' + cur_time.strftime("%Y-%m-%d"))
os.system('python ' + sql_path + '03*' + ' ' + cur_time.strftime("%Y-%m-%d"))
os.system('python ' + sql_path + '04*' + ' ' + cur_time.strftime("%Y-%m-%d"))
os.system('python ' + sql_path + '05*' + ' ' + tr_time.strftime("%Y-%m-%d"))


