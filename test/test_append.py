import dolphindb as ddb
from datetime import date,datetime, time
import csv
from collections import defaultdict
s = ddb.session()
s.connect("38.124.1.173",8924,"admin","123456")
csvFile = "C:/DolphinDB/db_testing/data/TAQ20070801_csv/TAQ20070801.csv/TAQ20070801.csv"
#`symbol`date`time`bid`ofr`bidsiz`ofrsiz`mode`ex`mmid
quotes = s.loadTable("quotes","dfs://TAQ_PYTHON")
with open(csvFile, encoding='utf-8') as fin:
    ct = 0
    z = defaultdict(list)
    for row in csv.DictReader(fin):
        z['SYMBOL'].append(row['SYMBOL'].strip())
        dt_str = row['DATE'].strip()
        if len(dt_str) == 8:
            dt = ddb.Date.from_date(date(int(dt_str[0:4]), int(dt_str[4:6]), int(dt_str[6:8])))
        else:
            dt = ddb.Date.null()
        z['DATE'].append(dt)
        sec_str = row['TIME'].strip()
        if len(sec_str)>=7:
            hour, min, second = map(int, sec_str.split(':'))
            tm = ddb.Second.from_time(time(hour,min,second))
        else:
            tm = ddb.Second.null()
        z['TIME'].append(tm)
        z['BID'].append(ddb.floatNan if row['BID']=="" else float(row['BID']))
        z['OFR'].append(ddb.floatNan if row['OFR']=="" else float(row['OFR']))
        z['BIDSIZ'].append(ddb.intNan if row['BIDSIZ'] == "" else int(row['BIDSIZ']))
        z['OFRSIZ'].append(ddb.intNan if row['OFRSIZ'] == "" else int(row['OFRSIZ']))
        z['MODE'].append(ddb.intNan if row['MODE'] == "" else int(row['MODE']))
        z['EX'].append(ddb.byteNan if row['MODE'] == "" else int(row['MODE']))
        z['MMID'].append(row['MMID'].strip())
        ct += 1
        if ct % 100 == 0:
            quotes.append(s.table(data=z, tableAliasName="t1").select("*"))
            print(ct)
            z = defaultdict(list)
            #quotes.append(s.table(data=z))


