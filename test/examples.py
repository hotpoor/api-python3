import os
import sys
import dolphindb as ddb
sys.path.append('..')
from dolphindb import *

HOST = "localhost"
PORT = 8848
s = ddb.session(HOST, PORT)
s = session()
s.connect(HOST,PORT,"admin","123456")
WORK_DIR = "C:/Tutorials_EN/data"

if s.existsDatabase(WORK_DIR+"/valdb"):
    s.dropDatabase(WORK_DIR+"/valdb")
s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="")
trade=s.loadTextEx(dbPath="db", partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.toDF())

print(s.run('wavg( [100, 60, 300], [1, 1.5, 2])'))

v1=s.run("v1=3 1 2 5 7; sort v1")
print(v1)

s.run("t1=table(1 2 3 as id, 4 5 6 as v)")
t=s.run("select * from t1")
print(t)

df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
s.upload({'t1': df})
print(s.run("t1.value.avg()"))


trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade")
t = s.run("select bid, ask, prc from trade where bid!=NULL, ask!=NULL, vol>1000")
print(t)
#1
print("test loadText")
trade = s.loadText("trade", WORK_DIR+"/example.csv")
print(trade.count())

# 2
print("test loadText parallel mode")
trade = s.loadText("trade", WORK_DIR+"/example.csv",parallel=True)
print(trade.count())

trade=s.loadTextEx(partitionType=VALUE,partitions=["GFGC","EWST", "EGAS"], partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.toDF())


# 3
print("test drop & create partitioned database; loadTextEx")
if s.existsDatabase(WORK_DIR+"/valdb"):
    s.dropDatabase(WORK_DIR+"/valdb")
s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="C:/Tutorials_EN/data/valdb")
trade = s.loadTextEx(dbPath=WORK_DIR+"/valdb", partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.count())

trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade")
t1 = s.table(data={'sym': ['EGAS', 'EGAS', 'EGAS'], 'date': ['2015.12.31', '2015.12.30', '2015.12.29'], 'open': [7.1, 7.5, 7.3]})
print("test merge")
print(trade.merge(t1,on=["sym","date"]).toDF())


# 4. loadTableBySQL
print("test loadTableBySQL")
trade = s.loadTableBySQL(dbPath=WORK_DIR + "/valdb", tableName="trade", sql = "select * from trade where date > 2010.01.01")
print(trade.count())

# 5 test top
print("test select")
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
t1=trade.select(['bid','ask','prc','vol']).where('bid!=NULL').where('ask!=NULL').where('vol>1000')
print(t1.count())
# print (trade.top(5).toDF())
#
#
# 6 update
print("test update")
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
trade = trade.update(["VOL"],["200"]).where(["date>1992.01.14"]).execute().where("VOL=200")
t1=trade.where("VOL=200")
print(t1.count())
print(trade.count())


# # 7 top
print("test top 10")
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
# print(trade.top(10).count())
#
# # 8 agg2
print(trade.groupby('sym').agg2('wavg',('prc','vol')).toDF())



#8 executeAs: assign a subset to a new table
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
top10 = trade.top(10).executeAs("top10")
print(top10.toDF())
#

# 'int' object is not callable
# # 9 count num of records
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
num = trade.count()
print(num)
#
#
# 10 append
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
c1 = trade.count()
print (c1)
top10 = trade.top(10).executeAs("top10")
c2 = trade.append(top10).count()
print (c2)
assert c1 + 10 == c2


# 11 delete
trade = s.table(dbPath=WORK_DIR+"/valdb", data="trade", inMem=True)
top10=trade.where('date<1992.01.15').delete().execute().top(10).executeAs("top10")
print (top10.toDF().min())

# 12 drop drop column
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
trade.drop(['ask', 'prc'])
print(trade.top(10).toDF())

#13 group by
print("group by")
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
print(trade.select('prc').groupby(['sym']).count().sort(bys=['sym desc']).toDF())
print(trade.select(['vol','prc']).groupby(['sym']).sum().toDF())

#14 group by having
print("group by having")
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
print(trade.select('count(ask)').groupby(['VOL']).having('count(ask)>15').toDF())


#15 ols
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
# print(trade.select(['bid','ask','prc']).ols('PRC', ['BID', 'ASK']))
z=trade.select(['bid','ask','prc']).where('bid!=NULL').where('ask!=NULL').ols('PRC', ['BID', 'ASK'])
print(z.keys())
print(z["ANOVA"])
print(z["RegressionStat"])
print(z["Coefficient"])
#16 columns, count, schema
trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
print(trade.columns)
print(trade.count())
print(trade.schema)

#17 select as vector

trade = s.table(dbPath=WORK_DIR + "/valdb", data="trade", inMem=True)
v1=trade.selectAsVector("prc")
print(v1)

#### DFS

# 1 create db

if s.existsDatabase("dfs://valdb"):
    s.dropDatabase("dfs://valdb")
s.database('db',partitionType=VALUE, partitions=["GFGC","EWST", "EGAS"], dbPath="dfs://valdb")
trade = s.loadTextEx(dbPath="dfs://valdb", partitionColumns=["sym"], tableName='trade', filePath=WORK_DIR + "/example.csv")
print(trade.count())




