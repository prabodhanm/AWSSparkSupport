from __future__ import print_function
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext

# import pandas as pad


conf = (SparkConf()
        .setAppName("ParseDat")
        .setMaster("local")
        )

sc = SparkContext(conf=conf)

sqlcontext = SQLContext(sc)


'''
Converting a main file into delimited form
'''

ddf = sqlcontext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load(
    'D:/Projects/AARP/Schema/comm_schema.csv')
ddf.show()

df = sqlcontext.read.text("D:/Projects/AARP/Schema/dmrecomm.dat17123")

df.show()

datardd = ddf.rdd.map(lambda xx: (xx[0], xx[1], xx[2]))


datardd1 = datardd.collect()


def parsemainrows(dfrdd1, l):
    m = ""
    for x in dfrdd1:
        m = m + l[0][x[1]:x[2]] + ","
    m = m[:-1]
    return m


def createDF(rdd1):
    m = ""
    for x in rdd1:
        m = m + x[0] + ","
    m = m[:-1]
    # print (m)
    return m


d1 = createDF(datardd1)

d1 = d1.split(',')

d2 = df.rdd.map(lambda l: parsemainrows(datardd1, l))

commDF = d2.map(lambda x: str(x)).map(lambda w: w.split(',')).toDF(d1)

print("Show final df")
commDF.show()

'''
Converting subject file into delimited form
'''

ddf = sqlcontext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load(
    'D:/Projects/AARP/Schema/subject_schema.csv')
ddf.show()

dfsubject = sqlcontext.read.text("D:/Projects/AARP/Schema/dmsbject.dat17123")

datardd = ddf.rdd.map(lambda xx: (xx[0], xx[1], xx[2]))

datardd1 = datardd.collect()

d1 = createDF(datardd1)

d1 = d1.split(',')

d2 = dfsubject.rdd.map(lambda l: parsemainrows(datardd1, l))

subDF = d2.map(lambda x: str(x)).map(lambda w: w.split(',')).toDF(d1)

print("Show final df")
subDF.show()

'''
Converting topic file into delimited form
'''

ddf = sqlcontext.read.format('com.databricks.spark.csv').options(header='false', inferschema='true').load(
    'D:/Projects/AARP/Schema/topic_schema.csv')
ddf.show()

dftopic = sqlcontext.read.text("D:/Projects/AARP/Schema/dmtopic.dat17123")

datardd = ddf.rdd.map(lambda xx: (xx[0], xx[1], xx[2]))

datardd1 = datardd.collect()

d1 = createDF(datardd1)

d1 = d1.split(',')

d2 = dftopic.rdd.map(lambda l: parsemainrows(datardd1, l))

topicDF = d2.map(lambda x: str(x)).map(lambda w: w.split(',')).toDF(d1)

print("Show final df")
topicDF.show()

sqlcontext.registerDataFrameAsTable(commDF, "dfMainTable")
sqlcontext.registerDataFrameAsTable(subDF, "dfSubjectTable")
sqlcontext.registerDataFrameAsTable(topicDF, "dfTopicTable")

# sqlquery = "select tbl1.acct_nbr, tbl1.person_id,tbl1.receive_dt,tbl1.comm_method,"
# sqlquery = sqlquery + "tbl1.subject_cd,tbl2.subject_desc,tbl1.topic1,tbl3.topic_desc,tbl1.topic2,tbl1.topic3, "
# sqlquery = sqlquery + "tbl1.comm_id,tbl1.language_desc from dfMainTable as tbl1 left outer join dfSubjectTable as tbl2 on "
# sqlquery = sqlquery + "tbl1.subject_cd=tbl2.subject_cd left outer join dfTopicTable as tbl3 on "
# sqlquery = sqlquery + "tbl1.topic1=tbl3.topic_cd"

sqlquery = "select tbl1.acct_nbr, tbl1.person_id,tbl1.receive_dt,tbl1.comm_method,"
sqlquery = sqlquery + "tbl1.subject_cd,tbl2.subject_desc,tbl1.topic1,tbl1.topic2,tbl1.topic3, "
sqlquery = sqlquery + "tbl1.comm_id,tbl1.language_desc from dfMainTable as tbl1 left outer join dfSubjectTable as tbl2 on "
sqlquery = sqlquery + "tbl1.subject_cd=tbl2.subject_cd"



sqljoin = sqlcontext.sql(sqlquery)
sqljoin.show()

sqljoin.write.parquet("D:/Projects/AARP/Schema/data.parquet")

'''
Read a parquet file
'''

newDataDF = sqlcontext.read.parquet("D:/Projects/AARP/Schema/data.parquet")

newDataDF.show()


