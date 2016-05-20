# -*- coding: utf-8 -*-

import MySQLdb
import sqlite3
if __name__ == '__main__':
    print "transform start!"
    sqlitecon = sqlite3.connect("./POJ_status.db")
    sqlitecur = sqlitecon.cursor()
    mysqlconn = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="OJ_data",
        charset="utf8"
    )
    sqlitecur.execute("select count(*) from POJ_Status")
    sum = int(sqlitecur.fetchall()[0][0])
    print "there are %d records" % (sum)
    selsql = "select * from POJ_Status limit %d,%d"
    sql = "REPLACE INTO poj_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysqlcur = mysqlconn.cursor()
    # sum = 5000
    step = 50000
    for i in range(15239716 / step, sum / step + 10):
        startptr = i * step
        print startptr
        sqlitecur.execute(selsql % (startptr, step))
        for items in sqlitecur.fetchall():
            param = items
            mysqlcur.execute(sql, param)
        mysqlconn.commit()
    mysqlconn.close()
    sqlitecon.close()
