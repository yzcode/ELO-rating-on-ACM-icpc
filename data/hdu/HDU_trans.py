# -*- coding: utf-8 -*-

import MySQLdb
import sqlite3
if __name__ == '__main__':
    print "transform start!"
    sqlitecon = sqlite3.connect("./HDU_Status.db")
    sqlitecur = sqlitecon.cursor()
    mysqlconn = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="OJ_data",
        charset="utf8"
    )
    sqlitecur.execute("select count(*) from HDU_Status")
    sum = int(sqlitecur.fetchall()[0][0])
    print "there are %d records" % (sum)
    selsql = "select * from HDU_Status limit %d,%d"
    sql = "REPLACE INTO hdu_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysqlcur = mysqlconn.cursor()
    # sum = 5000
    step = 50000
    for i in range(0, sum / step + 10):
        startptr = i * step
        print startptr
        sqlitecur.execute(selsql % (startptr, step))
        for items in sqlitecur.fetchall():
            param = items
            mysqlcur.execute(sql, param)
        mysqlconn.commit()
    mysqlconn.close()
    sqlitecon.close()
