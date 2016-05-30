# -*- coding: utf-8 -*-

import MySQLdb
import json
import time


def logging(msg, lv):
    ISOTIMEFORMAT = "%Y-%m-%d %X"
    logtime = time.strftime(ISOTIMEFORMAT, time.localtime())
    lvstr = ["MASSAGE", "WARNING", "ERROR  "]
    print lvstr[lv], logtime, ":", msg


if __name__ == '__main__':
    logging("POJ problem infomation transform start!", 0)
    mysqlconn_Local = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="OJ_data",
        charset="utf8"
    )
    sql = "REPLACE INTO problem_info(repo, label,rating,vec) VALUES ('Pku', '%s', '%s','{}')"
    rating_file = open("result_poj.txt", "r")
    vec_file = open("vec.txt", "r")
    count = int(rating_file.readline())
    vec_count = int(vec_file.readline().replace('\n', '').split(' ')[0])
    mysqlcur = mysqlconn_Local.cursor()
    for i in range(1000, 4055):
        if i % 100 == 0:
            logging("Handling the no.%d problem's rating" % i, 0)
        pro_info = rating_file.readline().split('\t')
        pid, prating = int(pro_info[0]), float(pro_info[1])
        # print pid, prating
        mysqlcur.execute(sql % (pid, prating))
    mysqlconn_Local.commit()
    sql = "UPDATE problem_info SET vec = '%s' WHERE repo = 'Pku' and label = %d"
    for i in range(0, vec_count):
        if i % 100 == 0:
            logging("Handling the no.%d problem's vector" % i, 0)
        pro_info = filter(lambda x: len(x) > 1, vec_file.readline().split(' '))
        pid = int(pro_info[0])
        pvec = json.dumps(pro_info[1:])
        mysqlcur.execute(sql % (pvec, pid))
    mysqlconn_Local.commit()
    mysqlconn_Local.close()
    logging("POJ problem infomation transform finished!", 0)
