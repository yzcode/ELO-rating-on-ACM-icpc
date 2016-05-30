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
    logging("transform start!", 0)
    mysqlconn_Local = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="fishteam_onlinejudge",
        charset="utf8"
    )
    sql = "select * from  where "
    sql = "UPDATE fishteam_problems SET extra_data = '%s', rating = %d WHERE repo = 'Pku' and label = %d"
    problem_file = open("output.txt", "r")
    for i in range(1000, 4055):
        pinfo = problem_file.readline().replace('\n', '').split(" ")
        related_problem = problem_file.readline().replace('\n', '').split(" ")
        pid, prating = int(pinfo[0]), float(pinfo[1])
        logging("transforming " + str(pid), 0)
        extra_data = {
            'rating': prating,
            'related_problem': map(int, filter(lambda x: len(x) > 0, related_problem))
        }
        # print json.dumps(extra_data)
        mysqlcur = mysqlconn_Local.cursor()
        mysqlcur.execute(sql % (json.dumps(extra_data), prating, pid))
    mysqlconn_Local.commit()
    mysqlconn_Local.close()
    logging("transform finished!", 0)
