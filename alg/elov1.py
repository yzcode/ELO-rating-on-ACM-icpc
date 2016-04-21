# -*- coding: utf-8 -*-
import MySQLdb
import time


class user():
    """docstring for user"""
    def __init__(self):
        self.ac = []
        self.rating = 1500
        self.cnt = 0


class status():
    """docstring for status"""
    def __init__(self, item):
        self.run_id = item[0]
        self.username = item[1]
        self.problem_id = item[2]
        self.result = item[3]
        self.memory = item[4]
        self.time = item[5]
        self.language = item[6]
        self.code_len = item[7]
        self.time = item[8]


DATA_LEN = 0
PROBLEM_RATING = [1500 for i in range(0, 5000)]
USR = {}


def logging(msg, lv):
    ISOTIMEFORMAT = "%Y-%m-%d %X"
    logtime = time.strftime(ISOTIMEFORMAT, time.localtime())
    lvstr = ["MASSAGE", "WARNING", "ERROR  "]
    print lvstr[lv], logtime, ":", msg
    pass


def fetch_data(mysqlcur, start, limit):
    sql = "select * from poj_data limit %d,%d" % (start, limit)
    logging("Fetching Data from %d to %d Start..." % (start, start + limit), 0)
    mysqlcur.execute(sql)
    logging("Fetching Data from %d to %d Finish" % (start, start + limit), 0)
    return mysqlcur.fetchall()


def init_modal():
    pass


def status_filter(data):
    logging("Data Cleaning Start", 0)
    data_arr = []
    for item in data:
        sta = status(item)
        if sta.result != "Compile Error":
            if not USR.get(sta.username):
                USR[sta.username] = user()
                data_arr.append(sta)
            else:
                if sta.problem_id not in USR[sta.username].ac:
                    USR[sta.username].ac.append(sta.problem_id)
                    data_arr.append(sta)
    logging("Data Cleaning Finish", 0)
    return data_arr


def cal_elo(ra, rb, res):
    EA = 1 / (1 + 10 ** ((rb - ra) / 400.0))
    EB = 1 / (1 + 10 ** ((ra - rb) / 400.0))
    KA = KB = SA = SB = 0
    if ra > 2400:
        KA = 10
    elif ra > 1800:
        KA = 15
    else:
        KA = 30
    if rb > 2400 or rb < 600:
        KB = 10
    elif rb > 1900 or rb < 1100:
        KB = 15
    else:
        KB = 30
    if res:
        SA = 1
        SB = 0
        factor = 1
    else:
        SA = 0
        SB = 1
        factor = 0.35
    RA = ra + KA * (SA - EA) * factor
    RB = rb + KB * (SB - EB)
    return RA, RB


def elo_pro(mysqlcur):
    init_modal()
    # DATA_LEN = 1
    step = 500000
    for i in range(0, DATA_LEN / step + 1):
        start_ptr = i * step
        datas = fetch_data(mysqlcur, start_ptr, step)[:]
        datas = status_filter(datas)
        logging("Computing the Problem Rating...", 0)
        for sta in datas:
            USR[sta.username].rating, PROBLEM_RATING[sta.problem_id] =\
                cal_elo(
                    USR[sta.username].rating,
                    PROBLEM_RATING[sta.problem_id],
                    True if sta.result == "Accepted" else False
                )
        logging("Computing the Problem Rating Finish", 0)
    # print PROBLEM_RATING
    pass


def print_to_file():
    fp = open("result_poj.txt", "w")
    for i in range(1000, 4055):
        fp.write("%d\t%f\n" % (i, PROBLEM_RATING[i]))
    fp.close()
    pass


if __name__ == '__main__':
    logging("ELO Rating System Start", 0)
    logging("Connecting to the MySQL Server...", 0)
    mysqlconn = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="OJ_data",
        charset="utf8"
    )
    logging("MySQL Server Connected", 0)
    mysqlcur = mysqlconn.cursor()
    logging("Getting the Amount of Data...", 0)
    mysqlcur.execute("select count(RunID) from poj_data")
    DATA_LEN = mysqlcur.fetchall()[0][0]
    logging("There are %d records in the DataBase" % DATA_LEN, 0)
    # print len(fetch_data(mysqlcur, 0, 100))
    elo_pro(mysqlcur)
    print_to_file()
    mysqlconn.close()
