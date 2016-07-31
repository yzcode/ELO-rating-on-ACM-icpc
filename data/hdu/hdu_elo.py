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


REPO = 'hdu'
CE_STR = 'Compilation Error'
DATA_LEN = 0
MAX_RED = 0
MIN_RED = 100000
PROBLEM_RATING = [1500 for i in range(0, 10000)]
USR = {}


def logging(msg, lv):
    ISOTIMEFORMAT = "%Y-%m-%d %X"
    logtime = time.strftime(ISOTIMEFORMAT, time.localtime())
    lvstr = ["MASSAGE", "WARNING", "ERROR  "]
    print lvstr[lv], logtime, ":", msg
    pass


def fetch_data(mysqlcur, start, limit):
    sql = "select * from %s_data limit %d,%d" % (REPO, start, limit)
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
        if sta.result != CE_STR:
            if not USR.get(sta.username):
                USR[sta.username] = user()
                data_arr.append(sta)
            else:
                if sta.problem_id not in USR[sta.username].ac:
                    USR[sta.username].ac.append(sta.problem_id)
                    data_arr.append(sta)
    logging("Data Cleaning Finish", 0)
    return data_arr


def cal_elo(ra, rb, res, lowflag):
    EA = 1 / (1 + 10 ** ((rb - ra) / 400.0))
    EB = 1 / (1 + 10 ** ((ra - rb) / 400.0))
    KA = KB = SA = SB = 0
    if ra > 2400:
        KA = 10
    elif ra > 1800:
        KA = 15
    else:
        KA = 30
    if rb > 1900 or rb < 1100:
        KB = 10
    elif rb > 1700 or rb < 1300:
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
    RB = rb + KB * (SB - EB) * (factor if lowflag else 1)
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
            if sta.problem_id < 10000:
                global MIN_RED, MAX_RED
                MIN_RED = min(MIN_RED, sta.problem_id)
                MAX_RED = max(MAX_RED, sta.problem_id)
                USR[sta.username].rating, PROBLEM_RATING[sta.problem_id] =\
                    cal_elo(
                        USR[sta.username].rating,
                        PROBLEM_RATING[sta.problem_id],
                        True if sta.result == "Accepted" else False,
                        sta.problem_id <= 2400
                    )
        logging("Computing the Problem Rating Finish", 0)
    # print PROBLEM_RATING
    pass


def print_to_file():
    fp = open("result_%s.txt" % REPO, "w")
    fp.write("%d\n" % (MAX_RED + 1 - MIN_RED))
    for i in range(MIN_RED, MAX_RED + 1):
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
    mysqlcur.execute("select count(RunID) from %s_data" % REPO)
    DATA_LEN = mysqlcur.fetchall()[0][0]
    logging("There are %d records in the DataBase" % DATA_LEN, 0)
    # print len(fetch_data(mysqlcur, 0, 100))
    elo_pro(mysqlcur)
    print_to_file()
    mysqlconn.close()
