# -*- coding: utf-8 -*-
import time
import numpy as np
import MySQLdb
PROBLEM_MAP = {}
MYSQLCUR = {}


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


class problem():
    """docstring for problem"""
    def __init__(self):
        self.vec = []
        self.rating = 0


def cal_elo(ra, rb, res):
    EA = 1 / (1 + 10 ** ((rb - ra) / 400.0))
    EB = 1 / (1 + 10 ** ((ra - rb) / 400.0))
    KA = KB = SA = SB = 0
    if ra > 2400:
        KA = 10
    elif ra > 1800:
        KA = 20
    else:
        KA = 40
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
        factor = 0.20
    RA = ra + KA * (SA - EA) * factor
    RB = rb + KB * (SB - EB)
    return RA, RB


def get_elo(username):
    sql = "select * from poj_data where User = '%s'\
        and Result != 'Compile Error'"
    MYSQLCUR.execute(sql % username)
    rating = 1500.0
    black_hole = 1500
    ac_arr = []
    for item in MYSQLCUR.fetchall():
        sta = status(item)
        if sta.result == 'Accepted' and sta.problem_id not in ac_arr:
            ac_arr.append(sta.problem_id)
            rating, black_hole = cal_elo(
                rating,
                PROBLEM_MAP[sta.problem_id].rating,
                True
            )
        elif sta.result != 'Accepted' and sta.problem_id not in ac_arr:
            rating, black_hole = cal_elo(
                rating,
                PROBLEM_MAP[sta.problem_id].rating,
                False
            )
    return rating, ac_arr


def logging(msg, lv):
    ISOTIMEFORMAT = "%Y-%m-%d %X"
    logtime = time.strftime(ISOTIMEFORMAT, time.localtime())
    lvstr = ["MASSAGE", "WARNING", "ERROR  "]
    print lvstr[lv], logtime, ":", msg
    pass


def cal_cosin(veca, vecb):
    x = np.array(veca)
    y = np.array(vecb)
    lx = np.sqrt(x.dot(x))
    ly = np.sqrt(y.dot(y))
    return float(x.dot(y) / (lx * ly))
    pass


def rmd_by_problem(problem_id):
    if len(PROBLEM_MAP[problem_id].vec) != 200:
        return []
    ans = []
    for key in PROBLEM_MAP:
        if key != problem_id and len(PROBLEM_MAP[key].vec) == 200:
            cos_dis = cal_cosin(
                PROBLEM_MAP[problem_id].vec,
                PROBLEM_MAP[key].vec
            )
            ans.append((key, cos_dis))
    ans = sorted(ans, cmp=lambda x, y: cmp(y[1], x[1]))
    return ans


def rmd_by_user(username):
    ac_sql = "select * from poj_data where User = '%s' and Result = 'Accepted'\
        group by Problem order by RunId desc limit 3"
    MYSQLCUR.execute(ac_sql % username)
    problem_set = set()
    for item in MYSQLCUR.fetchall():
        pid = status(item).problem_id
        if PROBLEM_MAP.get(pid) and len(PROBLEM_MAP[pid].vec) == 200:
            problem_set.add(pid)
    ans_set = set()
    virtual_vec = np.zeros((200))
    for item in problem_set:
        virtual_vec += np.array(PROBLEM_MAP[item].vec)
    print virtual_vec
    print len(problem_set)
    virtual_vec /= len(problem_set)
    PROBLEM_MAP[0] = problem()
    PROBLEM_MAP[0].vec = list(virtual_vec)
    ans_set = rmd_by_problem(0)
    # print ans_set
    ans = []
    user_elo, ac_arr = get_elo(username)
    for item in ans_set:
        if item[0] not in ac_arr:
            rl_err = 1 - abs(user_elo - PROBLEM_MAP[item[0]].rating) / user_elo
            ans.append((item[0], item[1] * rl_err))
    ans = sorted(
        ans,
        cmp=lambda x, y: cmp(y[1], x[1])
    )
    return ans


def rmd_fun():
    while True:
        print "(1)Recommandation By Problem id"
        print "(2)Recommandation By Username"
        print "(3)Quit"
        option = int(input())
        if option == 1:
            print "Please Input A Porblem Id:"
            problem_id = int(input())
            ans = rmd_by_problem(problem_id)
            for i in range(0, min(len(ans), 10)):
                print ans[i][0], ans[i][1], PROBLEM_MAP[ans[i][0]].rating
        elif option == 2:
            username = raw_input("Please Input A Username:")
            ans = rmd_by_user(username)
            for i in range(0, min(len(ans), 10)):
                print ans[i][0], ans[i][1], PROBLEM_MAP[ans[i][0]].rating
        elif option == 3:
            break
        else:
            continue

if __name__ == '__main__':
    logging("Recommandation System Start", 0)
    vec_file = open("vec.txt", "r")
    rating_file = open("result_poj.txt", "r")
    logging("Reading Vector File...", 0)
    vector_amt, dem_amt = map(int, vec_file.readline().split(" "))
    for i in range(0, vector_amt + 1):
        raw_data = vec_file.readline().split(" ")
        if(len(raw_data) == 1 and raw_data[0] == ""):
            continue
        raw_problem = problem()
        raw_problem.vec = map(float, raw_data[1:-1])
        PROBLEM_MAP[int(raw_data[0])] = raw_problem
    logging("Reading Vector File Finish", 0)
    logging("Reading Rating File...", 0)
    rating_amt = int(rating_file.readline())
    for i in range(0, rating_amt + 1):
        raw_data = rating_file.readline().split("\t")
        if len(raw_data) < 2:
            continue
        raw_label, raw_rating = int(raw_data[0]), float(raw_data[1])
        if not PROBLEM_MAP.get(raw_label):
            PROBLEM_MAP[raw_label] = problem()
        PROBLEM_MAP[raw_label].rating = raw_rating
    logging("Reading Rating File Finish", 0)
    logging("Connect to The MySQL Server...", 0)
    mysqlconn = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="OJ_data",
        charset="utf8"
    )
    MYSQLCUR = mysqlconn.cursor()
    logging("Connected to The MySQL Server", 0)
    logging("Recommandation System Initial Succeeded!", 0)
    rmd_fun()
    logging("Recommandation System Quit!", 0)
