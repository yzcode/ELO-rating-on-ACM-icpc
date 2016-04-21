# -*- coding: utf-8 -*-
import time
import numpy as np
PROBLEM_MAP = {}


class problem():
    """docstring for problem"""
    def __init__(self):
        self.vec = []
        self.rating = 0


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


def rmd_fun():
    while True:
        print "Please Input a problem ID:"
        problem_id = int(input())
        # problem_id = 1000
        if problem_id == 0:
            break
        ans = rmd_by_problem(problem_id)
        for i in range(0, min(len(ans), 10)):
            print ans[i][0], ans[i][1], PROBLEM_MAP[ans[i][0]].rating


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
    logging("Recommandation System Initial Succeeded!", 0)
    rmd_fun()
    logging("Recommandation System Quit!", 0)
