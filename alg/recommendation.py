#!/usr/bin/env python
# -*- coding: utf-8 -*-
import MySQLdb
import numpy as np


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
        self.rating = 1500


class rmd(object):
    def __init__(self, MySQL_info):
        self.sqlcon = MySQLdb.connect(
            host=MySQL_info["host"],
            user=MySQL_info["user"],
            passwd=MySQL_info["passwd"],
            db=MySQL_info["db"],
            charset=MySQL_info["charset"]
        )
        self.PROBLEM_MAP = {}
        self.MYSQLCUR = self.sqlcon.cursor()
        sql = "select * from OJ_data.problem_rating where repo = 'Pku'"
        self.MYSQLCUR.execute(sql)
        for record in self.MYSQLCUR.fetchall():
            print record[0]

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

    def get_elo(self, username):
        sql = "select * from poj_data where User = '%s' and Result != 'Compile Error'"
        self.MYSQLCUR.execute(sql % username)
        rating = 1500.0
        black_hole = 1500
        ac_arr = []
        for item in self.MYSQLCUR.fetchall():
            sta = status(item)
            if sta.result == 'Accepted' and sta.problem_id not in ac_arr:
                ac_arr.append(sta.problem_id)
                rating, black_hole = self.cal_elo(
                    rating,
                    self.PROBLEM_MAP[sta.problem_id].rating,
                    True
                )
            elif sta.result != 'Accepted' and sta.problem_id not in ac_arr:
                rating, black_hole = self.cal_elo(
                    rating,
                    self.PROBLEM_MAP[sta.problem_id].rating,
                    False
                )
        return rating, ac_arr

    def cal_cosin(veca, vecb):
        x = np.array(veca)
        y = np.array(vecb)
        lx = np.sqrt(x.dot(x))
        ly = np.sqrt(y.dot(y))
        return float(x.dot(y) / (lx * ly))

    def rmd_by_problem(self, problem_id):
        if len(self.PROBLEM_MAP[problem_id].vec) != 200:
            return []
        ans = []
        for key in self.PROBLEM_MAP:
            if key != problem_id and len(self.PROBLEM_MAP[key].vec) == 200:
                cos_dis = self.cal_cosin(
                    self.PROBLEM_MAP[problem_id].vec,
                    self.PROBLEM_MAP[key].vec
                )
                ans.append((key, cos_dis))
        ans = sorted(ans, cmp=lambda x, y: cmp(y[1], x[1]))
        return ans

    def rmd_by_user(self, username):
        ac_sql = "select * from poj_data where User = '%s' \
            order by RunId desc limit 10"
        self.MYSQLCUR.execute(ac_sql % username)
        problem_set = set()
        for item in self.MYSQLCUR.fetchall():
            sta = status(item)
            problem_set.add(sta.problem_id)
        ans_set = set()
        for item in problem_set:
            rmd_res = self.rmd_by_problem(item)
            for items in rmd_res:
                ans_set.add(items)
        # print ans_set
        ans = []
        user_elo, ac_arr = self.get_elo(username)
        for item in ans_set:
            if item[0] not in ac_arr:
                rl_err = 1 - abs(user_elo - self.PROBLEM_MAP[item[0]].rating) / user_elo
                ans.append((item[0], item[1] * rl_err))
        ans = sorted(
            ans,
            cmp=lambda x, y: cmp(y[1], x[1])
        )
        return ans

if __name__ == '__main__':
    rmd_sys = rmd(
        MySQL_info={
            "host": "localhost",
            "user": "root",
            "passwd": "199528",
            "db": "OJ_data",
            "charset": "utf8"
        },
    )