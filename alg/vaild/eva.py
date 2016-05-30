# -*- coding: utf-8 -*-

import MySQLdb
import json
import time


mysql_con = None


def logging(msg, lv):
    ISOTIMEFORMAT = "%Y-%m-%d %X"
    logtime = time.strftime(ISOTIMEFORMAT, time.localtime())
    lvstr = ["MASSAGE", "WARNING", "ERROR  "]
    print lvstr[lv], logtime, ":", msg


def generate(count, delta, raw):
    logging("Start generating the test file!", 0)
    logging("Fetching Username in the Database!", 0)
    sql_cur = mysql_con.cursor()
    user_sql = "SELECT user,count(User) as subs FROM OJ_data.poj_data group by User order by subs desc limit 2000"
    sql_cur.execute(user_sql)
    user_list = []
    for record in sql_cur.fetchall():
        if int(record[1]) < 10000 and int(record[1]) > delta * 2 and record[0].find("judge") == -1 and record[0].find("nlgxh") == -1 and record[0].find("oj") == -1 and record[0].find("vj") == -1 and record[0].find("test") == -1:
            user_list.append(record[0])
    user_list = user_list[:count]
    logging("Fetching Username in the Database finished!", 0)
    logging("Fetching records in the Database", 0)
    status_sql = "SELECT Problem FROM OJ_data.poj_data where user = '%s' and result != 'Compile Error';"
    test_data = []
    for username in user_list:
        if len(test_data) == len(user_list) * 0.75:
            logging("Fetching records 75%", 0)
        elif len(test_data) == len(user_list) * 0.5:
            logging("Fetching records 50%", 0)
        elif len(test_data) == len(user_list) * 0.25:
            logging("Fetching records 25%", 0)
        user_record = []
        sql_cur.execute(status_sql % username)
        for status in sql_cur.fetchall():
            if len(user_record) == 0 or user_record[-1] != int(status[0]):
                user_record.append(int(status[0]))
        if len(user_record) > 2 * delta:
            test_data.append({
                "user": username,
                "status": user_record,
            })
    logging("Fetching records in the Database finished!", 0)
    logging("There will be %d users in the test file" % len(test_data), 0)
    logging("Start writing to the file", 0)
    test_file = open("./testfile.in" if raw else "./Rtestfile.in", "w")
    std_file = open("./testfile.out" if raw else "./Rtestfile.out", "w")
    if raw:
        test_file.write("%d\n" % len(test_data))
        std_file.write("%d\n" % len(test_data))
    for test_record in test_data:
        if raw:
            test_file.write("%s %d\n" % (test_record["user"], len(test_record["status"][:-delta])))
            std_file.write("%s %d\n" % (test_record["user"], len(test_record["status"][-delta:])))
        for status in test_record["status"][:-delta]:
            test_file.write("%d " % status)
        test_file.write("\n")
        for status in test_record["status"][-delta:]:
            std_file.write("%d " % status)
        std_file.write("\n")
    test_file.flush()
    std_file.flush()
    logging("Finish writing to the file", 0)
    logging("Finish generating the test file!", 0)


def vaild(fname):
    logging("Vaild start!", 0)
    out_file = open(fname, "r")
    std_file = open("./testfile.out", "r")
    count = int(std_file.readline())
    hit = 0
    amt_out = 0
    amt_std = 0
    for i in range(0, count):
        out_info = out_file.readline().split(" ")
        out_cnt = int(out_info[1])
        amt_out += out_cnt
        std_info = std_file.readline().split(" ")
        std_cnt = int(std_info[1])
        amt_std += std_cnt
        out_sta = map(int, filter(lambda x: len(x) > 1, out_file.readline().split(" ")))
        std_sta = map(int, filter(lambda x: len(x) > 1, std_file.readline().split(" ")))
        for sta in out_sta:
            if sta in std_sta:
                hit += 1
    P = hit * 1.0 / amt_out
    R = hit * 1.0 / amt_std
    F1 = 2 * P * R / (P + R)
    print ("Pre: %f, Rec: %f, F1: %f" % (P, R, F1))
    logging("Vaild finish!", 0)


def main():
    while True:
        print "(1)Generate Test File"
        print "(2)Vaild F1"
        print "(3)Generate Raw File"
        print "(4)Quit"
        option = int(input())
        if option == 1:
            count = input("please input the number of user in the test file: ")
            delta = input("please input the delta of vaild in the test file: ")
            generate(count, delta, True)
        elif option == 2:
            file_name = raw_input("please input the output file: ")
            vaild(file_name)
        elif option == 3:
            count = input("please input the number of user in the test file: ")
            delta = input("please input the delta of vaild in the test file: ")
            generate(count, delta, False)
        elif option == 4:
            break
        else:
            continue
    pass


if __name__ == '__main__':
    logging("F1 Vaild start!", 0)
    mysql_con = MySQLdb.connect(
        host="localhost",
        user="root",
        passwd="199528",
        db="OJ_data",
        charset="utf8"
    )
    main()
    mysql_con.close()
    logging("F1 Vaild finished!", 0)