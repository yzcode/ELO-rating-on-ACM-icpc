# -*- coding: utf-8 -*-

import MySQLdb
import requests
import re
import datetime
import time
import sys


class HDU_fetcher(object):
    """docstring for POJ_fetcher"""
    def __init__(self, arg=None, MySQL_info=None, quiet=False):
        super
        (HDU_fetcher, self).__init__()
        self.arg = arg
        # Sqlite
        self.con = MySQLdb.connect(
            host=MySQL_info["host"],
            user=MySQL_info["user"],
            passwd=MySQL_info["passwd"],
            db=MySQL_info["db"],
            charset=MySQL_info["charset"]
        )
        try:
            self.create_table()
        except Exception, e:
            if re.match(r'Table .* already exists', e[1]):
                print "table already exists"
            else:
                raise e
        else:
            print "create table hdu_data successfully"
        # requests
        self.s = requests.Session()
        self.fileds = ['RunID','User','Problem','Result','Memory','Time','Language','Code_Length','Submit_Time']
        self.quiet = quiet

    def create_table(self):
        cu = self.con.cursor()
        cu.execute('''CREATE TABLE `hdu_data` (
            `RunID` int(11) NOT NULL,
            `User` varchar(45) NOT NULL,
            `Problem` int(11) NOT NULL,
            `Result` varchar(45) NOT NULL,
            `Memory` varchar(45) DEFAULT NULL,
            `Time` varchar(45) DEFAULT NULL,
            `Language` varchar(45) NOT NULL,
            `Code_Length` varchar(45) NOT NULL,
            `Submit_Time` datetime NOT NULL,
            PRIMARY KEY (`RunID`),
            UNIQUE KEY `RunID_UNIQUE` (`RunID`),
            KEY `index3` (`User`),
            KEY `index4` (`Problem`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        ''')
        cu.close()

    def fetch_html(self, start_at=16668947):
        # http://acm.hdu.edu.cn/status.php?first=16668947&user=&pid=&lang=&status=#status
        time.sleep(0.1)
        url = "http://acm.hdu.edu.cn/status.php?first=" + \
            str(start_at) + "&user=&pid=&lang=&status=#status"
        while True:
            success = True
            print "Fetch RunID %9d" % start_at
            try:
                resp = self.s.get(url, timeout=5)
            except Exception, e:
                print e
                success = False
            else:
                print "status code=%s" % resp.status_code
                if resp.status_code != 200:
                    success = False
                if re.search(r'Please retry after (?P<time>\d+)ms\.Thank you\.', resp.text):
                    success = False
                    retry_time = re.search(
                        r'Please retry after (?P<time>\d+)ms\.Thank you\.', resp.text).group('time')
                    retry_time = int(retry_time) / 1000.0
                    print "too_often  retry after %.3f s" % retry_time
                    time.sleep(retry_time)
            if success:
                break
        return resp

    def fetch(self, start_at=16668947):

        resp = self.fetch_html(start_at)
        resp.encoding = "GBK"
        patternstr = r'''
        <tr\s(bgcolor=\#D7EBFF\s)?align=center\s>
          <td.*?>(?P<RunID>\d+)</td>
          <td.*?>(?P<Submit_Time>.*?)</td>
          <td>(<a.*?>)?<font\scolor=.*?>(?P<Result>.*?)</font>(</a>)?</td>
          <td><a.*?>(?P<Problem>\d+)</a></td>
          <td>(?P<Time>\d+)MS</td>
          <td>(?P<Memory>\d+)K</td>
          <td>(?P<Code_Length>\d+)B</td>
          <td>(?P<Language>.*?)</td>
          <td.*?><a\shref=\"/userstatus.php\?user=(?P<User>.*?)\">.*?</a></td>
        </tr>
        '''
        # patternstr = patternstr.replace('\n','')
        # patternstr = re.escape(patternstr)
        # print patternstr
        pattern = re.compile(patternstr, re.S | re.VERBOSE)
        results = []
        # with open(r"Z:\resp%s.html" % start_at, 'w') as f:
        #     f.write(resp.content)
        for m in pattern.finditer(resp.text):
            line = {
                'RunID': m.group('RunID'),
                'User': m.group('User'),
                'Problem': m.group('Problem'),
                'Result': m.group('Result'),
                'Memory': m.group('Memory'),
                'Time': m.group('Time'),
                'Language': m.group('Language'),
                'Code_Length': m.group('Code_Length'),
                'Submit_Time': m.group('Submit_Time'),
            }
            results.append(line)
            # print line
        # print results
        print "got %d status" % len(results)
        if len(results) == 0:
            print resp.text
        self.insert(results)
        return results

    def insert(self, status):
        cu = self.con.cursor()
        status_array = []
        for s in status:
            sarr = []
            for key in self.fileds:
                sarr.append(s[key])
            status_array.append(sarr)
            if not self.quiet:
                print sarr
        # print status_array
        sql = "REPLACE INTO hdu_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cu.executemany(sql, status_array)
        self.con.commit()

    def make_up(self, detla, only_print=False, verify=False):
        cu = self.con.cursor()
        cu.execute("SELECT S1.RunID as A, S2.RunID as B from hdu_data as S1,hdu_data as S2 where B - A > ? and B = (select min(RunID) from poj_data where RunID > A) ORDER BY S1.RunID",[detla])
        bk = cu.fetchall()
        for A, B in bk:
            if verify:
                print "%8d --> %8d --> %8d  : %s" % (A, B-A, B, int(self.fetch(B)[0]['RunID']) == A)
            else:
                print "%8d --> %8d --> %8d" % (A, B-A, B)
            if not only_print:
                for i in xrange(A+20, B+20, 20):
                    self.fetch(i)

    def main(self, begin, end, now_time):
        if begin is None:
            cu = self.con.cursor()
            cu.execute("select RunID from hdu_data order by RunID DESC LIMIT 1")
            begin = cu.fetchone()
            if begin:
                begin = int(begin[0])
            else:
                begin = 1020
        if end is None:
            end = begin + 100 * 10000
        pre_time = datetime.datetime.strptime("2009-06-23 07:06:55", "%Y-%m-%d %H:%M:%S")
        for i in xrange(begin, end, 20):
            res = self.fetch(i)
            rcd_time = datetime.datetime.strptime(res[0]["Submit_Time"], "%Y-%m-%d %H:%M:%S")
            print rcd_time
            if rcd_time > now_time or (rcd_time == pre_time and (now_time - rcd_time).seconds < 3600):
                print now_time, rcd_time, pre_time
                break
            pre_time = rcd_time
            time.sleep(0.1)

if __name__ == '__main__':
    fetcher = HDU_fetcher(
        MySQL_info={
            "host": "localhost",
            "user": "root",
            "passwd": "199528",
            "db": "OJ_data",
            "charset": "utf8"
        },
        quiet=True
    )
    fetcher.main(None, None, datetime.datetime.today())
# 合并表的方法
# INSERT OR REPLACE INTO POJ_Status SELECT * FROM POJ_status.POJ_Status
