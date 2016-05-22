import sqlite3
import requests
import re
import time
import getopt
import sys
import logging


class HDU_fetcher(object):
    """docstring for HDU_fetcher"""

    def __init__(self, arg=None, filename=None, quiet=False):
        super
        (HDU_fetcher, self).__init__()
        self.arg = arg
        # Sqlite
        self.con = sqlite3.connect(filename)
        try:
            self.create_table()
        except Exception, e:
            if re.match(r'table .* already exists', e.message):
                print "table already exists"
            else:
                raise e
        else:
            print "create table HDU_status successfully"
        # requests
        self.s = requests.Session()
        self.fileds = ['RunID', 'User', 'Problem', 'Result',
                       'Memory', 'Time', 'Language', 'Code_Length', 'Submit_Time']
        self.quiet = quiet

    def create_table(self):
        cu = self.con.cursor()
        cu.execute('''CREATE TABLE [HDU_Status] (
              [RunID] integer NOT NULL ON CONFLICT REPLACE PRIMARY KEY,
              [User] varchar(20),
              [Problem] integer,
              [Result] varchar(40),
              [Memory] varchar(10),
              [Time] varchar(10),
              [Language] varchar(10),
              [Code_Length] varchar(10),
              [Submit_Time] datetime);
        ''')
        cu.execute('CREATE INDEX [RunID] ON [HDU_Status] ([RunID] ASC);')
        cu.close()

    def fetch_html(self, start_at=16668947):
        # http://acm.hdu.edu.cn/status.php?first=16668947&user=&pid=&lang=&status=#status
        import time
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
        cu.executemany(
            'INSERT OR REPLACE INTO HDU_Status Values(?,?,?,?,?,?,?,?,?)', status_array)
        self.con.commit()

    def make_up(self, detla, only_print=False, verify=False):
        cu = self.con.cursor()
        cu.execute(
            "SELECT S1.RunID as A, S2.RunID as B from HDU_Status as S1,HDU_Status as S2 where B - A > ? and B = (select min(RunID) from HDU_Status where RunID > A) ORDER BY S1.RunID", [detla])
        bk = cu.fetchall()
        for A, B in bk:
            if verify:
                print "%8d --> %8d --> %8d  : %s" % (A, B - A, B, int(self.fetch(B)[0]['RunID']) == A)
            else:
                print "%8d --> %8d --> %8d" % (A, B - A, B)
            if not only_print:
                for i in xrange(A + 15, B + 15, 15):
                    self.fetch(i)

    def merge(self, to_merge):
        cu = self.con.cursor()
        cu.execute("ATTACH ? as toMerge", [to_merge])
        cu.execute(
            "SELECT Count(RunID), MIN(RunID), MAX(RunID) From toMerge.HDU_Status")
        merge_info = cu.fetchone()
        logging.info(
            'row_to_merge: {:,} , from {:,} to {:,}.'.format(*merge_info))
        cu.execute("INSERT OR REPLACE INTO HDU_Status SELECT * FROM toMerge.HDU_Status")
        self.con.commit()
        logging.info("Merge finished.")

    def main(self, begin, end):
        if begin is None:
            cu = self.con.cursor()
            cu.execute(
                "select RunID from HDU_Status order by RunID DESC LIMIT 1")
            begin = cu.fetchone()
            if begin:
                begin = int(begin[0])
            else:
                begin = 15
        if end is None:
            end = begin + 5000000
        if end < begin:
            begin, end = end, begin
        for i in xrange(begin, end, 15):
            self.fetch(i)
            # time.sleep(0.1)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.INFO,
                        format='%(asctime)s %(message)s')
    filename = "D:\Work\Python\HDU_status\HDU_status5M.db"
    begin = None
    end = None
    detla = None
    quiet = False
    only_print = False
    to_merge = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:b:e:r:m:hqp')
        for opt, val in opts:
            if opt == '-f':
                filename = val
            elif opt == '-b':
                begin = int(val)
            elif opt == '-e':
                end = int(val)
            elif opt == '-r':
                detla = int(val)
            elif opt == '-m':
                to_merge = val
            elif opt == '-q':
                quiet = True
            elif opt == '-p':
                only_print = True
            elif opt == '-h':
                print "Usage:[-f filename],[-b begin_runid],[-e end_runid]"
                sys.exit()
    except getopt.GetoptError:
        print "Usage:[-f filename],[-b begin_runid],[-e end_runid],[-d missing_detla]"
        # print help information and exit:
    fetcher = HDU_fetcher(filename=filename, quiet=quiet)
    if to_merge is not None:
        fetcher.merge(to_merge=to_merge)
    elif detla is None:
        fetcher.main(begin=begin, end=end)
    else:
        fetcher.make_up(detla=detla, only_print=only_print)

# 合并表的方法
# INSERT OR REPLACE INTO HDU_Status SELECT * FROM HDU_status.HDU_Status
