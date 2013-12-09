import urllib2
import json
import datetime
import calendar
import os
import sys
from time import time

"""
Fetch bioinfo-fr logs to diplay them using a log manager.
Running "$ python get-bioinfo-logs.py" will fetch all logs from august 2012 till now.
"""


SITE = 'bioinfo-fr.net'
OVH_LOGS_URL = 'https://logs.ovh.net'
PASSWORD_FILE = '.bioinfologs-pwd'
LOG = True


def log(s):
    if LOG:
        sys.stdout.write(s,)
        sys.stdout.flush()

def get_login_pass(secretfile):
    """
    Get the login & password from secretfile.
    Secretfile just contains log & pass formatted in JSON like that: 
    {"log": "thelogin", "pwd": "thepassword"}
    """
    conf = None
    with open(secretfile) as infile:
        conf = json.load(infile)
    return conf['log'], conf['pwd']


def configure_authentication(log, pazz):
    # password manager
    passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
    passman.add_password(None, OVH_LOGS_URL + '/' + SITE, log, pazz)
    # Create an OpenerDirector with support for Basic HTTP Authentication...
    authhandler = urllib2.HTTPBasicAuthHandler(passman)
    opener = urllib2.build_opener(authhandler)
    # ...and install it globally so it can be used with urlopen.
    urllib2.install_opener(opener)

def openurl(url):
    return urllib2.urlopen(url)


def day_looper(start, end):
    """
    Loop from start to end, one day at the time.
    """
    start = datetime.date(*start) - datetime.timedelta(days=1)
    end = datetime.date(*end)
    current_date = start
    while current_date < end:
        current_date += datetime.timedelta(days=1)
        yield current_date

def get_monthly_log_urls(year, month):
    """
    Yield all urls for the month given.
    """
    strmonth = month
    if len(str(month)) == 1:
        strmonth = '0%s' % month
    url = '%s/%s/%s' % (OVH_LOGS_URL, SITE, 'logs-%s-%s/' % (strmonth, year))
    for day in day_looper((year, month, 1), (year, month, calendar.monthrange(year, month)[1])):
        # url to get are https://logs.ovh.net/bioinfo-fr.net/logs-07-2012/bioinfo-fr.net-14-07-2012.log.gz for instance
        day_url = '%s%s-%s.log.gz' % (url, SITE, datetime.datetime.strftime(day, '%d-%m-%Y'))
        yield day_url


def get_logs_urls(year, month):
    """
    Yield all url from the month given to today.
    """
    start = datetime.date(year, month, 1)
    end = start.today()
    while start <= end:
        for url in get_monthly_log_urls(start.year, start.month):
            yield url
        start += datetime.timedelta(days=calendar.monthrange(year, month)[1])


def fetch(year, month, outdir):
    """
    Download all logs from OVH to the outdir specified.
    Downloaded them from the month specified to now.
    (Urllib must be configurated)
    """
    block_sz = 1024 * 4
    stats = {
        'failed': 0,
        'success': 0
    }
    t1 = time()
    for url in get_logs_urls(year, month):
        try:
            log('.')
            u = urllib2.urlopen(url)
            with open(os.path.join(outdir, os.path.split(url)[-1]), 'w') as out:
                while True:
                    buffer = u.read(block_sz)
                    if not buffer:
                        break
                    out.write(buffer)
            stats['success'] += 1
        except Exception as e:
            log('\n[%s] : %s\n' % (url, e))
            stats['failed'] += 1
    t2 = time()
    stats['time elapsed'] = '%0.2f' % (t2-t1)
    return stats

if __name__ == '__main__':
    login, pazz = get_login_pass(PASSWORD_FILE)
    configure_authentication(login, pazz)
    outdir = datetime.datetime.strftime(datetime.datetime.now(), '%d-%H:%M-bioinfo-fr.net-logs')
    os.mkdir(outdir)
    print fetch(2012, 7, outdir)
