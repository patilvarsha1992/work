import socket
import re
import urllib2
from bs4 import BeautifulSoup
import csv
import threading
import datetime
from datetime import datetime
import sys
import MySQLdb
import Queue
# csvFile='C:/Users/vpatil/Desktop/google_search_output_test.csv'

config_file='config1'
open_config_file=open(config_file,'rU')

# after openning the config file, each line would be assigned to a variable
allinfo=open_config_file.readlines()
dbhost=allinfo[0].rstrip('\n')
dbuser=allinfo[1].rstrip('\n')
dbpasswd=allinfo[2].rstrip('\n')
dbdb=allinfo[3].rstrip('\n')
proxy_name=allinfo[4].rstrip('\n')
threadcount=int(allinfo[5].rstrip('\n'))
timeoutvar=int(allinfo[6].rstrip('\n'))
sql_query=allinfo[7].rstrip('\n')
csv_file=allinfo[8].rstrip('\n')
flag_capture_all_email=allinfo[9].rsplit('\n')
flag_capture_email_match_url_domain=allinfo[10].rsplit('\n')
flag_check_all_pattarn=allinfo[11].rsplit('\n')
flag_check_for_any_pattarn=allinfo[12].rsplit('\n')
flag_check_specified_pattarn=allinfo[13].rsplit('\n')
email_pattern_no=allinfo[14].rsplit('\n')
flag_record_all_result=allinfo[15].rsplit('\n')


get_emailPattern_no_in_string = (str(email_pattern_no))
position = get_emailPattern_no_in_string.split('_')[1]
get_position = position[:1]
get_position_in_int = int(get_position)
#

queue = Queue.Queue()
task_done = False


def replace_last(source_string, replace_what, replace_with):
    head, sep, tail = source_string.rpartition(replace_what)
    return head + replace_with + tail

class ThreadUrl(threading.Thread):

    def __init__(self, queue, name):
        threading.Thread.__init__(self)
        self.name = "UrlGrab " + name
        self.queue = queue
        return

    def run(self):
        counter = 0

        while True:
            record = self.queue.get()
            jobDone = False
            first_name = record[0]
            print 'first name:', first_name
            last_name = record[1]
            print 'last name:', last_name
            website = record[2]
            print 'website:', website
            auto_id = record[3]
            li_id = record[4]
            experience_id = record[5]

            fname = first_name.lower()
            lname = last_name.lower()
            domain = website.lower()
            baseurl = 'https://www.google.com/search?q='
            initial_lname = lname[:1].lower()
            initial_fname = fname[:1].lower()

            email_1 = fname + '@' + domain
            email_2 = fname + initial_lname + '@' + domain
            email_3 = initial_fname + lname + '@' + domain
            email_4 = fname + lname + '@' + domain
            email_5 = fname + '.' + initial_lname + '@' + domain
            email_6 = initial_fname + '.' + lname + '@' + domain
            email_7 = fname + '.' + lname + '@' + domain
            email_8 = fname + '_' + initial_lname + '@' + domain
            email_9 = initial_fname + '_' + lname + '@' + domain
            email_10 = fname + '_' + lname + '@' + domain
            email_11 = fname + '-' + initial_lname + '@' + domain
            email_12 = initial_fname + lname + '@' + domain
            email_13 = fname + '-' + lname + '@' + domain
            email_14 = lname + '@' + domain
            email_15 = lname + '.' + fname + '@' + domain
            email_16 = lname + initial_fname + '@' + domain
            email_17 = lname + '_' + fname + '@' + domain
            email_18 = initial_fname + initial_lname + '@' + domain
            email_19 = initial_fname + initial_lname + '2' + '@' + domain
            email_20 = initial_fname + initial_lname + '3' + '@' + domain
            email_21 = initial_fname + initial_lname + '4' + '@' + domain
            email_22 = initial_fname + initial_lname + '5' + '@' + domain
            email_23 = initial_fname + initial_lname + '6' + '@' + domain
            email_24 = initial_fname + initial_lname + '7' + '@' + domain
            email_25 = initial_fname + initial_lname + '8' + '@' + domain

            emails_pattern = [email_1, email_2, email_3, email_4, email_5, email_6, email_7, email_8, email_9, email_10,
                              email_11, email_12,
                              email_13, email_14, email_15, email_16, email_17, email_18, email_19, email_20, email_21,
                              email_22, email_23, email_24, email_25]

            email_pattern_no = emails_pattern[get_position_in_int - 1]

            rows = []
            counter = 0
            for email in emails_pattern:
                counter = counter + 1
                index = emails_pattern.index(email)
                print 'email_pattern_count :',index
                url = baseurl + email + '&hl=en'
                global m
                print '------Auto Id-------'
                print 'Auto_Id=' + str(auto_id)
                print 'Searching for Email_pattern-', ':', email
                print '----Original url----'
                print 'url:', url
                failed = True
                fail_count = 15
                while failed is True:

                    try:
                        proxy = urllib2.ProxyHandler({'https': '209.126.105.212:8081'})
                        opener = urllib2.build_opener(proxy)
                        urllib2.install_opener(opener)
                        opener.addheaders = [('User-agent',
                                              'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36')]
                        socket.setdefaulttimeout(10)
                        open_url = urllib2.urlopen(url)
                        failed = False
                        soup = BeautifulSoup(open_url, 'html.parser')
                        # save data to html file
                        filename = 'html2/test' + str(counter) + '.html'
                        with open(filename, 'w') as f:
                            f.write(str(soup))

                    except Exception as err:
                        print (threading.currentThread().name, " ******** Something went wrong ********: " + str(err))
                        fail_count -= 1
                        print "retries remaining:", fail_count
                        if fail_count <= 0:
                            failed = False
                            print ("no more retry")


                try:
                    for i in xrange(0, 10):
                        cols = [li_id, experience_id, url]
                        blocks = soup.findAll("div", {"class": "g"})

                        print '------URLs-------'
                        get_url_block = soup.findAll("h3", {"class": "r"})
                        get_blocks = get_url_block[i]
                        links = get_blocks.findAll('a')
                        for a in links:
                            get_url = re.split(":(?=http)", a["href"].replace("/url?q=", ""))
                            print get_url
                            cols.append(get_url)

                        try:
                            get_date_block = soup.findAll("span", {"class": "st"})
                            date_block = get_date_block[i]
                            text_date = date_block.get_text()

                            print '----Date Text----'
                            print text_date
                            print '------Date-------'
                            regex = r'[\w]+\s\d{1,2},\s[0-9]{4}'
                            match_date = re.findall(regex, text_date)
                            if len(match_date) > 0:
                                m =match_date[:1]
                                print m
                                cols.append(m)
                            else:
                                msg = ''
                                cols.append(msg)
                                print 'Date Not found'
                        except:
                            print 'DATE LIST INDEX OUT OF RANGE'
                            msg = ''
                            cols.append(msg)

                        get_url = str(get_url)
                        url1 = get_url.split('?q=')[:1]
                        url1 = str(url1)
                        print url1

                        if len(blocks) > 0:
                            get_email_block = soup.findAll("span", {"class": "st"})
                            email_blocks = get_email_block[i]
                            text_email = email_blocks.get_text()

                            print '-----Emails------'
                            regex =r'[a-zA-Z0-9]+\.[a-zA-Z0-9]+@[\w.]+'
                            match_email = re.findall(regex, text_email)
                            print match_email
                            email_found_flag = False


                            if len(match_email) > 0:

                                if (flag_capture_all_email[0] == 'Y'):

                                    print '*******capture all emails*******'
                                    for each_email in match_email:
                                        print 'got capture all email',each_email
                                        email_found_flag = True
                                        cols.append(each_email )
                                        jobDone = True
                                        break

                                        print 'done*******'
                                else:
                                    print 'dont capture'



                                if (flag_capture_email_match_url_domain[0] == 'Y'):
                                    for e in match_email:  # First Example
                                        if e[-1] == '.':
                                            e = replace_last(e, '.', '')
                                            print e

                                        e = e.lower()
                                        print 'email:', e
                                        print '*******_capture_email_matches_to_url_domain*******'
                                        domain_from_email = e.split("@")[-1]
                                        print 'email_domain:', domain_from_email
                                        print 'url:', get_url
                                        # get_url = str(get_url)

                                        if '?q=' in get_url:

                                            if domain_from_email in url1:
                                                print 'email domain and url domain  match in ?q=:', e
                                                email_found_flag = True
                                                cols.append(e)
                                                jobDone = True
                                                break
                                            else:
                                                print 'email domain and url domain not match in ?q= '
                                        else:
                                            if domain_from_email in url1:
                                                print 'email domain and url domain  match:', e
                                                email_found_flag = True
                                                cols.append(e)
                                                jobDone = True
                                                break
                                            else:
                                                print 'email domain and url domain not match '

                                else:
                                    print 'dont capture to get same domain email'

                                if (flag_check_all_pattarn[0] == 'Y'):
                                    print '******* search for  all emails patterns *******'
                                    for e in match_email:  # First Example
                                        if e[-1] == '.':
                                            e = replace_last(e, '.', '')
                                            print 'regax found email:',e

                                        e = e.lower()

                                        for i in range(len(emails_pattern)):
                                            print 'email_pattern:', emails_pattern[i]
                                            print 'found email '
                                            if e == emails_pattern[i]:
                                                email_found_flag = True
                                                cols.append(e)
                                                cols.append(i + 1)
                                                print cols


                                            else:
                                                print '*******_email does not match to email pattern*****'
                                else:
                                    print 'dont check for all pattern'

                                if (flag_check_for_any_pattarn[0] == 'Y'):
                                    print '******* search for any emails patterns. when found break *******'
                                    for e in match_email:  # First Example
                                        if e[-1] == '.':
                                            e = replace_last(e, '.', '')
                                            # print e

                                        e = e.lower()
                                        print "found email:",e

                                        for i in range(len(emails_pattern)):
                                            print 'email_pattern:', emails_pattern[i]
                                            if e == emails_pattern[i]:
                                                email_found_flag = True
                                                cols.append(e)
                                                cols.append(i + 1)
                                                print cols
                                                jobDone = True
                                                break
                                            else:
                                                print '*******_email does not match to  email pattern*****'
                                else:
                                    print 'dont check for any pattern'

                                if (flag_check_specified_pattarn[0] == 'Y'):

                                    print 'Assigned Email_pattern_no is:', email_pattern_no

                                    for e in match_email:
                                        if e[-1] == '.':
                                            e = replace_last(e, '.', '')
                                            # print e

                                        e = e.lower()
                                        print 'regex_found_email:', e
                                        print "Email Pattern:",email_pattern_no

                                        if e == email_pattern_no:
                                            print e
                                            print 'email matched to the assined pattern:', e
                                            cols.append(e)
                                            email_found_flag = True
                                            jobDone = True
                                            break
                                        else:
                                            print 'does not match to assined pattern'

                                else:
                                    print 'dont search for assined pattern'

                            else:
                                print 'no emails found'

                            if (flag_record_all_result[0] == 'Y'):
                                rows.append(cols)
                            else:
                                if email_found_flag == True:
                                    print"write to the file"
                                    rows.append(cols)

                            # rows.append(cols)

                        if jobDone is True:
                            print 'no need to go all blocks'
                            break
                    if jobDone is True:
                        print 'no need to go all pattern'
                        break

                except:
                    print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                    print '=========================================XXXXX====================================================================='

                # store to queue2 instead of wiring to file
            with open(csv_file, "a") as output:
                writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in rows:
                    writer.writerow(row)
            self.queue.task_done()
        return


def main():
    global task_done
    start_time1 = datetime.now()
    print  "Start_time" + str(start_time1)
    thread_count =threadcount
    for i in range(thread_count):
        thread_name = "thread " + str(i)
        t = ThreadUrl(queue, thread_name)
        t.setDaemon(True)
        t.start()

    db = MySQLdb.connect(
        host=dbhost,
        user=dbuser,
        passwd=dbpasswd,
        db=dbdb)

    cur = db.cursor()
    cur.execute(sql_query)

    counter=0
    for row in cur.fetchall():
        counter = counter + 1
        first_name = row[0]
        last_name = row[1]
        website = row[2]
        auto_id = row[3]
        li_id = row[4]
        experience_id = row[5]

        queue.put((first_name, last_name, website, auto_id,li_id, experience_id))
    queue.join()
    db.close()
    end_time1 = datetime.now()
    print 'End_Time=' + str(end_time1)
    print('Total_Duration: {}'.format(end_time1 - start_time1))
    task_done=True
if __name__ == '__main__':
    main()
