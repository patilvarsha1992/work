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
from threading import Thread, current_thread
import msvcrt

# csvFile='C:/Users/vpatil/Desktop/google_search_output_test.csv'
global m
config_file = 'config1'
open_config_file = open(config_file, 'rU')

# after openning the config file, each line would be assigned to a variable
allinfo = open_config_file.readlines()
dbhost = allinfo[0].rstrip('\n')  # get db host name from config file
dbuser = allinfo[1].rstrip('\n')  # get db user name from config file
dbpasswd = allinfo[2].rstrip('\n')  # get db password name from config file
dbdb = allinfo[3].rstrip('\n')  # get db database name from config file
proxy_name = allinfo[4].rstrip('\n')  # get proxy from config file
threadcount = int(allinfo[5].rstrip('\n'))  # get thread count from config file
timeoutvar = int(allinfo[6].rstrip('\n'))  # get time out value from config file
sql_query = allinfo[7].rstrip('\n')  # get sql query from config file
csv_file = allinfo[8].rstrip('\n')  # get csv file name from config file
flag_check_specified_pattern = allinfo[9].rsplit('\n')  # get yes or no value from config file
specified_email_pattern_no = allinfo[10].rsplit('\n')
flag_capture_all_email = allinfo[11].rsplit('\n')  # get yes or no value from config file
falg_capture_email_domain_matches_to_url_domain = allinfo[12].rsplit('\n')  # get yes or no value from config file
flag_check_all_patterns = allinfo[13].rsplit('\n')  # get yes or no value from config file
flag_check_for_any_pattern = allinfo[14].rsplit('\n')
flag_record_all_result = allinfo[15].rsplit('\n')  # get yes or no value from config file

get_emailPattern_no_in_string = (str(specified_email_pattern_no))  # extracting specified email pattern in number
position = get_emailPattern_no_in_string.split('_')[1]
get_position = position[:1]
get_position_in_int = int(get_position)  # get email pattern number
queue = Queue.Queue()

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
        print 'Thread start'+str(current_thread())

        while True:
            record = self.queue.get()
            if record is None:
                break

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
            email_12 = initial_fname + '-' + lname + '@' + domain
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

            emails_pattern = [email_1, email_2, email_3
                              ,email_4, email_5, email_6, email_7 ,email_8, email_9, email_10,
                              email_11, email_12,email_13, email_14, email_15, email_16, email_17, email_18, email_19, email_20, email_21,
                              email_22, email_23, email_24, email_25]

            email_pattern_no = emails_pattern[get_position_in_int - 1]
            print 'assign email_pattern:', email_pattern_no

            if (flag_check_specified_pattern[0] == 'Y'):
                print 'check one:'
                jobDone = False
                email_found_flag = False
                for e in emails_pattern:
                    if e == email_pattern_no:
                        print 'regax found email:', e
                        print 'assign email_pattern:', email_pattern_no
                        index = emails_pattern.index(e)
                        print 'email_pattern_count :', index+1
                        url = baseurl + e + '&hl=en'
                        global m
                        print '------Auto Id-------'
                        print 'Auto_Id=' + str(auto_id)
                        print'li_id:', str(li_id)
                        print 'Searching for Email_pattern1-', ':', e
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
                                socket.setdefaulttimeout(timeoutvar)
                                open_url = urllib2.urlopen(url)
                                failed = False
                                soup = BeautifulSoup(open_url, 'html.parser')
                                # save data to html file
                                filename = 'html2/test' + str(counter) + '.html'
                                with open(filename, 'w') as f:
                                    f.write(str(soup))
                            except Exception as err:
                                print (
                                    threading.currentThread().name,
                                    " ******** Something went wrong ********: " + str(err))
                                fail_count -= 1
                                print "retries remaining:", fail_count
                                if fail_count <= 0:
                                    failed = False
                                    print ("no more retry")
                                continue

                        rows = []
                        try:
                            for i in range(0, 10):
                                print 'Block no :', i
                                cols = [li_id, experience_id, url]
                                blocks = soup.findAll("div", {"class": "g"})
                                print '------URLs-------'
                                get_url_block = soup.findAll("h3", {"class": "r"})
                                if len(get_url_block) <= 0 or len(get_url_block) < (i + 1):
                                    continue
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
                                        m = match_date[:1]
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

                                try:
                                    if len(blocks) > 0:
                                        get_email_block = soup.findAll("span", {"class": "st"})
                                        email_blocks = get_email_block[i]
                                        text_email = email_blocks.get_text()

                                        print '-----Emails------'
                                        regex = r'[\w\.-]+@[\w\.-]+'
                                        match_email = re.findall(regex, text_email)
                                        print match_email
                                        if len(match_email) > 0:

                                            print '******check_specified_pattarn*******:', email_pattern_no
                                            print 'Assigned Email_pattern_no is:', get_position_in_int
                                            for each_email in match_email:
                                                # First Example
                                                if each_email[-1] == '.':
                                                    each_email = replace_last(each_email, '.', '')
                                                    each_email = each_email.lower()
                                                print "regax found email:", each_email
                                                if each_email == email_pattern_no:
                                                    print 'email matched to the specified pattern:', each_email
                                                    cols.append(each_email)
                                                    email_found_flag = True
                                                    jobDone = True
                                                    break
                                                else:
                                                    print 'does not match to specified pattern'
                                        else:
                                            print 'email does not found'
                                    else:
                                        print 'block does not found'

                                    if (flag_record_all_result[0] == 'Y'):
                                        rows.append(cols)
                                    else:
                                        if email_found_flag == True:
                                            rows.append(cols)
                                        else:
                                            print 'email does not found'
                                except:
                                    print 'block LIST INDEX OUT OF RANGE'

                                print '=========================================XXXXX====================================================================='
                                if jobDone is True:
                                    print "email found, no need to go for all blocks"
                                    break
                            if jobDone is True:
                                print "email found, no need to go for all patterns"
                                break

                        except RuntimeError as e:
                            print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                            print 'Error is ', sys.exc_info()[0]
                            print '===========', e

                print 'write into file'
                ## store to queue2 instead of wiring to file
                with open(csv_file, "a") as output:
                    writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,lineterminator='\n')
                    for row in rows:
                        writer.writerow(row)

            else:
                jobDone = False
                email_found_flag = False
                print'select  any option'
                for email in emails_pattern:
                    index = emails_pattern.index(email)
                    print 'email_pattern_count :', index+1
                    url = baseurl + email + '&hl=en'

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
                            opener.addheaders = [('User-agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36')]
                            socket.setdefaulttimeout(timeoutvar)
                            print 'opening the url'
                            open_url = urllib2.urlopen(url)
                            failed = False
                            soup = BeautifulSoup(open_url, 'html.parser')
                            filename = 'html2/test' + str(counter) + '.html'
                            with open(filename, 'w') as f:
                                f.write(str(soup))
                        except Exception as err:
                            print (
                            threading.currentThread().name, " ******** Something went wrong ********: " + str(err))
                            fail_count -= 1
                            print "retries remaining:", fail_count
                            if fail_count <= 0:
                                failed = False
                                print ("no more retry")
                                pass

                    rows = []
                    try:
                        # looping for each block (google search page )
                        for i in xrange(0, 10):
                            print'block no:', i + 1
                            print'auto_id:', str(auto_id)
                            print'li_id:', str(li_id)
                            print'email Pattern no:', index + 1
                            try:
                                cols = [li_id, experience_id, url]  # putting ld into list (cols)
                                blocks = soup.findAll("div",
                                                      {"class": "g"})  # getting div from google search result page
                                print '------URLs-------'
                                get_url_block = soup.findAll("h3", {"class": "r"})
                                if len(get_url_block) <= 0 or len(get_url_block) < (i + 1):
                                    continue
                                get_blocks = get_url_block[i]
                                links = get_blocks.findAll('a')  # finding for a tag (links)
                                for a in links:
                                    get_url = re.split(":(?=http)", a["href"].replace("/url?q=",
                                                                                      ""))  # cleaning url links (replacing "/url?q=" with empty string)
                                    print "url:", get_url
                                    cols.append(get_url)  # putting url into colm list

                                get_date_block = soup.findAll("span", {"class": "st"})  # looking for span to get date
                                date_block = get_date_block[i]
                                text_date = date_block.get_text()

                                print '----Date Text----'
                                print text_date
                                print '------Date-------'
                                regex = r'[\w]+\s\d{1,2},\s[0-9]{4}'  # regax pattern to find date
                                match_date = re.findall(regex, text_date)
                                if len(match_date) > 0:  # get only one date if more than one
                                    m = match_date[:1]
                                    print 'date:', m
                                    cols.append(m)  # appending to cols
                                else:
                                    msg = ''
                                    cols.append(msg)
                                    print 'Date Does Not Found'
                            except:
                                print 'DATE LIST INDEX OUT OF RANGE'
                                msg = ''
                                cols.append(msg)

                            try:
                                if len(blocks) > 0:
                                    get_email_block = soup.findAll("span", {"class": "st"})
                                    if len(blocks) <= 0 or len(blocks) < (i + 1):
                                        continue
                                    email_blocks = get_email_block[i]
                                    text_email = email_blocks.get_text()

                                    print '-----Emails------'
                                    regex = r'[\w\.-]+@[A-Za-z]+.[A-Za-z]+'
                                    match_email = re.findall(regex, text_email)
                                    print match_email

                                    if len(match_email) > 0:
                                        for each_email in match_email:
                                            email_found_flag=False
                                            tmp_row = []
                                            for c in cols:
                                                tmp_row.append(c)

                                            if (flag_capture_all_email[0] == 'Y'):
                                                # print '*******capture all emails*******'
                                                print 'capture all email', each_email
                                                email_found_flag = True
                                                if each_email[-1] == '.':
                                                    each_email = replace_last(each_email, '.','')  # removing dot from end of an email
                                                each_email = each_email.lower()
                                                if each_email in url:
                                                    tmp_row.append(each_email)
                                                    tmp_row.append('0')
                                                else:
                                                    tmp_row.append(each_email)
                                                    tmp_row.append('1')
                                            # else:
                                            #     print 'dont capture'

                                            # # capture those emails which domain matches to url domain
                                            if (falg_capture_email_domain_matches_to_url_domain[0] == 'Y'):
                                                    if each_email[-1] == '.':
                                                        each_email = replace_last(each_email, '.', '')  # removing dot from the end of an email
                                                    each_email = each_email.lower()
                                                    print '*******capture those emails which domain matches to url domain*******'
                                                    domain_from_email = each_email.split("@")[-1]  # extracting domain from email
                                                    get_url = str(get_url)

                                                    if 'www' in get_url:
                                                        domain_from_url = get_url.split("www.")[-1].split("/")[0]  # extracting domain from url
                                                        if domain_from_email in domain_from_url:  # checking if url domain and email domain match append into colm(list)
                                                            email_found_flag = True  # set flag true if email found
                                                            if (flag_capture_all_email[0] == 'N'):
                                                                tmp_row.append(each_email)
                                                                # tmp_row.append('1')
                                                                jobDone = True
                                                                print 'url domain ',domain_from_url,'email domain ',domain_from_email
                                                                print'tmp_row**',tmp_row
                                                                # break
                                                            else:
                                                                tmp_row.append(' 1')
                                                                jobDone = True
                                                                print 'url domain ', domain_from_url, 'email domain ', domain_from_email
                                                                print'tmp_row**', tmp_row
                                                                # break
                                                        else:
                                                            print 'email domain and url domain did not match '

                                                    else:
                                                        domain_from_url = get_url.split("://")[-1].split("/")[0]  # checking if url domain and email domain match, append into colm(list)
                                                        if domain_from_email in domain_from_url:
                                                            email_found_flag = True  # set flag true if email found
                                                            if (flag_capture_all_email[0] == 'N'):
                                                                tmp_row.append(each_email)
                                                                # tmp_row.append('1')
                                                                jobDone = True
                                                                print 'url domain ', domain_from_url, 'email domain ', domain_from_email
                                                                print'tmp_row**', tmp_row

                                                            else:
                                                                tmp_row.append(' 1')
                                                                jobDone = True
                                                                print 'url domain ', domain_from_url, 'email domain ', domain_from_email
                                                                print'tmp_row**', tmp_row


                                                    if jobDone is False:
                                                        tmp_row.append('0')


                                            # capturing email for all pattern
                                            if (flag_check_all_patterns[0] == 'Y'):
                                                if each_email[-1] == '.':
                                                    each_email = replace_last(each_email, '.', '')  # removing dot from end of an email
                                                each_email = each_email.lower()
                                                for i in range(len(emails_pattern)):
                                                    if each_email == emails_pattern[i]:  # checking found email matches to email pattern
                                                        if (flag_capture_all_email[0] == 'N'):
                                                            email_found_flag = True
                                                            tmp_row.append(each_email)
                                                            tmp_row.append(i + 1)
                                                            print "email:",each_email
                                                            print 'email matched to pattern'
                                                        else:
                                                            email_found_flag = True
                                                            tmp_row.append(i + 1)
                                                    else:
                                                        print"email did not matche to the pattern"



                                            if (flag_check_for_any_pattern[0]=='Y'):
                                                print 'match to one pattern***'
                                                if each_email[-1] == '.':
                                                    each_email = replace_last(each_email, '.', '')  # removing dot from end of an email
                                                each_email = each_email.lower()
                                                for i in range(len(emails_pattern)):
                                                    if each_email == emails_pattern[i]:  # checking found email matches to email pattern
                                                        if (flag_capture_all_email[0] == 'N'):
                                                            email_found_flag = True
                                                            tmp_row.append(each_email)
                                                            tmp_row.append(i + 1)
                                                            jobDone=True
                                                            print 'email matched to pattern'
                                                            break
                                                        else:
                                                            email_found_flag = True
                                                            tmp_row.append(i + 1)
                                                            jobDone=True
                                                            print 'email matched to pattern'
                                                            break

                                                print'did not match to the pattern'


                                            if (flag_record_all_result[0] == 'Y'):
                                                rows.append(tmp_row)
                                            else:
                                                if email_found_flag == True:
                                                    rows.append(tmp_row)
                                                else:
                                                    print 'email does not found'

                                            if jobDone is True:
                                                print "email found, no need to go for all blocks"
                                                break

                                    else:
                                        if (flag_record_all_result[0] == 'Y'):
                                            print 'email does not found'
                                            rows.append(cols)
                                        else:
                                            print 'email does not found'
                                else:
                                    print 'block does not found'

                            except RuntimeError as e:
                                print '***********block list INDEX OUT OF RANGE******** '
                                print 'Error is ', sys.exc_info()[0]
                                print '===========', e

                            print '=========================================XXXXX====================================================================='
                            if jobDone is True:
                                print "email found, no need to go for all blocks"
                                break

                    except:
                        print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                        print '=========================================XXXXX====================================================================='

                        # store to queue2 instead of wiring to file
                    print 'writing into the csv_file:',csv_file
                    with open(csv_file, "a") as output:
                        writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,
                                            lineterminator='\n')
                        for row in rows:
                            print'writing'
                            writer.writerow(row)
                    print 'writing done'

                    if jobDone is True:
                        print 'no need to go all pattern'
                        break

            self.queue.task_done()
        return


def main():
    start_time1 = datetime.now()  # set start time
    print  "Start_time" + str(start_time1)
    thread_count = threadcount
    for i in range(thread_count):
        thread_name = "thread " + str(i)
        t = ThreadUrl(queue, thread_name)  # creating thread passing the queu
        t.setDaemon(True)  # kill background thread
        t.start()  # start

    # creating SQL connection
    db = MySQLdb.connect(
        host=dbhost,
        user=dbuser,
        passwd=dbpasswd,
        db=dbdb)
    cur = db.cursor()
    cur.execute(sql_query)
    counter = 0
    for row in cur.fetchall():
        counter = counter + 1
        first_name = row[0]
        last_name = row[1]
        website = row[2]
        auto_id = row[3]
        li_id = row[4]
        experience_id = row[5]

        # Getting all data and placing it the queue
        queue.put((first_name, last_name, website, auto_id, li_id, experience_id))

    # wait for all the threads to finish their jobs and join main thread with all threads
    print queue
    print 'queue join****'
    queue.join()
    print 'queue join done****'
    db.close()
    # printing Total_time
    end_time1 = datetime.now()
    print 'End_Time=' + str(end_time1)
    print('Total_Duration: {}'.format(end_time1 - start_time1))
    raw_input("Press Enter to Exit...")



if __name__ == '__main__':
    main()


