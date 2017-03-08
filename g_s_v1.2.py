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
flag_capture_all_email = allinfo[9].rsplit('\n')  # get yes or no value from config file
falg_capture_email_domain_matches_to_url_domain = allinfo[10].rsplit('\n')  # get yes or no value from config file
flag_check_all_patterns = allinfo[11].rsplit('\n')  # get yes or no value from config file
flag_check_for_any_pattern = allinfo[12].rsplit('\n')
flag_check_specified_pattern = allinfo[13].rsplit('\n')  # get yes or no value from config file
specified_email_pattern_no = allinfo[14].rsplit('\n')  # get yes or no value from config file
flag_record_all_result = allinfo[15].rsplit('\n')  # get yes or no value from config file

get_emailPattern_no_in_string = (str(specified_email_pattern_no))  # extracting specified email pattern in number
position = get_emailPattern_no_in_string.split('_')[1]
get_position = position[:1]
get_position_in_int = int(get_position)  # get email pattern number
queue = Queue.Queue()

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
            email_2 = fname + '.' + lname + '@' + domain
            email_3 = initial_fname + lname + '@' + domain
            email_4 = fname + lname + '@' + domain
            email_5 = fname + '.' + initial_lname + '@' + domain
            email_6 = initial_fname + '.' + lname + '@' + domain
            email_7 = fname + '.' + lname + '@' + domain

            emails_pattern = [email_1, email_2, email_3]

            email_pattern_no = emails_pattern[get_position_in_int - 1]
            print 'assign email_pattern:', email_pattern_no

            if (flag_check_specified_pattern[0] == 'Y'):
                print 'check one:'
                for e in emails_pattern:
                    if e == email_pattern_no:
                        print 'regax found email:', e
                        print 'assign email_pattern:', email_pattern_no
                        index = emails_pattern.index(e)
                        print 'email_pattern_count :', index
                        url = baseurl + e + '&hl=en'
                        global m
                        print '------Auto Id-------'
                        print 'Auto_Id=' + str(auto_id)
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

                        rows = []
                        try:
                            for i in xrange(0, 10):
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
                                        regex = r'[a-zA-Z0-9]+\.[a-zA-Z0-9]+@[\w.]+'
                                        match_email = re.findall(regex, text_email)
                                        print match_email

                                        email_found_flag = False

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
                    writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    for row in rows:
                        writer.writerow(row)
                self.queue.task_done()

            else:
                print'select  any option'
                for email in emails_pattern:
                    index = emails_pattern.index(email)
                    print 'email_pattern_count :', index
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
                            threading.currentThread().name, " ******** Something went wrong ********: " + str(err))
                            fail_count -= 1
                            print "retries remaining:", fail_count
                            if fail_count <= 0:
                                failed = False
                                print ("no more retry")

                    rows = []
                    all_emails_col = []
                    try:
                        # looping for each block (google search page )
                        for i in xrange(0, 10):
                            print'block no:', i + 1
                            print'auto_id:', str(auto_id)
                            print'email Pattern no:', index + 1
                            try:
                                cols = [li_id, experience_id, url]  # putting ld into list (cols)
                                blocks = soup.findAll("div",
                                                      {"class": "g"})  # getting div from google search result page

                                print '------URLs-------'
                                get_url_block = soup.findAll("h3",
                                                             {
                                                                 "class": "r"})  # getting heading tag from google search page
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
                                    email_blocks = get_email_block[i]
                                    text_email = email_blocks.get_text()

                                    print '-----Emails------'
                                    regex = r'[a-zA-Z0-9]+\.[a-zA-Z0-9]+@[\w.]+'
                                    match_email = re.findall(regex, text_email)
                                    print match_email
                                    email_found_flag = False

                                    if len(match_email) > 0:
                                        lines=[]
                                        if (flag_capture_all_email[0] == 'Y'):
                                            # if len(match_email) > 0:
                                            print '*******capture all emails*******'
                                            for each_email in match_email:
                                                if each_email in url:
                                                    print 'got capture all email', each_email
                                                    email_found_flag = True
                                                    all_emails_col.append(each_email)
                                                    all_emails_col.append('0')
                                                else:
                                                    all_emails_col.append(each_email)
                                                    all_emails_col.append('1')

                                            cols.append(all_emails_col)
                                        else:
                                            print 'dont capture'


                                        # # capture those emails which domain matches to url domain
                                        if (falg_capture_email_domain_matches_to_url_domain[0] == 'Y'):
                                            # if len(match_email) > 0:
                                            for e in match_email:
                                                if e[-1] == '.':
                                                    e = replace_last(e, '.',
                                                                     '')  # removing dot from the end of an email

                                                e = e.lower()
                                                # print 'regax found email:', e
                                                print '*******capture those emails which domain matches to url domain*******'
                                                domain_from_email = e.split("@")[-1]  # extracting domain from email
                                                print 'email_domain:', domain_from_email
                                                get_url = str(get_url)

                                                if 'www' in get_url:
                                                    domain_from_url = get_url.split("www.")[-1].split("/")[
                                                        0]  # extracting domain from url
                                                    print 'url domain :', domain_from_url
                                                    if domain_from_email in domain_from_url:  # checking if url domain and email domain match append into colm(list)
                                                        print 'email domain and url domain  matched ', e
                                                        email_found_flag = True  # set flag true if email found
                                                        cols.append(e)
                                                        jobDone = True
                                                        break
                                                    else:
                                                        print 'email domain and url domain did not match '
                                                    # cols.append('email domain not match1')

                                                else:
                                                    domain_from_url = get_url.split("://")[-1].split("/")[
                                                        0]  # checking if url domain and email domain match, append into colm(list)
                                                    print 'url domain :', domain_from_url
                                                    if domain_from_email in domain_from_url:
                                                        print 'email domain and url domain matched: ', e
                                                        email_found_flag = True  # set flag true if email found
                                                        cols.append(e)
                                                        jobDone = True
                                                        break
                                                    else:
                                                        print 'email domain and url domain did not match '
                                                    # cols.append('email domain not match2')
                                            if jobDone is False:
                                                cols.append('email domain not match')
                                        else:
                                            print '*******domain did not match*******'
                                            cols.append('email domain not match3')

                                        # # capturing email for all pattern
                                        if (flag_check_all_patterns[0] == 'Y'):
                                            # if len(match_email) > 0:
                                            print '******* search for  all emails patterns *******'
                                            for e in match_email:  # First Example
                                                if e[-1] == '.':
                                                    e = replace_last(e, '.', '')  # removing dot from end of an email
                                                    print 'regax found email:', e

                                                e = e.lower()
                                                for i in range(len(emails_pattern)):
                                                    print 'email_pattern:', emails_pattern[i]
                                                    if e == emails_pattern[
                                                        i]:  # checking found email matches to email pattern
                                                        print "regax found email matched with email pattern"
                                                        email_found_flag = True
                                                        cols.append(e)
                                                        cols.append(i + 1)  # appending email pattern number
                                                    else:
                                                        print '*******_email did not match to the email pattern*****'

                                        else:
                                            print 'email did not match to any pattern'
                                            cols.append('not match to all patt')

                                        # capturing found email matches to any pattern
                                        if (flag_check_for_any_pattern[0] == 'Y'):
                                            print '******* search for any email pattern. when found break *******'
                                            # if len(match_email) > 0:
                                            for e in match_email:  # First Example
                                                if e[-1] == '.':
                                                    e = replace_last(e, '.', '')

                                                e = e.lower()
                                                print "regax found email:", e

                                                for i in range(len(emails_pattern)):
                                                    print 'email_pattern:', emails_pattern[i]
                                                    if e == emails_pattern[i]:
                                                        print "regax found email matched with email pattern"
                                                        email_found_flag = True
                                                        cols.append(e)
                                                        # cols.append(i + 1)
                                                        jobDone = True
                                                        break
                                                    else:
                                                        print '*******_email did not match to any email pattern*****'
                                        else:
                                            print 'email did not match any pattern'
                                            cols.append('not match to any patt')
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
                        print 'write into file'
                        ## store to queue2 instead of wiring to file
                        with open(csv_file, "a") as output:
                            print"writtng"
                            writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL,lineterminator='\n')
                            print'writer:', writer
                            for row in rows:
                                writer.writerow(row)
                                print'write done***'


                    except RuntimeError as e:
                        print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                        print 'Error is ', sys.exc_info()[0]
                        print '===========', e

                    if jobDone is True:
                        print "email found, no need to go for all patterns"
                        break

            self.queue.task_done()

        return


def main():
    start_time1 = datetime.now()  # set start time
    print  "Start_time" + str(start_time1)

    # creating thread for downloading URL (or whatever it is)
    thread_count = threadcount
    for i in range(thread_count):
        thread_name = "thread " + str(i)
        t = ThreadUrl(queue, thread_name)  # creating thread passing the queue
        t.setDaemon(True)  # kill background thread
        t.start()  # starting the thread

    # creating SQL connection
    db = MySQLdb.connect(
        host=dbhost,
        user=dbuser,
        passwd=dbpasswd,
        db=dbdb)

    # create cursor to be used again
    cur = db.cursor()

    # execute an sql Command
    cur.execute(sql_query)

    counter = 0
    # fetching all results
    # fetchall() will give u all the results in a list/array
    # fetchone() will give u results one by one
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
    queue.join()
    # once done close database
    db.close()

    # printing Total_time
    end_time1 = datetime.now()
    print 'End_Time=' + str(end_time1)
    print('Total_Duration: {}'.format(end_time1 - start_time1))


if __name__ == '__main__':
    main()


