import csv
import threading
from datetime import datetime
import MySQLdb
import socket
import re
import urllib2
from bs4 import BeautifulSoup
import Queue
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
specified_email_pattern = allinfo[14].rsplit('\n')  # get yes or no value from config file
flag_record_all_result = allinfo[15].rsplit('\n')  # get yes or no value from config file

get_emailPattern_no_in_string = (str(specified_email_pattern))  # extracting specified email pattern in number

position = get_emailPattern_no_in_string.split('_')[1]
get_position = position[:1]
get_position_in_int = int(get_position)  # get email pattern number
queue = Queue.Queue()


def google_call(url):
    failed = True
    fail_count = 15
    while failed is True:
        try:
            proxy = urllib2.ProxyHandler({'https': '209.126.105.212:8081'})
            opener = urllib2.build_opener(proxy)
            urllib2.install_opener(opener)
            opener.addheaders = [('User-agent',
                                  'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36')]
            socket.setdefaulttimeout(25)
            open_url = urllib2.urlopen(url)
            failed = False
            soup = BeautifulSoup(open_url, 'html.parser')
            return soup

            # save data to html file
            # filename = 'html2/test' + str(counter) + '.html'
            # with open(filename, 'w') as f:
            #     f.write(str(soup))
        except Exception as err:
            fail_count -= 1
            print ("******** Something went wrong ********: " + str(err))
            if fail_count <= 0:
                failed = False
                print ("no more retry")
                # failed and no result from google

def email_patterns(first_name, last_name, domain, first_initial, last_initial):
    email_1 = first_name + '@' + domain
    email_2 = first_name + last_initial + '@' + domain + '&hl=en'
    email_3 = first_initial + last_name + '@' + domain + '&hl=en'
    email_4 = first_name + last_name + '@' + domain + '&hl=en'
    email_5 = first_name + '.' + last_initial + '@' + domain + '&hl=en'
    email_6 = first_initial + '.' + last_name + '@' + domain + '&hl=en'
    email_7 = first_name + '.' + last_name + '@' + domain
    email_8 = first_name + '_' + last_initial + '@' + domain + '&hl=en'
    email_9 = first_initial + '_' + last_name + '@' + domain + '&hl=en'
    email_10 = first_name + '_' + last_name + '@' + domain + '&hl=en'
    email_11 = first_name + '-' + last_initial + '@' + domain + '&hl=en'
    email_12 = first_initial + last_name + '@' + domain + '&hl=en'
    email_13 = first_name + '-' + last_name + '@' + domain + '&hl=en'
    email_14 = last_name + '@' + domain + '&hl=en'
    email_15 = last_name + '.' + first_name + '@' + domain + '&hl=en'
    email_16 = last_name + first_initial + '@' + domain + '&hl=en'
    email_17 = last_name + '_' + first_name + '@' + domain + '&hl=en'
    email_18 = first_initial + last_initial + '@' + domain + '&hl=en'
    email_19 = first_initial + last_initial + '2' + '@' + domain + '&hl=en'
    email_20 = first_initial + last_initial + '3' + '@' + domain + '&hl=en'
    email_21 = first_initial + last_initial + '4' + '@' + domain + '&hl=en'
    email_22 = first_initial + last_initial + '5' + '@' + domain + '&hl=en'
    email_23 = first_initial + last_initial + '6' + '@' + domain + '&hl=en'
    email_24 = first_initial + last_initial + '7' + '@' + domain + '&hl=en'
    email_25 = first_initial + last_initial + '8' + '@' + domain + '&hl=en'
    return [email_1, email_2, email_3, email_4, email_5,
            email_6, email_7, email_8, email_9, email_10,
            email_11, email_12, email_13, email_14, email_15,
            email_16, email_17, email_18, email_19, email_20,
            email_21, email_22, email_23, email_24, email_25]

def replace_last(source_string, replace_what, replace_with):  # creating function to remove dot from the end of an email
    head, sep, tail = source_string.rpartition(replace_what)
    return head + replace_with + tail

def get_links(cols, links):
    for a in links:
        get_url = re.split(":(?=http)", a["href"].replace("/url?q=", ""))
        print get_url

        print get_url.split('www.')
        cols.append(get_url)

def extract_date(cols, date_block):
    try:
        text_date = date_block.get_text()

        print '----Date Text----'
        print text_date
        print '------Date-------'
        regex = r'[\w]+\s\d{1,2},\s[0-9]{4}'  # date pattern
        match_date = re.findall(regex, text_date)
        if len(match_date) > 0:
            msg = match_date[:1]
            print msg
            cols.append(msg)
        else:
            msg = ''
            cols.append(msg)
            print 'Date Not found'
    except:
        print 'DATE LIST INDEX OUT OF RANGE'
        msg = ''
        cols.append(msg)

def extract_email_for_specified_email_pattern(cols, get_url_block, get_email_block,jobDone,rows,specified_email_pattern_no):
    if len(get_url_block) > 0:  # checking that google search result block is greater than 0
        email_blocks = get_email_block
        text_email = email_blocks.get_text()

        print '-----Emails------'
        regex = r'[a-zA-Z0-9]+\.[a-zA-Z0-9]+@[\w.]+'  # regax pattern to get email
        match_email = re.findall(regex, text_email)
        print "regax found emails:", match_email
        email_found_flag = False
        print'lent of email:', len(match_email)

        if len(match_email) > 0:  # checking more than one email

            # capture all emails
            # if (flag_capture_all_email[0] == 'Y'):
            #     # if len(match_email) > 0:
            #     print '*******capture all emails*******'
            #     for each_email in match_email:
            #         print 'got capture all email', each_email
            #         email_found_flag = True
            #         cols.append(each_email)
            #         jobDone = True
            #         break
            # else:
            #     print 'dont capture'
            #
            # # capture those emails which domain matches to url domain
            # if (falg_capture_email_domain_matches_to_url_domain[0] == 'Y'):
            #     # if len(match_email) > 0:
            #     for e in match_email:
            #         if e[-1] == '.':
            #             e = replace_last(e, '.', '')  # removing dot from the end of an email
            #
            #         e = e.lower()
            #         # print 'regax found email:', e
            #         print '*******capture those emails which domain matches to url domain*******'
            #         domain_from_email = e.split("@")[-1]  # extracting domain from email
            #         print 'email_domain:', domain_from_email
            #         get_url = str(get_url)
            #
            #         if 'www' in get_url:
            #             domain_from_url = get_url.split("www.")[-1].split("/")[
            #                 0]  # extracting domain from url
            #             print 'url domain :', domain_from_url
            #             if domain_from_email == domain_from_url:  # checking if url domain and email domain match append into colm(list)
            #                 print 'email domain and url domain  matched ', e
            #                 email_found_flag = True  # set flag true if email found
            #                 cols.append(e)
            #                 jobDone = True
            #                 break
            #             else:
            #                 print 'email domain and url domain did not match '
            #                 # cols.append('domain not match')
            #         else:
            #             domain_from_url = get_url.split("://")[-1].split("/")[
            #                 0]  # checking if url domain and email domain match append into colm(list)
            #             print 'url domain :', domain_from_url
            #             if domain_from_email == domain_from_url:
            #                 print 'email domain and url domain matched: ', e
            #                 email_found_flag = True  # set flag true if email found
            #                 cols.append(e)
            #                 jobDone = True
            #                 break
            #             else:
            #                 print 'email domain and url domain did not match '
                            # cols.append('domain not match')
            if (flag_check_specified_pattern == 'Y'):
                # if len(match_email) > 0:

                print 'Assigned email_pattern_no is:', specified_email_pattern_no

                for e in match_email:
                    if e[-1] == '.':
                        e = replace_last(e, '.', '')
                        # print e

                    e = e.lower()
                    print 'regex_found_email:', e
                    print " Specified Email Pattern:", specified_email_pattern_no

                    if e == specified_email_pattern_no:  # checking found email matches to email pattern
                        print e
                        print 'regax found email matched to the Specified email pattern:', e
                        cols.append(e)
                        email_found_flag = True
                        jobDone = True  # if email found set jobdone equal to true
                        break
                    else:
                        print 'did not match to Specified pattern'

        else:
            print 'email does not found'

    return (jobDone,rows,email_found_flag)

def extract_emails(cols, get_url_block, get_email_block,jobDone,rows,specified_email_pattern_no):

   if len(get_url_block) > 0:  # checking that google search result block is greater than 0
        email_blocks = get_email_block
        text_email = email_blocks.get_text()

        print '-----Emails------'
        regex = r'[a-zA-Z0-9]+\.[a-zA-Z0-9]+@[\w.]+'  # regax pattern to get email
        match_email = re.findall(regex, text_email)
        print "regax found emails:", match_email
        email_found_flag = False
        print'lent of email:', len(match_email)

        if len(match_email) > 0:  # checking more than one email

            # capture all emails
            if (flag_capture_all_email[0] == 'Y'):
                # if len(match_email) > 0:
                print '*******capture all emails*******'
                for each_email in match_email:
                    print 'got capture all email', each_email
                    email_found_flag = True
                    cols.append(each_email)
                    jobDone = True
                    break
            else:
                print 'dont capture'

            # capture those emails which domain matches to url domain
            if (falg_capture_email_domain_matches_to_url_domain[0] == 'Y'):
                # if len(match_email) > 0:
                for e in match_email:
                    if e[-1] == '.':
                        e = replace_last(e, '.', '')  # removing dot from the end of an email

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
                        if domain_from_email == domain_from_url:  # checking if url domain and email domain match append into colm(list)
                            print 'email domain and url domain  matched ', e
                            email_found_flag = True  # set flag true if email found
                            cols.append(e)
                            jobDone = True
                            break
                        else:
                            print 'email domain and url domain did not match '
                            # cols.append('domain not match')
                    else:
                        domain_from_url = get_url.split("://")[-1].split("/")[
                            0]  # checking if url domain and email domain match append into colm(list)
                        print 'url domain :', domain_from_url
                        if domain_from_email == domain_from_url:
                            print 'email domain and url domain matched: ', e
                            email_found_flag = True  # set flag true if email found
                            cols.append(e)
                            jobDone = True
                            break
                        else:
                            print 'email domain and url domain did not match '
                            # cols.append('domain not match')

            # capturing email for all pattern
            if (flag_check_all_patterns[0] == 'Y'):
                # if len(match_email) > 0:
                print '******* search for  all emails patterns *******'
                for e in match_email:  # First Example
                    if e[-1] == '.':
                        e = replace_last(e, '.', '')  # removing dot from end of an email
                        print 'regax found email:', e

                    e = e.lower()
                    for i in range(len(email_patterns)):
                        print 'email_pattern:', email_patterns[i]
                        if e == email_patterns[i]:  # checking found email matches to email pattern
                            print "regax found email matched with email pattern"
                            email_found_flag = True
                            cols.append(e)
                            cols.append(i + 1)  # appending email pattern number
                        else:
                            print '*******_email did not match to the email pattern*****'

            else:
                print 'email did not match to any pattern'

            # capturing found email matches to any pattern
            if (flag_check_for_any_pattern[0] == 'Y'):
                print '******* search for any email pattern. when found break *******'
                # if len(match_email) > 0:
                for e in match_email:  # First Example
                    if e[-1] == '.':
                        e = replace_last(e, '.', '')

                    e = e.lower()
                    print "regax found email:", e

                    for i in range(len(email_patterns)):
                        print 'email_pattern:', email_patterns[i]
                        if e == email_patterns[i]:
                            print "regax found email matched with email pattern"
                            email_found_flag = True
                            cols.append(e)
                            cols.append(i + 1)
                            jobDone = True
                            break
                        else:
                            print '*******_email did not match to any email pattern*****'

            else:
                print 'email did not match any pattern'

        else:
            print 'email does not found'

        # check want to capture all result or only capture those results which have emails
        if (flag_record_all_result[0] == 'Y'):
            rows.append(cols)
        else:
            if email_found_flag == True:
                print"write to the file"
                rows.append(cols)

   return (jobDone, rows, email_found_flag)

class ThreadUrl(threading.Thread):  # creating thread class

    def __init__(self, queue, name):  # creating function to init
        threading.Thread.__init__(self)
        self.name = "UrlGrab " + name
        self.queue = queue
        return

    def run(self):  # creating function to run thread

        while True:

            record = self.queue.get()  # getting record from queue
            first_name = record[0]
            print 'first name:', first_name
            last_name = record[1]
            print 'last name:', last_name
            website = record[2]
            print 'website:', website
            auto_id = record[3]
            li_id = record[4]
            experience_id = record[5]

            first_name_lower = first_name.lower()
            last_name_lower = last_name.lower()
            domain_lower = website.lower()
            base_url = 'https://www.google.com/search?q='
            first_initial_lower = first_name_lower[:1].lower()
            last_initial_lower = last_name_lower[:1].lower()

            # putting email patterns into list
            emails_pattern = email_patterns(first_name_lower, last_name_lower, domain_lower,
                                            first_initial_lower, last_initial_lower)

            specified_email_pattern_no = emails_pattern[get_position_in_int - 1]  # getting email pattern no. eg:email_7 so extracting '7' from email



            rows = []  # create list
            counter = 0
            if (flag_check_specified_pattern[0] == 'Y'):
                jobDone = False
                print 'specified_email_pattern:', specified_email_pattern_no

                # getting each email from email pattern list
                for e in emails_pattern:

                    if e == specified_email_pattern_no:
                        index = emails_pattern.index(e)
                        print 'email_pattern_no :', index+1
                        url = base_url + e + '&hl=en'
                        soup = google_call(url)
                        global m
                        print 'Searching for Email_pattern1-', ':', e
                        print '----Original url----'
                        print 'original url:', url

                try:
                    # looping for each block (google search page )
                    for i in xrange(0, 10):

                        cols = [li_id, experience_id, url]  # putting ld into list (cols)
                        print '------URLs-------'
                        get_url_block = soup.findAll("h3",{"class": "r"})  # getting heading tag from google search page
                        get_blocks = get_url_block[i]
                        links = get_blocks.findAll('a')  # finding for a tag (links)
                        get_links(cols, links)

                        get_date_block = soup.findAll("span", {"class": "st"})  # looking for span to get date
                        date_block = get_date_block[i]
                        extract_date(cols, date_block)

                        get_email_block = soup.findAll("span", {"class": "st"})  # looking for span to get email
                        extract_email_for_specified_email_pattern(cols, get_url_block, get_email_block,jobDone,rows,specified_email_pattern_no)

                    if jobDone is True:
                        print 'no need to go all blocks'
                        break

                except:
                    print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                    print '=========================================XXXXX====================================================================='

                if jobDone is True:
                    print 'no need to go all pattern'
                    break
            else:
                for email in emails_pattern:

                    print 'Auto_Id=' + str(auto_id)
                    counter = counter + 1
                    index = emails_pattern.index(email)  # to keep track which email pattern is currently running
                    print 'email_pattern_number :', index + 1
                    print 'Searching for Email_pattern', ':', email
                    url = base_url + email + '&hl=en'  # creating url (&hl=en is used bcz to set google search result into english language
                    soup = google_call(url)
                    global m
                    print '----Original url----'
                    print 'original url:', url
                    jobDone = False

                    try:
                        # looping for each block (google search page )
                        for i in xrange(0, 10):
                            cols = [li_id, experience_id, url]  # putting ld into list (cols)
                            print '------URLs-------'
                            get_url_block = soup.findAll("h3",
                                                         {"class": "r"})  # getting heading tag from google search page
                            get_blocks = get_url_block[i]
                            links = get_blocks.findAll('a')  # finding for a tag (links)
                            get_links(cols, links)

                            get_date_block = soup.findAll("span", {"class": "st"})  # looking for span to get date
                            date_block = get_date_block[i]
                            extract_date(cols, date_block)

                            get_email_block = soup.findAll("span", {"class": "st"})  # looking for span to get email
                            extract_emails(cols, get_url_block, get_email_block, jobDone, rows,
                                           specified_email_pattern_no)

                        if jobDone is True:
                            print 'no need to go all blocks'
                            break

                    except:
                        print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                        print '=========================================XXXXX====================================================================='

                    if jobDone is True:
                        print 'no need to go all pattern'
                        break
                    # wiring to file
            print 'write to the file'
            with open(csv_file, "a") as output:
                writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                for row in rows:
                    writer.writerow(row)
            self.queue.task_done()  # task done
            print 'write done'
        return

def main():
    global m
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
