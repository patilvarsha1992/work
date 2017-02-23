import Queue
import csv
import datetime
import re
import socket
import threading
import urllib2
from datetime import datetime
import sys
import MySQLdb
from bs4 import BeautifulSoup

csvFile = "C:/Users/vpatil/Desktop/re2.csv"

queue = Queue.Queue()

class ThreadUrl(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.name = "UrlGrab"
        self.queue = queue
        return

    def run(self):
        counter = 0
        while True:
            record = self.queue.get()
            jobDone = False
            first_name = record[0]
            print first_name
            last_name = record[1]
            print last_name
            website = record[2]
            print website
            auto_id = record[3]
            li_id = record[4]
            experience_id= record[5]


            fname = first_name.lower()
            # print fname
            lname = last_name.lower()
            # print lname
            domain = website.lower()
            # print website
            baseurl = 'https://www.google.com/search?q='
            initial_lname = lname[:1].lower()
            # print initial_lname
            initial_fname = fname[:1].lower()
            # print initial_fname


            email_1 = fname + '@' + domain
            print email_1
            email_2 = fname + initial_lname + '@' + domain + '&hl=en'
            email_3 =  initial_fname + lname + '@' + domain + '&hl=en'
            email_4 = fname + lname + '@' + domain + '&hl=en'
            email_5 = fname + '.' + initial_lname + '@' + domain + '&hl=en'
            email_6 = initial_fname + '.' + lname + '@' + domain + '&hl=en'
            email_7 = fname + '.' + lname + '@' + domain
            email_8 = fname + '_' + initial_lname + '@' + domain + '&hl=en'
            email_9 = initial_fname + '_' + lname + '@' + domain + '&hl=en'
            email_10 = fname + '_' + lname + '@' + domain + '&hl=en'
            email_11 = fname + '-' + initial_lname + '@' + domain + '&hl=en'
            email_12 = initial_fname + lname +'@' + domain + '&hl=en'
            email_13 = fname + '-' +lname + '@' + domain + '&hl=en'
            email_14 = lname + '@' + domain + '&hl=en'
            email_15=   lname + '.' +  fname + '@' + domain + '&hl=en'
            email_16= lname + initial_fname +'@' + domain + '&hl=en'
            email_17= lname + '_' + fname + '@' + domain + '&hl=en'
            email_18 = initial_fname + initial_lname  + '@' + domain + '&hl=en'
            email_19 = initial_fname + initial_lname+ '2' + '@' + domain + '&hl=en'
            email_20= initial_fname + initial_lname+ '3' + '@' + domain + '&hl=en'
            email_21= initial_fname + initial_lname+ '4' + '@' + domain + '&hl=en'
            email_22= initial_fname + initial_lname+ '5' + '@' + domain + '&hl=en'
            email_23= initial_fname + initial_lname+ '6' + '@' + domain + '&hl=en'
            email_24= initial_fname + initial_lname+ '7' + '@' + domain + '&hl=en'
            email_25 = initial_fname + initial_lname+ '8' + '@' + domain + '&hl=en'





            emails_pattarn = [ email_1, email_7, email_3,email_4,email_5,email_6,email_2,email_8,email_9,email_10,email_11,email_12,
                              email_13,email_14,email_15,email_16,email_17,email_18,email_19,email_20,email_21,email_22,email_23,email_24,email_25]

            for email in emails_pattarn:

                url = baseurl + email
                global m
                print '------Auto Id-------'
                print 'Auto_Id=' + str(auto_id)
                print 'Email_pattern-',':',email
                print '----Original url----'
                print url
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
                        fail_count -= 1
                        print ("******** Something went wrong ********: " + str(err))
                        if fail_count <= 0:
                            failed = False
                            print ("no more retry")
                # end while

                # start reading google search results for top 10
                rows = []
                try:
                    # blocks = soup.findAll("div", {"class": "g"}) # search for all div with class g
                    get_url_block = soup.findAll("h3", {"class": "r"}) # search for all h3 with class r
                    get_date_block = soup.findAll("span", {"class": "st"})  # search for all span with class st
                    get_email_block = get_date_block
                    for i in xrange(0, 10):
                    # for get_blocks in get_url_block:
                        cols = [li_id,experience_id,url]
                        print '------URLs-------'
                        get_blocks = get_url_block[i]
                        links = get_blocks.findAll('a')
                        for a in links:
                            get_url = re.split(":(?=http)", a["href"].replace("/url?q=", ""))
                            print get_url

                            print get_url.split('www.')
                            cols.append(get_url)

                        try:
                            date_block = get_date_block[i]
                            text_date = date_block.get_text()

                            print '----Date Text----'
                            print text_date
                            print '------Date-------'
                            regex = r'[\w]+\s\d{1,2},\s[0-9]{4}' # date pattern
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


                        if len(get_url_block) > 0:
                            email_blocks = get_email_block[i]
                            text_email = email_blocks.get_text()
                            print '-----Emails------'
                            regex =r'[\w\.-]+@[\w\.-]+[\w]'
                            match_email = re.findall(regex, text_email)
                            print match_email

                            if len(match_email) > 0:
                                for e in match_email:  # First Example
                                    e= e.lower()
                                    if e in emails_pattarn:
                                        print 'match email :', e
                                        domain_from_email = e.split("@")[-1]

                                        if domain_from_email in get_url:
                                            print 'email domain and url domain match '
                                            cols.append(e)
                                            jobDone = True
                                        else:
                                            print 'email domain and url domain not match '

                                    else:
                                      print 'email not match to pattern'

                            else:
                                print 'no emails found '

                                      # for msg in match_email:
                            #     if ((fname in msg or lname in msg) and domain in msg):
                            #         cols.append(msg)
                            #     elif str.lower(fname) in msg and str.lower(lname) in msg and str.lower(domain) in msg:
                            #         cols.append(msg)
                            #     else:
                            #         print 'Emails Not match'
                        else:
                            print 'Not found'
                            msg = ''
                            cols.append(msg)
                        rows.append(e,cols)

                        print '=========================================XXXENDXXX====================================================================='


                        if jobDone is True:
                            print "email found, no need to go all blocks"
                            break
                    # end for 1 to 10

                    if jobDone is True:
                        print "email found, no need to go all patterns"
                        break

                except:
                    print '***********GOOGLE SERACH RESULT NOT FOUND******** '
                    print '=========================================XXXXX====================================================================='



                ## store to queue2 instead of wiring to file
                print 'write file'
                with open(csvFile, "a") as output:
                    writer = csv.writer(output, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    for row in rows:
                        writer.writerow(row)

                self.queue.task_done()
            # end for
        return


def main():
    # printing start time here
    start_time1 = datetime.now()
    print "start_time:" + str(start_time1)

    # creating thread for downloading URL (or whatever it is)
    threadcount = 4
    for i in range(threadcount):
        t = ThreadUrl(queue)    # creating thread passing the queue
        # t.setDaemon(True)     # set thread as daemon ## dont worry about it
        t.start()               # starting the thread

    # creating SQL connection
    db = MySQLdb.connect(
        host="192.168.2.196",
        user="varsha",
        passwd="Varsha123",
        db="adhoc")

    # create cursor to be used again
    cur = db.cursor()

    # execute an sql Command
    cur.execute(
        "select first_name,last_name,domain,id_auto,li_id,experience_id from varsha_01_purple_lizard where id_auto between 282 and 286")

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
        li_id =row[4]
        experience_id=row[5]

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
