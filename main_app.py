import requests
import json
import os
import re
import datetime as dt
import pandas as pd
import sys, getopt
import praw
import prawcore
from psaw import PushshiftAPI
from datetime import timedelta, date

reddit = praw.Reddit(client_id='I1tNw_qvW9yMLw', \
                     client_secret='thSepgJ0Y-TRDd-Z3yyF7GA8Awk', \
                     user_agent='politics_crawler', \
                     username='jakapun', \
                     password='redditucr123')


api = PushshiftAPI()

def jprint(obj):
    text = json.dumps(obj, indent=4)
    print(text)
    return text

def readFile(filename):
    filePD = pd.read_csv(filename, sep=',')
    return filePD

def get_date_string(created):
    return dt.datetime.fromtimestamp(created).strftime("%Y/%m/%d-%H:%M:%S")

def stringToDateTime(dateTxt):
    result = dt.datetime.strptime(dateTxt, "%Y/%m/%d-%H:%M:%S")
    return result

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def createFolder(basePath,subboard,year,month):
    year = str(year)
    month = str(month)
    if not os.path.exists(os.path.join(basePath,subboard,year,month)):
        os.makedirs(os.path.join(basePath,subboard,year,month))
    return os.path.join(basePath, subboard, year, month)

def getDataBydate(start_date,end_date,subreddit):


    for current_date in daterange(start_date, end_date):


        current_datetime = dt.datetime.combine(current_date, dt.time(0, 0))
        next_datetime = dt.datetime.combine(current_date + timedelta(days=1), dt.time(0, 0))

        start_epoch = int(current_datetime.timestamp())
        end_epoch = int(next_datetime.timestamp())

        resultList = list(api.search_submissions(after=start_epoch,
                                              before=end_epoch,
                                    subreddit=subreddit,
                                    filter=['id', 'subreddit'],
                                    # limit=10
                                    ))
        # print(resultList)
        for idx in range(0,len(resultList)):
            id = resultList[idx].id
            threadContent,threadAuthor = getPrawbyID(id)
            print("{},({}/{}) getting tid: {}, #comemnts: {}".format( current_datetime.date(),idx+1,len(resultList),id,threadContent['commsNum']))
            posts, authors = getCommentById(id)

            JSON_output ={}
            JSON_output['op'] = threadContent
            JSON_output['posts'] = posts



            saveThread(JSON_output,start_date,end_date,subreddit)
            # jprint(posts)
            # jprint(JSON_output)
            if threadAuthor is not None:
                authors.append(threadAuthor)
            authors = list(set(authors))
            saveUser(authors)

    return

def saveUser(authors):

    if not os.path.exists("redditors"):
        os.makedirs("redditors")

    for each in authors:
        outDict = {}
        outDict['author_id'] = each.id
        outDict['author_name'] = each.name
        outDict['comment_karma'] = each.comment_karma
        outDict['create_date'] = get_date_string(each.created_utc)
        outDict['has_verified_email'] = each.has_verified_email
        outDict['is_employee'] = each.is_employee
        outDict['is_mod'] = each.is_mod
        outDict['is_gold'] = each.is_gold

        outPath = os.path.join("redditors", each.id)
        dumptext = json.dumps(outDict, indent=4)

        with open(outPath, 'w') as f:
            f.write(dumptext)


    return

def saveThread(JSON_output,start_date,end_date,subreddit):
    threadDateTime = stringToDateTime(JSON_output['op']['timeStamp'])
    threadId = JSON_output['op']['id']
    # print(threadId)
    subboard = subreddit

    basePath = start_date.strftime("%Y-%m-%d")+'_'+end_date.strftime("%Y-%m-%d")

    dumptext = json.dumps(JSON_output, indent=4)
    folPath = createFolder(basePath, subboard, threadDateTime.year, threadDateTime.month)
    outPath = os.path.join(folPath, threadId)
    with open(outPath, 'w') as f:
        f.write(dumptext)

    return


def getPrawbyID(id):

    outDict = {}
    submission = reddit.submission(id=id)

    if not submission.author:
        author_id = '[deleted]'
        author_name = '[deleted]'
        threadAuthor =None
    else:
        try:
            author_id = submission.author.id
            author_name = submission.author.name
            threadAuthor = submission.author
        except prawcore.exceptions.NotFound:
            author_id = '[deleted]'
            author_name = '[deleted]'
            threadAuthor = None


    outDict['title'] = submission.title
    outDict['score'] = submission.score
    outDict['id'] = submission.id
    outDict['commsNum'] = submission.num_comments
    outDict['timeStamp'] = get_date_string(submission.created_utc)
    outDict['author_id'] = author_id
    outDict['author_name'] = author_name
    outDict['distinguished'] = submission.distinguished
    outDict['edited'] = submission.edited
    outDict['is_self'] = submission.is_self
    outDict['locked'] = submission.locked
    outDict['selftext'] = submission.selftext
    outDict['num_comments'] = submission.num_comments
    outDict['over_18'] = submission.over_18
    outDict['spoiler'] = submission.spoiler
    outDict['subreddit'] = submission.subreddit.name
    outDict['stickied'] = submission.stickied
    outDict['upvote_ratio'] = submission.upvote_ratio
    outDict['url'] = submission.url

    return outDict, threadAuthor

def getCommentById(id):
    submission = reddit.submission(id=id)

    # print(submission.title)
    posts = {}
    authors = []
    submission.comments.replace_more(limit=None)
    # print(len(submission.comments.list()))
    for comment in submission.comments.list():
        if not comment.author:
            author_id = '[deleted]'
            author_name = '[deleted]'
        elif not hasattr(comment.author,'id') or not hasattr(comment.author,'name') :
            author_id = '[deleted]'
            author_name = '[deleted]'
        else:
            author_id = comment.author.id
            author_name = comment.author.name
            authors.append(comment.author)

        eachPost = {}
        eachPost['comment'] = comment.body
        eachPost['timeStamp'] = get_date_string(comment.created_utc)
        eachPost['distinguished'] = comment.distinguished
        eachPost['edited'] = comment.edited
        eachPost['id'] = comment.id
        eachPost['is_submitter'] = comment.is_submitter
        eachPost['link_id'] = comment.link_id
        eachPost['parent_id'] = comment.parent_id
        eachPost['score'] = comment.score
        eachPost['stickied'] = comment.stickied
        eachPost['author_id'] = author_id
        eachPost['author_name'] = author_name
        posts[comment.id] = eachPost

    # print(len(posts))
    # print(posts)
    # jprint(posts)

    return posts,authors

def main(argv):
    startdate =  [int(x) for x in str(sys.argv[1]).split("-")]
    enddate = [int(x) for x in str(sys.argv[2]).split("-")]


    start_date = date(startdate[0], startdate[1], startdate[2])
    end_date = date(enddate[0], enddate[1], enddate[2])

    getDataBydate(start_date,end_date,'politics')

    # posts,authors = getCommentById('dmi3qx')
    #
    # jprint(posts)



if __name__ == "__main__":
    main(sys.argv[1:])