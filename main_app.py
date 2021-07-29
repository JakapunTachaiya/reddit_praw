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
import time

#add your own Reddit Rest Tokens
reddit = praw.Reddit(client_id="", \
                     client_secret='', \
                     user_agent='', \
                     username='', \
                     password='')

api = PushshiftAPI()

def jprint(obj):
    text = json.dumps(obj, indent=4)
    print(text)
    return text

def readFile(filename):
    filePD = pd.read_csv(filename, sep=',')
    return filePD

def get_date_string(created):
    # print(created,type(created))
    if created is None:
        return None
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


# def checkFetchedFile(JSON_output,start_date,end_date,subreddit):
#
#     threadDateTime = stringToDateTime(JSON_output['timeStamp'])
#
#     basePath = start_date.strftime("%Y-%m-%d") + '_' + end_date.strftime("%Y-%m-%d")
#     folPath = createFolder(basePath, subreddit, threadDateTime.year, threadDateTime.month)
#     threadId = JSON_output['id']
#     outPath = os.path.join(folPath, threadId)
#     resultBoolean = os.path.isfile(outPath)
#     if resultBoolean:
#         print("skip id : {}".format(threadId))
#
#     return resultBoolean
#
#
# def checkFetchedFile2(JSON_output,working_date,subreddit):
#
#     threadDateTime = stringToDateTime(JSON_output['timeStamp'])
#
#     basePath = "reddit"
#     folPath = createFolder(basePath, subreddit, threadDateTime.year, threadDateTime.month)
#     threadId = JSON_output['id']
#     outPath = os.path.join(folPath, threadId)
#     resultBoolean = os.path.isfile(outPath)
#     if resultBoolean:
#         print("skip id : {}".format(threadId))
#
#     return resultBoolean

def checkCurrentIdx(subreddit,working_date,idx,end_idx):
    outPath = "{} {}".format(subreddit,working_date)
    if not os.path.exists(outPath):
        with open(outPath, 'w') as f:
            f.write("{}_{}".format(idx,end_idx))
            f.close()
            currentIdx = 0

    if os.path.exists(outPath):
        with open(outPath, 'r') as f:
            currentIdx = int(f.readline().split("_")[0])

    return currentIdx


def addCurrentIdx(subreddit,working_date,idx,end_idx):
    outPath = "{} {}".format(subreddit,working_date)
    if not os.path.exists(outPath):
        with open(outPath, 'w') as f:
            f.write("{}_{}".format(idx,end_idx))
            f.close()
            currentIdx = 0

    if os.path.exists(outPath):
        with open(outPath, 'w') as f:
            f.write("{}_{}".format(idx+1, end_idx))
            f.close()
            currentIdx = 0

    return currentIdx

def getDataBydate(working_date,subreddit):


    current_datetime = dt.datetime.combine(working_date, dt.time(0, 0))
    next_datetime = dt.datetime.combine(working_date + timedelta(days=1), dt.time(0, 0))

    start_epoch = int(current_datetime.timestamp())
    end_epoch = int(next_datetime.timestamp())

    resultList = list(api.search_submissions(after=start_epoch,
                                             before=end_epoch,
                                             subreddit=subreddit,
                                             # filter=['id', 'subreddit'],
                                             # limit=10
                                             ))

    for idx in range(0, len(resultList)):

        idx = checkCurrentIdx(subreddit,working_date,idx,len(resultList))
        id = resultList[idx].id
        # t = time.process_time()
        # print("start getThread")

        threadContent, threadAuthor = getThreadDictFromList(resultList[idx])
        # print("end getcommentID",time.process_time() - t)

        print("{},({}/{}) getting tid: {}, #comemnts: {}".format(current_datetime.date(), idx + 1, len(resultList), id,
                                                                 threadContent['commsNum']))
        # t = time.process_time()
        # print("start getcommentID")
        posts, authors = getCommentByIdPushshift(id,subreddit)
        # print("end getcommentID",time.process_time() - t)

        # t = time.process_time()
        # print("start saveUser")
        JSON_output = {}
        threadContent['subreddit'] = subreddit
        JSON_output['op'] = threadContent
        JSON_output['posts'] = posts
        saveThread(JSON_output, subreddit)
        if threadAuthor is not None:
            authors.append(threadAuthor)
        authors = list(set(authors))

        saveUser(authors)

        addCurrentIdx(subreddit,working_date, idx, len(resultList))
        # print("end saveUser",time.process_time() - t)

    return


def checkhasattri(obj,name):
    try:
        if hasattr(obj, name):
            return getattr(obj, name)
        else:
            return None
    except prawcore.exceptions.NotFound:
        return None


def saveUser(authors):

    if not os.path.exists("redditors"):
        os.makedirs("redditors")

    for each in authors:
        outDict = {}

        # redditorObj = reddit.redditor(each.name)
        redditorObj = each

        # outDict['author_id'] = checkhasattri(redditorObj,"id")
        outDict['author_name'] = redditorObj.name
        outDict['comment_karma'] = checkhasattri(redditorObj,"comment_karma")
        outDict['create_date'] = get_date_string(checkhasattri(redditorObj,"created_utc"))
        outDict['has_verified_email'] = checkhasattri(redditorObj,"has_verified_email")
        outDict['is_employee'] = checkhasattri(redditorObj,"is_employee")
        outDict['is_mod'] = checkhasattri(redditorObj,"is_mod")
        outDict['is_gold'] = checkhasattri(redditorObj,"is_gold")

        outPath = os.path.join("redditors", redditorObj.name)
        dumptext = json.dumps(outDict, indent=4)

        with open(outPath, 'w') as f:
            f.write(dumptext)

    return

def saveThread(JSON_output,subreddit):
    threadDateTime = stringToDateTime(JSON_output['op']['timeStamp'])
    threadId = JSON_output['op']['id']
    # print(threadId)
    subboard = subreddit

    basePath = "reddit"

    dumptext = json.dumps(JSON_output, indent=4)
    folPath = createFolder(basePath, subboard, threadDateTime.year, threadDateTime.month)
    outPath = os.path.join(folPath, threadId)
    with open(outPath, 'w') as f:
        f.write(dumptext)

    return



def getThreadDictFromList(submission):
    outDict = {}

    if not submission.author:
        author_id = '[deleted]'
        author_name = '[deleted]'
        threadAuthor = None
    else:
        try:
            redditor = reddit.redditor(submission.author)
            # author_id = redditor.id
            author_name = redditor.name
            threadAuthor = redditor
        except prawcore.exceptions.NotFound:
            author_id = '[deleted]'
            author_name = '[deleted]'
            threadAuthor = None

    if not submission.upvote_ratio:
        outDict['upvote_ratio'] = None
    else:
        outDict['upvote_ratio'] = submission.upvote_ratio

    outDict['title'] = submission.title
    outDict['score'] = submission.score
    outDict['id'] = submission.id
    outDict['commsNum'] = submission.num_comments
    outDict['timeStamp'] = get_date_string(submission.created_utc)
    # outDict['author_id'] = author_id
    outDict['author_name'] = author_name
    # outDict['distinguished'] = submission.distinguished
    # outDict['edited'] = submission.edited
    outDict['is_self'] = submission.is_self
    outDict['locked'] = submission.locked
    outDict['selftext'] = submission.selftext
    outDict['num_comments'] = submission.num_comments
    outDict['over_18'] = submission.over_18
    outDict['spoiler'] = submission.spoiler
    outDict['subreddit'] = submission.subreddit
    outDict['stickied'] = submission.stickied

    outDict['url'] = submission.url

    return outDict, threadAuthor

# def getPrawbyID(id):
#
#     outDict = {}
#     submission = reddit.submission(id=id)
#
#     if not submission.author:
#         author_id = '[deleted]'
#         author_name = '[deleted]'
#         threadAuthor =None
#     else:
#         try:
#             redditor = reddit.redditor(submission.author.name)
#             # author_id = redditor.id
#             author_name = redditor.name
#             threadAuthor = redditor
#         except prawcore.exceptions.NotFound:
#             author_id = '[deleted]'
#             author_name = '[deleted]'
#             threadAuthor = None
#
#     outDict['title'] = submission.title
#     outDict['score'] = submission.score
#     outDict['id'] = submission.id
#     outDict['commsNum'] = submission.num_comments
#     outDict['timeStamp'] = get_date_string(submission.created_utc)
#     # outDict['author_id'] = author_id
#     outDict['author_name'] = author_name
#     outDict['distinguished'] = submission.distinguished
#     outDict['edited'] = submission.edited
#     outDict['is_self'] = submission.is_self
#     outDict['locked'] = submission.locked
#     outDict['selftext'] = submission.selftext
#     outDict['num_comments'] = submission.num_comments
#     outDict['over_18'] = submission.over_18
#     outDict['spoiler'] = submission.spoiler
#     outDict['subreddit'] = submission.subreddit.name
#     outDict['stickied'] = submission.stickied
#     outDict['upvote_ratio'] = submission.upvote_ratio
#     outDict['url'] = submission.url
#
#     return outDict, threadAuthor

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
        else:
            try:
                redditor = reddit.redditor(name=comment.author.name)
                # author_id = redditor.id
                author_name = redditor.name
                authors.append(redditor)
            except prawcore.exceptions.NotFound:
                author_id = '[deleted]'
                author_name = '[deleted]'

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
        # eachPost['author_id'] = author_id
        eachPost['author_name'] = author_name
        posts[comment.id] = eachPost


    return posts,authors

def getCommentByIdPushshift(id,subreddit):

    # get all comments from pushshift
    submission =  list(api.search_comments(
                                    subreddit=subreddit,link_id= id
                                    ))
    posts = {}
    authors = []

    for comment in submission:
        if not comment.author:
            author_id = '[deleted]'
            author_name = '[deleted]'
        else:
            try:
                #get User info from reddit api
                redditor = reddit.redditor(name=comment.author)
                # author_id = redditor.id
                author_name = redditor.name
                authors.append(redditor)
            except prawcore.exceptions.NotFound:
                author_id = '[deleted]'
                author_name = '[deleted]'

        eachPost = {}
        eachPost['comment'] = comment.body
        eachPost['timeStamp'] = get_date_string(comment.created_utc)
        # eachPost['distinguished'] = comment.distinguished
        # eachPost['edited'] = comment.edited
        eachPost['id'] = comment.id
        eachPost['is_submitter'] = comment.is_submitter
        eachPost['link_id'] = comment.link_id
        eachPost['parent_id'] = comment.parent_id
        eachPost['score'] = comment.score
        eachPost['stickied'] = comment.stickied
        # eachPost['author_id'] = author_id
        eachPost['author_name'] = author_name
        posts[comment.id] = eachPost


    return posts,authors

def main(argv):
    subreddit = sys.argv[1]

    startdate =  [int(x) for x in str(sys.argv[2]).split("-")]
    working_date = date(startdate[0], startdate[1], startdate[2])

    getDataBydate(working_date, subreddit)


if __name__ == "__main__":
    main(sys.argv[1:])