#!/usr/bin/python
import feedparser
from pprint import pprint
from pymongo import MongoClient,version as pymongoversion
import urllib.request
import datetime
import os
from bson.objectid import ObjectId
from bs4 import BeautifulSoup
import re

htmlfolder = "raw_html"
rssfolder = "raw_rss"
feedurl = "https://www.heise.de/newsticker/heise-atom.xml"

mongoclient = MongoClient()
database = mongoclient['heiseonline']
currenttime = datetime.datetime.now()
oid = ObjectId()
basefolder = str(oid)

index_collection = database['index']

indexobject = {}
indexobject['_id'] = oid
indexobject['date'] = currenttime

folder_tmp = basefolder+"/"+htmlfolder
if not os.path.exists(folder_tmp):
    os.makedirs(folder_tmp)
folder_tmp = basefolder+"/"+rssfolder
if not os.path.exists(folder_tmp):
    os.makedirs(folder_tmp)
del folder_tmp

rssfile = basefolder+"/"+rssfolder+"/"+"newsticker.xml"
urllib.request.urlretrieve(feedurl,rssfile)

feed = feedparser.parse(rssfile)

indexobject['feed'] = feed.feed
indexobject['rssfile'] = rssfile
index_collection.insert(indexobject)

collection = database[str(oid)]

for item in feed['items']:
    dbitem = {}
    dbitem['_id'] = ObjectId()
    htmlfile = basefolder+"/"+htmlfolder+"/"+str(dbitem['_id'])+".html"
    urllib.request.urlretrieve(item['link'],htmlfile)
    dbitem['feeditem'] = item
    dbitem['htmlfile'] = htmlfile
    collection.insert(dbitem)

# we fetched all articles, now we're parsing them

def get_number_of_comments(soup):
    comment_string = soup.find_all(class_="news")[0].find("b").string
    regex = re.compile("\d+")
    match = regex.search(comment_string)
    if match:
        return int(match.group())
    else:
        return 0

def get_meta_author(soup):
    field = soup.find("meta", attrs={"name":"author"})
    if not field:
        return None
    fieldvalue = field['content']
    authors = [ x.strip() for x in re.split('und|,', fieldvalue) ]
    return authors

for article in collection.find():
    print(article['_id'])
    with open(article['htmlfile']) as htmlfile:
        html = htmlfile.read()
    soup = BeautifulSoup(html, "html.parser")
    parsed = {}
    parsed['html_title'] = soup.title.string.strip()
    parsed['text_title'] = list(soup.find("h2").strings)[0].strip()
    parsed['number_of_comments'] = get_number_of_comments(soup)
    parsed['meta_author'] = get_meta_author(soup)
    parsed['text_author'] = soup.find(class_="author").string if soup.find(class_="author") else None

    article['parsed'] = parsed
    if not pymongoversion.startswith("3"):
        collection.remove({"_id":article['_id']})
        collection.insert(article)
    else:
        collection.replace_one({"_id":article['_id']},article)
        
