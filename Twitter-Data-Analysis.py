#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import json
import string
import urllib 
import time
import sys
import re

reload(sys)
sys.setdefaultencoding('utf8')

#Initializing Database handle
Conn = None
Conn = sqlite3.connect('TakeHomeTestt1.db')
Cur = Conn.cursor()

#Dropping tables if they already exist
Cur.execute('DROP TABLE IF EXISTS TWEETDATA')
Cur.execute('DROP TABLE IF EXISTS USERDICTIONARY')
Cur.execute('DROP TABLE IF EXISTS GEO')

#Creating tweeter table for loading data
Cur.execute("CREATE TABLE TWEETDATA (created_at DATE, id_str NUMERIC PRIMARY KEY, text TEXT, source TEXT, in_reply_to_user_id TEXT, in_reply_to_screen_name TEXT, in_reply_to_status_id TEXT, retweet_count NUMERIC, contributors TEXT, userid NUMERIC, geoid NUMERIC, FOREIGN KEY(userid) REFERENCES USERDICTIONARY(id), FOREIGN KEY(geoid) REFERENCES GEO(id));") 
#Creating user dictionary table for loading data
Cur.execute("CREATE TABLE USERDICTIONARY (id NUMERIC PRIMARY KEY, name TEXT, screen_name TEXT, description TEXT, friends_count NUMERIC, geoid NUMERIC, FOREIGN KEY(geoid) REFERENCES GEO(id));") 
#Creating Geo table for loading data
Cur.execute("CREATE TABLE GEO (id NUMERIC PRIMARY KEY, type TEXT, longitude NUMERIC, latitude NUMERIC);") 

starttime = time.time()
print "Start time: %s" % starttime

Geoid = "1234567890"
GeoType = "UNKNOWN"
Geolongitude = "12345"
Geolatitude = "67890"
GeoidTU = "NULL"

#Read file from Web
WebFile = urllib.urlopen("http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt")
#fd = open('Q:\TakeHomeFinal.txt','r')
#allTweets = fd.readlines()
allTweets = WebFile.readlines(700000000)

#Adding one default record into GEO table to corelate with all NULL location tweets in the tweetdata table
Onetime = 1
if Onetime == 1:
    InsertStringGeoTable = "Insert or Ignore into GEO values('" + str(Geoid) + "','" + str(GeoType) + "','" + str(Geolongitude) + "','" + str(Geolatitude) + "');"
    Cur.execute(InsertStringGeoTable)
    Onetime = Onetime + 1

#Looping through the parsed string to load individual lists(tweets) into json and then to database        
i = 0
for Tweet in allTweets:
    print "Number of tweet being written to Database %s" % i
    i = i + 1
    if i == 200001:
        break
    try:
        Parsed_json = json.loads(Tweet.decode('utf8'))
        
        created_at = Parsed_json['created_at']
        created_at = created_at.replace('\'',',')
        
        id_str = Parsed_json['id_str']
        id_str = id_str.replace('\'',',')
    
        text = Parsed_json['text']
        text = text.encode('utf-8')
        text = text.replace('\'',',')
    
        source = Parsed_json['source']
        source = source.replace('\'',',')
    
        in_reply_to_user_id = Parsed_json['in_reply_to_user_id']
        in_reply_to_user_id = str(in_reply_to_user_id).replace('None','NULL')
    
        in_reply_to_screen_name = Parsed_json['in_reply_to_screen_name']
        
        in_reply_to_status_id = Parsed_json['in_reply_to_status_id']
    
        retweet_count = Parsed_json['retweet_count']
        
        contributors = Parsed_json['contributors']
        
        userid = Parsed_json['user']['id']
                
        username = Parsed_json['user']['name']
        username = username.replace('\'',',')
        
        userscreenname = Parsed_json['user']['screen_name']
        userscreenname = userscreenname.replace('\'',',')
        
        userdescription = str(Parsed_json['user']['description'])
        userdescription = userdescription.replace('\'',',')
        
        userfriends_count = Parsed_json['user']['friends_count']
                
        if 'retweeted_status' in str(Parsed_json):
            retweet_count = Parsed_json['retweeted_status']['retweet_count']
    
        SearchString = "\"geo\":{\"type\":"
        
        if SearchString in str(Tweet).strip(' '):
            if Parsed_json['geo']:
                GeoType = Parsed_json['geo']['type']
                Geocoordinates = Parsed_json['geo']['coordinates']
                Geolongitude = Geocoordinates[0]
                Geolatitude = Geocoordinates[1]
                x = re.sub('[-.]', '', str(Geocoordinates[0]))
                y = re.sub('[-.]', '', str(Geocoordinates[1]))
                Geoid = str(x) + str(y)
        
        
                InsertStringGeoTable = "Insert or Ignore into GEO values('" + str(Geoid) + "','" + str(GeoType) + "','" + str(Geolongitude) + "','" + str(Geolatitude) + "');"
                InsertStringUserDictTable = "Insert or Ignore into USERDICTIONARY values('" + str(userid) + "','" + username + "','" + userscreenname + "','" + userdescription + "','" + str(userfriends_count) + "','" + str(Geoid) + "');"
                InsertStringTweetTable = "Insert or Ignore into TWEETDATA values('" + created_at + "','" + id_str + "','" + text + "','" + source + "','" + str(in_reply_to_user_id) + "','" + str(in_reply_to_screen_name) + "','" + str(in_reply_to_status_id) + "','" + str(retweet_count) + "','" + str(contributors) + "','" + str(userid) + "','" + str(Geoid) + "');"
        
                #Inserting tweets to database with condition that Geo location details are available with twet
                Cur.execute(InsertStringGeoTable)
                Cur.execute(InsertStringUserDictTable)
                Cur.execute(InsertStringTweetTable)
        else:
            InsertStringTweetTable = "Insert or Ignore into TWEETDATA values('" + created_at + "','" + id_str + "','" + text + "','" + source + "','" + str(in_reply_to_user_id) + "','" + str(in_reply_to_screen_name) + "','" + str(in_reply_to_status_id) + "','" + str(retweet_count) + "','" + str(contributors) + "','" + str(userid) + "','"  + str(GeoidTU) + "');"
            InsertStringUserDictTable = "Insert or Ignore into USERDICTIONARY values('" + str(userid) + "','" + username + "','" + userscreenname + "','" + userdescription + "','" + str(userfriends_count) + "','"  + str(GeoidTU) + "');"
        
            #Inserting tweets to database under condition that Geo location details arr NOT available with tweet
            Cur.execute(InsertStringUserDictTable)
            Cur.execute(InsertStringTweetTable)
    except ValueError:
         print Tweet
         
Conn.commit()
#Database is queried to retrive REQUIRED data

#Write and execute a SQL query to do the following: 
result = Cur.execute("Select count(*) from GEO")
rows = result.fetchall()
for row in rows:
    print row

result = Cur.execute("Select  count(*) from USERDICTIONARY")
rows = result.fetchall()
for row in rows:
    print row

result = Cur.execute("Select count(*) from TWEETDATA")
rows = result.fetchall()
for row in rows:
    print row
	
#Database is queried to retrive REQUIRED data

print "Tweets where tweet id_str contains “44” or “77” anywhere in the column:"
result = Cur.execute("Select * from TWEETDATA where id_str like '%44%' or '%77%'")
rows = result.fetchall()
for row in rows:
	print row

print "how many unique values are there in the “in_reply_to_user_id” column:"
result = Cur.execute("Select count(distinct(in_reply_to_user_id)) from TWEETDATA")
rows = result.fetchall()
for row in rows:
	print row

print "Find the tweet(s) with the longest text message"
result = Cur.execute("Select MAX(length(text)) from TWEETDATA")
rows = result.fetchall()
for row in rows:
	print row

print "Find the average longitude and latitude value for each user name. "
result = Cur.execute("Select Avg(longitude), Avg(latitude) from GEO where id IN (select geoid from USERDICTIONARY)")
rows = result.fetchall()
for row in rows:
	print row

for i in range(10):
	print "Find the average longitude and latitude value for each user name. "
	result = Cur.execute("Select Avg(longitude), Avg(latitude) from GEO where id IN (select geoid from USERDICTIONARY)")
	rows = result.fetchall()
	for row in rows:
		print row

Conn.close()