#!/usr/bin/env python

"""

Use Twitter API to grab tweets using hashtags; 
export text file

Uses Twython module to access Twitter API

"""

import sys
import string
import simplejson #install simplejson at https://pypi.python.org/pypi/simplejson/
from twython import Twython #install Twython at https://github.com/ryanmcgrath/twython

#WE WILL USE THE VARIABLES DAY, MONTH, AND YEAR FOR OUR OUTPUT FILE NAME
import datetime



#FOR OAUTH AUTHENTICATION -- NEEDED TO ACCESS THE TWITTER API
t = Twython(app_key='qtW8Q4270j67gooVD19tvAGo9', #REPLACE 'APP_KEY' WITH YOUR APP KEY, ETC., IN THE NEXT 4 LINES
    app_secret='0Jnd7nIrLYv3BhZdiT98iKcaQKZBEipXziib0CitV2RZ6zXATQ',
    oauth_token='2652772872-W9GTB3c973ayomnFPW1qEFgieNpskT5yJAD0c29',
    oauth_token_secret='nHdpiqLHzQVGSfVqgM7JgFN89FSpdkJpLdTNX1YYskx0G')

tagset = ['Celtics', 'Knicks', '76ers', 'Nets', 'Raptors', 'Bulls', 'Pacers', 'Bucks', 'Pistons', 'Cavs', 'MiamiHeat', 'OrlandoMagic', 'Hawks', 'Bobcats', 'Wizards', 'okcthunder', 'Nuggets', 'TrailBlazers', 'UtahJazz', 'TWolves', 'Lakers', 'Suns', 'GSWarriors', 'Clippers', 'NBAKings', 'GoSpursGo', 'Mavs', 'Hornets', 'Grizzlies', 'Rockets']

for h in tagset:

    now = datetime.datetime.now()
    day=int(now.day)
    month=int(now.month)
    year=int(now.year)

    hashtag = h ##### this line need to change
    delimiter = ','
    data = t.search(q='#'+hashtag, count=100)
    tweets = data['statuses']

    #NAME OUR OUTPUT FILE - %i WILL BE REPLACED BY CURRENT MONTH, DAY, AND YEAR
    outfn = hashtag+".csv"

    #NAMES FOR HEADER ROW IN OUTPUT FILE
    fields = "created_at text".split()

    #INITIALIZE OUTPUT FILE AND WRITE HEADER ROW   
    outfp = open(outfn, "w")
    #outfp.write(string.join(fields, ",") + "\n")  # comment out if don't need header

    for entry in tweets:
       
        r = {}
        for f in fields:
            r[f] = ""
        #ASSIGN VALUE OF 'ID' FIELD IN JSON TO 'ID' FIELD IN OUR DICTIONARY
        r['created_at'] = entry['created_at']
        s = entry['text']
        s = s.replace(',', '')
        s = s.replace('\n', '')
        s = s.replace(';', '')
        r['text'] = s
        
        print (r)
        #CREATE EMPTY LIST
        lst = []
        #ADD DATA FOR EACH VARIABLE
        for f in fields:
            lst.append(unicode(r[f]).replace("\/", "/"))
        #WRITE ROW WITH DATA IN LIST
        outfp.write(string.join(lst, delimiter).encode("utf-8") + "\n")

    outfp.close()  
