from __future__ import print_function
import sys
import string
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
from pyspark import SparkConf, SparkContext
from operator import add



def Func(lines):
    lines = lines.lower()
    lines = lines.split(',')
    return lines

def removePunctuation(x):
    s = x[1]
    for char in string.punctuation:
        s = s.replace(char, " ")
    res=s.lower().split(" ")
    return x[0], res


def main():

    conf = SparkConf().setAppName("BDA").setMaster("local")
    sc = SparkContext(conf=conf)

    #Get list of (filenames,content) as RDD
    filesRDD = sc.wholeTextFiles("file:///home/chaitanya/Downloads/blogs/*.xml")
    #5114.male.25.indUnk.Scorpio

    #Question 1
    #Store the filenames in fileNameRDD
    fileNameRDD = filesRDD.map(lambda x:x[0])
    #print(fileNameRDD.collect())
    
    #Store set of industries
    industriesNameRDD = fileNameRDD.map(lambda x:x.split('.')).map(lambda x:x[3]).distinct()
    
    #Broadcast variables
    broadcastRDD = sc.broadcast(industriesNameRDD.collect())
    #print broadcastRDD.value
    
    #Question 2
    #Extract only date and content [['date','content'],['date','content']]
    dcRDD = filesRDD.map(lambda x: x[1]).map(lambda x:x.split("<date>")).flatMap(lambda x: x[1:]).map(lambda x:x.split("</date>"))
    #print dcRDD.collect()
    
    #Remove punctuation
    removePunctuationRDD = dcRDD.map(lambda x: removePunctuation(x))
    #print removePunctuationRDD.collect()
    
    #Search industriesName in the content
    #searchRDD = removePunctuationRDD.map(lambda x: [x[0],x[1]]).map(lambda x: x[1])
    searchRDD = removePunctuationRDD.map(lambda x: x)
    #print searchRDD.collect() 
    
    #Get the format of date correctly [('2001','February',[Content]), ...]
    dtCleanRDD = searchRDD.map(lambda x: [x[0].split(','), x[1]]).map(lambda x: ((x[0][2], x[0][1]), x[1]))
    #print dtCleanRDD.collect()
    
    #tryRDD = dtCleanRDD.map(lambda x: x[1])
    #print tryRDD.collect()
    
    #alpha = ["Internet", "RealEstate"]
    
    findContentRDD = dtCleanRDD.flatMap(lambda x: [(word, x[0]) for word in broadcastRDD.value])
    #dateJoinRDD = findContentRDD.map(lambda x: (x))
    
    #print findContentRDD.collect()
    #print dateJoinRDD.collect()
        
    alignRDD = findContentRDD.map(lambda x: ((x[0],x[1][0],x[1][1]),1))
    #print alignRDD.take(5)

    
    countRDD = alignRDD.reduceByKey(lambda x,y:x+y)
    #print countRDD.take(10)
    
    
    tryRDD = countRDD.map(lambda x: x[0][1])
    #print tryRDD.collect()
    
    
    clubRDD = countRDD.map(lambda x: (x[0][0], str(x[0][1] + "-" + x[0][2]),x[1]))
    #print newRDD.take(5)
    
    mergeRDD = clubRDD.map(lambda x: ((x[0],x[1]),x[2]))
    #print newaRDD.take(5)
    
    
    #tryRDD = dtCleanRDD.map(lambda x: x[0])
    #print tryRDD.take(5)
    
    sumRDD = mergeRDD.reduceByKey(lambda x,y : x+y)
    #print newbRDD.take(5)
    
    #tryRDD = newbRDD.map(lambda x: x[1])
    #print tryRDD.take(5)
    
    clubNextRDD = sumRDD.map(lambda x : (x[0][0],(x[0][1],x[1])))
    #print newcRDD.take(5)
    
    solutionRDD = clubNextRDD.groupByKey()
    print(solutionRDD.mapValues(list).collect())
    


if __name__=="__main__":
    main()
