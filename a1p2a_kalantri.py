from __future__ import print_function

import sys
from operator import add
import os
import pyspark
import random

from pyspark import SparkContext, SparkConf


def main():
    conf = SparkConf().setAppName("sample_app")
    sc = SparkContext(conf=conf)


    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, \
             powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general \
             educated opinion will have altered so much that one will be able to speak of\
             machines thinking without expecting to be contradicted."),
			(9, "The car raced past the finish line just in time."),
			(10, "Car engines purred and the tires burned.")]
 
    print("***************\nWords Count\n*********************\n")
    rdd=sc.parallelize(data)
 

    wordcnt=rdd.flatMap(lambda x: x[1].split(' '))\
               .map(lambda w:(w,1)) \
               .reduceByKey(lambda a,b: a+b)


    print(wordcnt.collect())



    data1 = [('R', [x for x in range(50) if random.random() > 0.5]),
           ('S', [x for x in range(50) if random.random() > 0.75])]

    data0 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
			 ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]

    def func(x):
        if x:
            print("\n")
   
    dataRDD=sc.parallelize(data0)
    

    kvRDD = dataRDD.map(lambda x: [(number,x[0]) for number in x[1]])
    nonIntersectingRDD = kvRDD.flatMap(lambda x:x).reduceByKey(lambda x,y: (x,y)).map(lambda x: x[0] if (len(x[1])==1 and x[1][0]=='R') else None)

    print("\n\n***************Output of the first interesting data:****************************")
    outputRDD = nonIntersectingRDD.filter(func(x))
    print(outputRDD.collect())
   

    dataNextRDD=sc.parallelize(data1)
    
    kvRDD = dataNextRDD.map(lambda x: [(number,x[0]) for number in x[1]])
    nonIntersectingRDD = kvRDD.flatMap(lambda x:x).reduceByKey(lambda x,y: (x,y)).map(lambda x: x[0] if (len(x[1])==1 and x[1][0]=='R') else None)  
    print("\n\n***************Output of the second interesting data:****************************")
    outputRDD = nonIntersectingRDD.filter(func(x))
    print(outputRDD.collect())

    sc.stop()


if __name__ == "__main__":
    main()
