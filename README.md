# Big-Data-Analytics

## Assignment 1: Concepts of Spark
[https://docs.google.com/document/d/e/2PACX-1vTEhcyiTr-ANuO6sScz74OcPjZuOfwtIpvyUnDLmMkLzRLn4Hd2zNCotxwmKW0PiFKgjCVXRg_TkFO_/pub]
- The assignment was to familiarize us with all the basic concepts. Such as ***RDD***, ***Lazy Evaluation***, ***Transformations***, ***Actions***, ***Mapper***, 
***Reducer***, and ***Combiner***.
- The first part of the assignment was to get the ***word count*** of the given file. 
- Thereafter, we were asked to find some insights from the data based on the requirement posted in the assignment.

## Assignment 2: Find similar regions of Long Island by comparing satellite imagery
[https://docs.google.com/document/d/e/2PACX-1vTa6cWpKDa0T4AqkcktoZqYAD8wq7LGW6xxI72gNGN55UMcwPw4OnuAu1QCllFj9Gm6q5l7nPrkLDau/pub]
- This assignment was a type of ***Image processing using Pyspark***.
- The main objective of the assignment was the following:
  - Implement ***Locality Sensitive Hashing*** (LSH).
  - Implement dimensionality reduction by using ***Principle Component Analysis*** (PCA).
  - To understand how to process images of large size.
 
## Project: Role of Tourism on Economic Development
There were three main objectives of the project:
1. To find the effect of variation in tourism with respect to the economic development of different states within USA.
2. Develop a ***Recommendation System***.
  - Given the name of the place the user is visiting, the recommendation system would recommend the top 10 best places to visit in that particular state.
  - Given the name of the genre the user likes, the recommendation system would recommend the top 10 best places to visit throughout the USA.
3. Fetch real time data from ***Twitter API*** simultaneously, so that the present trend is also considered for the best results.
  
#### Size of the dataset: 30Gb

#### Technologies Used: 
- ***Pyspark***
  - Basic Libraries Used: pyspark, pandas, xlrd, 
  - Advanced Libraries Used: tkinter, PIL, tweepy, tweepy.streaming
- ***Amazon EMR***
  - To store the data, as performing any analytics on the local system was not possible because of the size of the data.
