import pandas as pd
import pymongo
import pprint

connection = pymongo.MongoClient("localhost", 27017)
collection = connection.mongodb.geoindex
collection1 = connection.mongodb.geotextualindex
collection2 = connection.mongodb.geoindexbigdata
collection3 = connection.mongodb.geotextualindexbigdata


def query_without_textual_index_polygon_data():
    query=list(collection.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$geometry": { "type":"Polygon","coordinates":[ [[ -118.937563,34.19198813],[ -96.6995,32.974964],[-95.389055,29.723262],[ -118.937563,34.19198813]]]}}}},{"Text":{"$in":["Italian","Coffee","Tea"] }}]}}]))
    print(query)

def query_with_textual_index_polygon_data():
    query=list(collection1.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$geometry": { "type":"Polygon","coordinates":[ [[ -118.937563,34.19198813],[ -96.6995,32.974964],[-95.389055,29.723262],[ -118.937563,34.19198813]]]}}}},{"$text":{"$search":"Italian Coffee Tea"}}]}}]))
    print(query)

def query_with_textual_index_polygon_topk_data():
    query=list(collection1.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$geometry": { "type":"Polygon","coordinates":[ [[ -118.937563,34.19198813],[ -96.6995,32.974964],[-95.389055,29.723262],[ -118.937563,34.19198813]]]}}}},{"$text": { "$search": "cake tea" }}]}},{ "$sort": { "score": { "$meta": "textScore" } } },{"$limit":3}]))
    print(query)
    
def query_without_textual_index_circle_data():
    query=list(collection.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$centerSphere": [[ -118.937563,40.78422],200/3963.2]}}},{"Text":{"$in":["Italian","Coffee","Tea"] }}]}}]))
    print(query)

def query_with_textual_index_circle_data():
    query=list(collection1.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$centerSphere": [[ -118.937563,40.78422],200/3963.2]}}},{"$text":{"$search":"Italian Coffee Tea"}}]}}]))
    print(query)


def query_with_textual_index_circle_topk_data():
    query=list(collection1.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$centerSphere": [[ -118.937563,40.78422],200/3963.2]}}},{"$text": { "$search": "cake tea" }}]}},{ "$sort": { "score": { "$meta": "textScore" } } },{"$limit":3}]))
    print(query)
  
def query_without_textual_index_polygon_bigdata():
    query=list(collection2.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$geometry": { "type":"Polygon","coordinates":[ [[ -118.937563,34.19198813],[ -96.6995,32.974964],[-95.389055,29.723262],[ -118.937563,34.19198813]]]}}}},{"Text":{"$in":["Italian","Coffee","Tea"] }}]}}]))
    print(query)

def query_with_textual_index_polygon_bigdata():
    query=list(collection3.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$geometry": { "type":"Polygon","coordinates":[ [[ -118.937563,34.19198813],[ -96.6995,32.974964],[-95.389055,29.723262],[ -118.937563,34.19198813]]]}}}},{"$text":{"$search":"Italian Coffee Tea"}}]}}]))
    print(query)
def query_with_textual_index_polygon_topk_bigdata():
    query=list(collection3.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$geometry": { "type":"Polygon","coordinates":[ [[ -118.937563,34.19198813],[ -96.6995,32.974964],[-95.389055,29.723262],[ -118.937563,34.19198813]]]}}}},{"$text": { "$search": "cake tea" }}]}},{ "$sort": { "score": { "$meta": "textScore" } } },{"$limit":3}]))
    print(query)
  
def query_without_textual_index_circle_bigdata():
    query=list(collection2.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$centerSphere": [[ -118.937563,40.78422],200/3963.2]}}},{"Text":{"$in":["Coffee","Italian","Tea"] }}]}}]))
    print(query)

def query_with_textual_index_circle_bigdata():
    query=list(collection3.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$centerSphere": [[ -118.937563,40.78422],200/3963.2]}}},{"Text":{"$in":["Coffee","Italian","Tea"] }}]}}]))
    print(query)
def query_with_textual_index_circle_topk_bigdata():
    query=list(collection3.aggregate([{"$match":{ "$and": [{"location": {"$geoWithin": {"$centerSphere": [[ -118.937563,40.78422],200/3963.2]}}},{"$text": { "$search": "cake tea" }}]}},{ "$sort": { "score": { "$meta": "textScore" } } },{"$limit":3}]))
    print(query)
      
def query_without_textual_index_knn_data():
    query=list(collection.aggregate([{"$geoNear": {"near": { "type": "Point", "coordinates": [ -73.98142 , 40.71782 ] },"key": "location","distanceField": "dist.calculated","query": { "Text": {"$in": ["Italian","Coffee","Tea"]} }}},{ "$limit": 5 }]))
    print(query)
               
def query_with_textual_index_knn_bigdata():
    query=list(collection2.aggregate([{"$geoNear": {"near": { "type": "Point", "coordinates": [ -73.98142 , 40.71782 ] },"key": "location","distanceField": "dist.calculated","query": { "Text": {"$in": ["Italian","Coffee","Tea"]} }}},{ "$limit": 5 }]))
    print(query)


# Main function that runs the program
def main():
    #create indexes
    collection.create_index([("location",pymongo.GEOSPHERE)])
    #collection.create_index([("hilbertindex",pymongo.ASCENDING)])
    collection1.create_index([("location",pymongo.GEOSPHERE)])
    collection1.create_index([("Text",pymongo.TEXT )])
    collection2.create_index([("location",pymongo.GEOSPHERE)])
    collection3.create_index([("location",pymongo.GEOSPHERE)])
    collection3.create_index([("Text",pymongo.TEXT )])

    #Geometry Polygon
    #data
    #query_without_textual_index_polygon_data()
    #query_with_textual_index_polygon_data()
    #query_with_textual_index_polygon_topk_data()
    #big data
    #query_without_textual_index_polygon_bigdata()
    #query_with_textual_index_polygon_bigdata()
    #query_with_textual_index_polygon_topk_bigdata()
    

    #Geometry Circle
    #data
    #query_without_textual_index_circle_data()
    #query_with_textual_index_circle_data()
    #query_with_textual_index_circle_topk_data()
    #big data
    #query_without_textual_index_circle_bigdata()
    #query_with_textual_index_circle_bigdata()
    #query_with_textual_index_circle_topk_bigdata()

    #knn
    #query_without_textual_index_knn_data()
    #query_with_textual_index_knn_bigdata()
    
if __name__ == "__main__":
    main()

    
