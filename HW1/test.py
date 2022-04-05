import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

conf = SparkConf().setAppName('G101HW1').setMaster("local[*]")
sc = SparkContext(conf=conf)

""" 
    K = <int> Number of Partitions
    H = <int> Number of Products with Highest Popularity
    S = <str> Name of the Country -- 'all' Means all Countries
    dataset_path = Path of the Dataset File

"""
K = 4
H = 5
S = 'Italy'
dataset_path = 'sample_10000.csv'
# sqlContext = SQLContext(sc)
rawData = sc.textFile(dataset_path, K).cache()
rawData.repartition(K)
# rawData.collect()

# def productCustomer(row, country='all'):
#     """
#     row = [0:TransactionID, 1:ProductID, 2:Description, 3:Quantity, 4:InvoiceDate, 5:UnitPrice, 6:CustomerID, 7:Country]
#     """
#     s = row.split(',')
#     if int(s[3]) > 0:
#         if country == 'all':
#             return ((s[1], int(s[6])),0)
#         elif s[7] == country:
#             return ((s[1], int(s[6])),0)


# product_customer = (rawData.map(lambda row: productCustomer(row, S)).filter(lambda row: row)
#                     .groupByKey()
#                     .map(lambda x: x[0]))
# print(f'Product-Customer Pairs = {product_customer.count()}')

# product_popularity1 = (product_customer
#                        .groupByKey()
#                        .mapValues(len))
# print(product_popularity1.count())

# product_popularity2 = (product_customer.map(lambda x: (x[0], 1))
#                        .reduceByKey(lambda x, y: x+y)
#                        )
# print(product_popularity2.count())
