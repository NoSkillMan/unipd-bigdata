import sys
import os
import random as rand
from pyspark import SparkContext, SparkConf


def productCustomer(row, country='all'):
    """
    row = [0:TransactionID, 1:ProductID, 2:Description, 3:Quantity, 4:InvoiceDate, 5:UnitPrice, 6:CustomerID, 7:Country]
    """
    s = row.split(',')
    if int(s[3]) > 0:
        if country == 'all':
            return [((s[1], int(s[6])), 0)]
        elif s[7] == country:
            return [((s[1], int(s[6])), 0)]
        else:
            return []
    else:
        return []


def changeValueToOne(iterator):
    for pair in iter(iterator):
        yield (pair[0], 1)


def main():
    """ 
        K = <int> Number of Partitions
        H = <int> Number of Products with Highest Popularity
        S = <str> Name of the Country -- 'all' Means all Countries
        dataset_path = Path of the Dataset File

    """
    # CHECKING NUMBER OF CMD LINE PARAMTERS
    assert len(
        sys.argv) == 5, "Usage: python G101HW1.py <K> <H> <S> <dataset_path>"

    # INPUT READING

    # 1. Read number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    # 2. Read Number of Products with Highest Popularity
    H = sys.argv[2]
    assert H.isdigit(), "H Must be an integer"
    H = int(H)

    # 3. Read Name of the Country
    S = sys.argv[3]
    assert S.isascii(), "S Must be an string"

    # 4. Read Path of Dataset
    dataset_path = sys.argv[4]
    assert os.path.isfile(dataset_path), "File not found"

    # SPARK SETUP
    conf = SparkConf().setAppName('G101HW1').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    ############### Task 1 ###############

    rawData = sc.textFile(dataset_path, K).cache()
    rawData.repartition(K)
    print(f'Number of rows = {rawData.count()}')

    ############### Old Task 2 ###############
    # def productCustomer(row, country='all'):
    #     """
    #     row = [0:TransactionID, 1:ProductID, 2:Description, 3:Quantity, 4:InvoiceDate, 5:UnitPrice, 6:CustomerID, 7:Country]
    #     """
    #     s = row.split(',')
    #     if int(s[3]) > 0:
    #         if country == 'all':
    #             return ((s[1], int(s[6])), 0)
    #         elif s[7] == country:
    #             return ((s[1], int(s[6])), 0)
    #
    # product_customer = (rawData.map(lambda row: productCustomer(row, S)).filter(lambda row: row)
    #                     .groupByKey()
    #                     .map(lambda x: x[0]))
    # print(f'Product-Customer Pairs = {product_customer.count()}')

    ############### Task 2 ###############
    product_customer = (rawData.flatMap(lambda row: productCustomer(row, S))
                        .groupByKey()
                        .map(lambda x: x[0])
                        )
    print(f'Product-Customer Pairs = {product_customer.collect()}')

    ############### Task 3 ###############

    # product_popularity1 = (product_customer
    #                        .groupByKey()
    #                        .mapValues(len))

    product_popularity1 = (product_customer
                           .mapPartitions(changeValueToOne)
                           .groupByKey()
                           .mapValues(sum)
                           )

    ############## Task 4 ################
    product_popularity2 = (product_customer.map(lambda x: (x[0], 1))
                           .reduceByKey(lambda x, y: x+y)
                           )

    ############## Task 5 ################
    if H > 0:
        highest_popularity = (product_popularity1
                              .sortBy(lambda x: x[1], ascending=False)
                              ).take(H)
        print(highest_popularity)

    ############## Task 6 ################

if __name__ == "__main__":
    main()

# python3 G101HW1.py 4 0 Italy sample_50.csv
# python3 G101HW1.py 4 5 United_Kingdom full_dataset.csv
