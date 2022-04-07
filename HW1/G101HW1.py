import sys
import os
from pyspark import SparkContext, SparkConf


def productCustomer(row, country='all', arbitary_value=0):
    """
    row = <str> '<0>TransactionID,<1>ProductID,<2>Description,<3>Quantity,<4>InvoiceDate,<5>UnitPrice,<6>CustomerID,<7>Country'
    it transform every row to ((<str> productID, <int> customerID),<int> arbitary_value) pair if conditions are met that v can 
    be any arbitary number because it will be removed at the end of MapReduce
    """
    s = row.split(',')
    if int(s[3]) > 0:
        if country == 'all':
            return [((s[1], int(s[6])), arbitary_value)]
        elif s[7] == country:
            return [((s[1], int(s[6])), 0)]
        else:
            return []
    else:
        return []


def changeValueToOne(iterator):
    """
    channges the value of (<K>,<V>) pair to 1 with generator
    """
    for pair in iter(iterator):
        yield (pair[0], 1)


def printProductPopularity(iterator):
    """
    formats list of (productID,popularity) to 'Product: {productID} Popularity: {popularity};' string and prints it
    """
    s = str()
    for i in iterator:
        s += f'Product: {i[0]} Popularity: {i[1]}; '
    print(s)


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

    # 1. Read Number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "<K> Number of partitions Must be an integer"
    K = int(K)

    # 2. Read Number of Products with Highest Popularity
    H = sys.argv[2]
    assert H.isdigit(), "<H> Number of Products with Highest Popularity  Must be an integer"
    H = int(H)

    # 3. Read Name of the Country
    S = sys.argv[3]
    assert S.isascii(), "<S> Name of the Country Must be a string"

    # 4. Read Path of Dataset
    dataset_path = sys.argv[4]
    assert os.path.isfile(dataset_path), "Dataset File not found"

    # SPARK SETUP
    conf = SparkConf().setAppName('G101HW1').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    ############### Task 1 ###############

    # read dataset and transform it to RDD with minimum K partitions and loads it to Memory
    rawData = sc.textFile(dataset_path, K).cache()
    rawData.repartition(K)  # enforces to repartition RDD to K partition
    print(f'Number of rows = {rawData.count()}')

    ############### Task 2 ###############
    product_customer = (rawData.flatMap(lambda row: productCustomer(row, S))  # <-- MAP PHASE (R1)
                        .groupByKey()  # <-- SHUFFLE+GROUPING
                        .map(lambda x: x[0])  # <-- REDUCE PHASE (R1)
                        )
    print(f'Product-Customer Pairs = {product_customer.count()}')

    ############### Task 3 ###############

    product_popularity1 = (product_customer.mapPartitions(changeValueToOne)  # <-- MAP PHASE (R1)
                           .groupByKey()  # <-- SHUFFLE+GROUPING
                           .mapValues(sum)  # <-- REDUCE PHASE (R1)
                           )

    ############## Task 4 ################
    product_popularity2 = (product_customer.map(lambda x: (x[0], 1))  # <-- MAP PHASE (R1)
                           .reduceByKey(lambda x, y: x+y) # <-- REDUCE PHASE (R1)
                           )

    ############## Task 5 ################
    if H > 0:
        highest_popularity = (product_popularity1
                              .sortBy(lambda x: (x[1], x[0]), ascending=False)
                              ).take(H) #make list of top H pairs from RDD
        print(f'Top {H} Products and their Popularities')
        printProductPopularity(highest_popularity)

    ############## Task 6 ################
    if H == 0:
        sorted_product_popularity1 = (product_popularity1
                                      .sortByKey()  # <-- SHUFFLE
                                      ).collect()   #transform RDD to list
        print("productPopularity1")
        printProductPopularity(sorted_product_popularity1)

        sorted_product_popularity2 = (product_popularity2
                                      .sortByKey()  # <-- SHUFFLE
                                      ).collect()  # transform RDD to list
        print("productPopularity2")
        printProductPopularity(sorted_product_popularity2)


if __name__ == "__main__":
    main()

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

############### Old Task 3 ###############
# product_popularity1 = (product_customer
#                        .groupByKey()
#                        .mapValues(len))

# python3 G101HW1.py 4 0 Italy sample_50.csv
# Number of rows = 50
# Product-Customer Pairs = 11
# productPopularity1:
# Product: 21733 Popularity: 2; Product: 22632 Popularity: 1; Product: 22960 Popularity: 4; Product: 22961 Popularity: 2; Product: 22969 Popularity: 1; Product: 84879 Popularity: 1;
# productPopularity2:
# Product: 21733 Popularity: 2; Product: 22632 Popularity: 1; Product: 22960 Popularity: 4; Product: 22961 Popularity: 2; Product: 22969 Popularity: 1; Product: 84879 Popularity: 1;

# python3 G101HW1.py 4 5 all sample_10000.csv
# Number of rows = 10000
# Product-Customer Pairs = 9104
# Top 5 Products and their Popularities
# Product POST Popularity 91; Product 22423 Popularity 91; Product 85123A Popularity 86; Product 47566 Popularity 84; Product 85099B Popularity 70;

# python3 G101HW1.py 4 5 United_Kingdom full_dataset.csv
# Number of rows = 406829
# Product-Customer Pairs = 238053
# Top 5 Products and their Popularities
# Product 85123A Popularity 821; Product 22423 Popularity 767; Product 47566 Popularity 659; Product 84879 Popularity 642; Product 22086 Popularity 594;
