import imp
import sys
from pyspark import SparkContext, SparkConf


def productCustomer(row, country='all'):
    s = row.split(',')
    if country == 'all':
        return (s[1], int(s[6]))
    elif s[7] == country:
        return (s[1], int(s[6]))
    else:
        return


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

    # SPARK SETUP
    conf = SparkConf().setAppName('G101HW1').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read number of partitions
    K = sys.argv[1]
    assert K.isdigit(), "K must be an integer"
    K = int(K)

    # 2. Read Number of Products with Highest Popularity
    H = sys.argv[2]
    assert H.isdigit, "H Must be an integer"
    H = int(H)

    # 3. Read Name of the Country
    S = sys.argv[3]
    assert S.isalpha, "S Must be an string"

    # 4. Read Path of Dataset
    dataset_path = sys.argv[4]

    ############### Task 1 ###############

    rawData = sc.textFile(dataset_path, K).cache()
    rawData.repartition(K)
    print(rawData.count())

    product_customer = rawData.map(
        lambda row: productCustomer(row, S)).filter(lambda row: row)


if __name__ == "__main__":
    main()
