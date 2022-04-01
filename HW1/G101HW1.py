import sys
from pyspark import SparkContext, SparkConf


def main():
    """ 
        K = <int> Number of Partitions
        H = <int> Number of Products with Highest Popularity
        S = <str> Name of the Country -- 'all' Means all Countries
        dataset_path = Path of the Dataset File

    """
    # CHECKING NUMBER OF CMD LINE PARAMTERS
    assert len(
        sys.argv) == 5, "Usage: python WordCountExample.py <K> <H> <S> <dataset_path>"

    # SPARK SETUP
    conf = SparkConf().setAppName('WordCountExample').setMaster("local[*]")
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


if __name__ == "__main__":
    main()
