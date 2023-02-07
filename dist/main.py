import configparser
import os
import sys


if os.path.exists('utils.zip'):
    sys.path.insert(0, 'utils.zip')
else:
    sys.path.insert(0, './utils')

from utils.Accident import Accident

if __name__ == '__main__':
    
    config = configparser.ConfigParser()
    config.read("config.ini")



    from pyspark.sql import SparkSession
    try:
        spark = SparkSession.builder \
            .appName(config["SPARK"]["APPNAME"]) \
            .getOrCreate()
        accident=Accident(spark,config)
        accident.run()
    finally:
        spark.stop()

     
    
