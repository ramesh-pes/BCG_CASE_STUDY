"""
Module containing helper function for use with Apache Spark
"""


def read_csv(spark, csv):
    """Load data from CSV file format to dataframe.
    :param spark: Spark session object.
    :param file_path: Path to input CSV file.
    :return: Spark DataFrame.
    """
    df =spark\
        .read\
        .format("csv")\
        .option("header",True)\
            .option("inferSchema",True)\
        .load(csv)
    return df

def write_output(df,output):
    """Write the contents of dataframe to parquet files.
    :param df: Spark dataframe.
    :param output: Path to output file
    """
    df.write.format('csv').mode('overwrite').option("header", "true").save(output)