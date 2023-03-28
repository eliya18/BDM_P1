from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name


def from_csv_to_parquet(folder, output_folder):
    spark = SparkSession.builder.appName("Transform CSV to Parquet").getOrCreate()
    # Load the CSV files into a DataFrame
    # Add a new column to the DataFrame containing the input file name
    df = spark.read.csv(folder, header=True, inferSchema=True).withColumn('filename', input_file_name())
    # Write the DataFrame to a Parquet file
    df.write.partitionBy('filename').parquet(output_folder)
    spark.stop()


def from_json_to_parquet(folder, output_folder):
    spark = SparkSession.builder.appName("Transform JSON to Parquet").getOrCreate()
    # Load the Json files into a DataFrame
    # Add a new column to the DataFrame containing the input file name
    df = spark.read.json(folder).withColumn('filename', input_file_name())
    # Write the DataFrame to a Parquet file
    df.write.partitionBy('filename').parquet(output_folder)
    spark.stop()


def read_parquet_file(file):
    spark = SparkSession.builder.appName("Read files").getOrCreate()
    parquet_df = spark.read.parquet(file)
    parquet_df.show()
    spark.stop()


from_csv_to_parquet('/Users/opendata-rent/',
                   '/Users/output/opendata-rent')

from_json_to_parquet('/Users/idealista/',
                    '/Users/idealista')
read_parquet_file('/Users/output/idealista/filename=file%3A%2F%2F%2FUsers%2Fximenamoure%2FDesktop%2Fdata%2Fidealista%2F2020_01_08_idealista.json/part-00005-5b90cb7e-30d8-41f3-8043-2825ee3f9b00.c000.snappy.parquet')
