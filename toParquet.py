from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import os
import shutil

## Converting CSV and JSON files to Parquet format and reading Parquet files.

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


def delete_directory(path):
    """
    Deletes a directory and all its contents if it exists.
    """
    if os.path.exists(path):
        try:
            # Remove read-only flag if it exists
            os.chmod(path, 0o777)
        
            # Remove the directory and all its contents
            shutil.rmtree(path)
            print(f"Successfully deleted existing directory: {path}")
        except Exception as e:
            print(f"Error deleting directory: {e}")
    else:
        print(f"Directory does not exist: {path}")


# deletes parquet_formats folder
delete_directory('/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/parquet_formats')



# from_csv_to_parquet('/Users/opendata-rent/',
#                    '/Users/output/opendata-rent')

from_csv_to_parquet('/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/opendata-rent', '/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/parquet_formats/opendata-rent')

# from_json_to_parquet('/Users/idealista/',
#                     '/Users/idealista')

from_json_to_parquet('/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/idealista', '/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/parquet_formats/idealista')



from_csv_to_parquet('/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/lookup_tables', '/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/parquet_formats/lookup_tables')
from_csv_to_parquet('/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/opendatabcn-income', '/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/parquet_formats/opendatabcn-income')





# Taking randomly one file to test function

## read_parquet_file('/Users/output/idealista/filename=file%3A%2F%2F%2FUsers%2Fximenamoure%2FDesktop%2Fdata%2Fidealista%2F2020_01_08_idealista.json/part-00005-5b90cb7e-30d8-41f3-8043-2825ee3f9b00.c000.snappy.parquet')

## read_parquet_file('/Users/miona.dimic/Desktop/MDS/Q2 2023/BDM/Project/BDM_P1/data/parquet_formats/idealista/filename=file%3A%2F%2F%2FUsers%2Fmiona.dimic%2FDesktop%2FMDS%2FQ2%25202023%2FBDM%2FProject%2FBDM_P1%2Fdata%2Fidealista%2F2021_01_13_idealista.json')