from hdfs import InsecureClient
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import io
import os
import datetime

# HDFS paths
temp_dir_name = '/user/bdm/temporal_landing'
perm_dir_name = '/user/bdm/persistent_landing'


# Connect to HDFS using InsecureClient
hdfs_cli = InsecureClient('http://10.4.41.48:9870', user='bdm')

def getTimestamp():
    return int(datetime.datetime.utcnow().timestamp())

def delete_hdfs_directory(dir_name):
    if not hdfs_cli.status(dir_name, strict=False):
        print(f"The folder {dir_name} does not exist.")
        return
    for root, dirs, files in hdfs_cli.walk(dir_name):
        for file in files:
            hdfs_cli.delete(os.path.join(root, file))
    hdfs_cli.delete(dir_name, recursive=True)
    print(f"The folder {dir_name} was deleted.")



delete_hdfs_directory(perm_dir_name)
print("Creating persistant_landing.....")

# Get list of subdirectories in temporary HDFS directory
subdirs = [f"{temp_dir_name}/{name}" for name in hdfs_cli.list(temp_dir_name) if hdfs_cli.status(f"{temp_dir_name}/{name}")['type'] == 'DIRECTORY']
# Loop over subdirectories and convert files to Parquet
for subdir in subdirs:
    # Get list of files in subdirectory
    timestamp = getTimestamp()
    files = [f"{subdir}/{name}" for name in hdfs_cli.list(subdir)]
    
    # Create corresponding subdirectory in new empty HDFS directory
    perm_subdir = f"{perm_dir_name}/{subdir[len(temp_dir_name):]}"
    hdfs_cli.makedirs(perm_subdir, permission=777)

    for file in files:
        # Check if file is in json format
        if file.endswith('.json'):
            # Read data from file
            with hdfs_cli.read(file) as reader:
                json_data = pd.read_json(io.BytesIO(reader.read()), orient='records')
            
            # Convert data to Parquet format
            table = pa.Table.from_pandas(json_data)
            output_file = f"{perm_subdir}/{os.path.basename(file).replace('.json', '')}_{timestamp}.parquet"
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer, compression='snappy', use_dictionary=True, version='2.6')
            
            # Write Parquet file buffer to HDFS
            with hdfs_cli.write(output_file) as writer:
                writer.write(buffer.getvalue())
    
        elif file.endswith('.csv'):
            # Read data from CSV file
            with hdfs_cli.read(file) as reader:
                csv_data = pd.read_csv(io.BytesIO(reader.read()))
            
            # Convert data to Parquet format
            table = pa.Table.from_pandas(csv_data)
            output_file = f"{perm_subdir}/{os.path.basename(file).replace('.csv', '')}_{timestamp}.parquet"
            buffer = pa.BufferOutputStream()
            pq.write_table(table, buffer, compression='snappy', use_dictionary=True, version='2.6')
            
            # Write Parquet file buffer to HDFS
            with hdfs_cli.write(output_file) as writer:
                writer.write(buffer.getvalue())

print("\nPersistant zone has been successfully populated.")




