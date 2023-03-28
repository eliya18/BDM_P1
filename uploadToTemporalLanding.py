from hdfs import InsecureClient
import os
import posixpath as psp

hdfs_cli = InsecureClient('http://10.4.41.48:9870', user='bdm')


# just testing to list all files in a directory
def local_files(dir_name):
    all_files = []
    for root, dirs, files in os.walk(dir_name):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
    print("length", len(all_files))
    print(all_files)
    return all_files


# just testing to look what directories are in hdfs
def hdfs_files(dir_name):
    if hdfs_cli.status(dir_name, strict=False):
        print('Directory already exists, so continue')
    else:
        print(f"The folder {dir_name} does not exist.")
        hdfs_cli.makedirs(dir_name)
        print(f"The folder {dir_name} was created.")
    f_paths = [psp.join(dpath, f_name)
               for dpath, _, f_names in hdfs_cli.walk(dir_name)
               for f_name in f_names]
    print(f_paths)
    return f_paths


def progress_callback(file_name, bytes_uploaded):
    if bytes_uploaded == -1:
        print(f"Finished uploading file {file_name}")
    else:
        print(f"Uploaded {bytes_uploaded} for file {file_name}")


# this uploads all the stuff in the local folder (idealista, lookup tables, opendata) to a folder called
# temporal_landing in hdfs
# first parameter is the path in the virtual machine for hdfs and
# the second on is the local folder where all the data is
hdfs_cli.upload('/user/bdm/temporal_landing', '/Users/Desktop/data/', progress=progress_callback,
                overwrite=True)

hdfs_files('/user/bdm/temporal_landing')
# local_files('/Users/Desktop/data/')
