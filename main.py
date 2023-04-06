import paramiko
from dotenv import load_dotenv
import os

## Transferring files from a local machine to a remote server over SSH


load_dotenv()
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(os.getenv('MACHINE_IP'), username=os.getenv('USER_NAME'), password=os.getenv('PASSWORD'))
# stdin, stdout, stderr = ssh.exec_command('ls')

local_folders = os.getenv('LOCAL_DATA_SOURCES').split(',')
print("Local folders : "  + str(local_folders))
sftp = ssh.open_sftp()

for i in local_folders:
    folder_path = os.getenv('MACHINE_DATA_PATH')+i
    stdin, stdout, stderr = ssh.exec_command(f'mkdir -p {folder_path}')
for i in local_folders:
    for filename in os.listdir(os.getenv('LOCAL_DATA_PATH')+i):
        # print (os.getenv('LOCAL_DATA_PATH')+i+'/'+filename , os.getenv('MACHINE_DATA_PATH')+i+'/'+filename)
        sftp.put(os.getenv('LOCAL_DATA_PATH')+i+'/'+filename,os.getenv('MACHINE_DATA_PATH')+i+'/'+filename)
sftp.close()
ssh.close()