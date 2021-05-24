import os
import boto3
import paramiko
from time import sleep


HOST = "X.XX.XXXXXX"
USER = "XXXXXXXX"
INSTANCE_ID = "XXXXXXXXXX"
S3_BUCKET = "BUCKET_NAME"
BUCKET_KEY = "BUCKET/key.pem"
PKEY_FILE = "/tmp/key.pem"
COMMANDS = [
    "echo 'ssh to ec2 instance successful'",
    "ls -a"
]


def execute_cmds(ssh=None, cmds=None):
    for cmd in cmds:
        stdin, stdout, stderr = ssh.exec_command(cmd)
        stdout_reader(stdout)
        print(stdout.read().decode('utf-8').strip("\n"))


def ssh_connect_ec2():
    privkey = paramiko.RSAKey.from_private_key_file(PKEY_FILE)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=HOST, username=USER, pkey=privkey)
    return ssh


def fetch_pkey():
    """Download private key file from secure S3 bucket"""
    s3_client = boto3.client('s3')
    s3_client.download_file(S3_BUCKET, BUCKET_KEY, PKEY_FILE)
    pkey_filename = PKEY_FILE.replace("/tmp/", "")
    if os.path.isfile(PKEY_FILE):
        return print(f"{pkey_filename} successfully downloaded from {S3_BUCKET}")


def main():
    privkey = fetch_pkey()
    ssh = ssh_connect_ec2()
    execute_cmds(ssh, COMMANDS)
    ssh.close()
    instance.stop()
    return print("Script execution completed. See Cloudwatch logs for complete output")


def worker_handler(event, context):
    main()
