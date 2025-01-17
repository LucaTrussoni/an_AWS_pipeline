# SCRIPT to import all the .CSV files in the current directory
# the credentials are stored in the user.key file
# (in csv format as it is downloaded from AWS)
import boto3
import os
from typing import Dict

def read_keys(filename: str)-> Dict[str, str]:
    """
        Function read_keys(filename)
        The function retrieves credential from a csv file with access keys
        from amazon aws

        Input:
        filename: string with the filename
        
        Returns:
        a dictionary containing the keys in the filr

        Exception:
        file not found exception if the file does not exists
    """
    if os.path.exists(filename):
        with open(filename, mode='r') as file:
            file.readline()
            keyline=file.readline().strip().split(',')
    else:
        raise FileNotFoundError(f"No '{filename}' was found.")
    return {"AccessKeyID":keyline[0],"SecretKey":keyline[1]}

def upload_csv_files(path: str,region: str, bucket: str, keys: Dict)-> None:
    """
        upload_csv_files(path,region,bucket)
        This function uploads all csv files in the current directory

        Input
        path: where to look for csv files, string
        region: the region of the bucket, string
        bucket: the name of the bucket, string

        Returns
        None
    """
    files=os.listdir(path)
    csv_files=[]
    for filename in files:
        if filename.endswith(".csv"):
            csv_files.append(filename)
    print("Preparing to upload:")
    for filename in csv_files:
        print(filename)
    s3_client=boto3.client("s3",
                           region_name=region,
                           aws_access_key_id=keys["AccessKeyID"],
                           aws_secret_access_key=keys["SecretKey"])
    for filename in csv_files:
        print(f"Uploading {filename}...")
        s3_client.upload_file(filename,bucket,filename)
        print("Uploaded")

if __name__ == "__main__":
    # Code in this block runs only when the script is executed directly
    my_keys=read_keys("user.key")
    current_dir=os.getcwd()
    upload_csv_files(current_dir,"eu-central-1","YOURPREFIX.progettoaws.raw",my_keys)