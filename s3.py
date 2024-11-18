"""
Python utility function to work with Amazon S3

Provides functions for listing, downloading, uploading, and deleting
objects from Amazon S3.

Functions:
 - list_objects_s3
 - upload_object_s3
 - download_object_s3
 - delete_object_s3

References:
 - Boto3 Official Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html
 - Amazon S3 examples: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html
"""

import os
import warnings
import boto3
from boto3.s3.transfer import TransferConfig

# Ignore warnings (useful for suppressing unnecessary warnings from Boto3)
warnings.filterwarnings("ignore")


def list_objects_s3(bucket_name: str, prefix: str = "", max_keys: int = 123):
    """
    List objects in an S3 bucket with a given prefix.

    The boto3 list_objects_v2 method paginates results if there are many objects.
    This function handles pagination to get all object keys.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - prefix (str, optional): A prefix to filter objects by. Default is an empty string, meaning no filtering.
    - max_keys (int, optional): The maximum number of object keys to retrieve per request. Default is 123.

    Returns:
    - List of object keys (str) in the specified S3 bucket with the given prefix.
    """
    # Create an S3 client to interact with AWS S3
    s3_client = boto3.client("s3")

    # Initialize the continuation token and list to hold object keys
    continuation_token = None
    object_keys = []

    try:
        while True:
            # Paginate results if necessary using ContinuationToken
            if continuation_token:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=max_keys,
                    ContinuationToken=continuation_token,
                )
            else:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=max_keys,
                )

            # Check if the response contains 'Contents' (object keys)
            if "Contents" in response:
                # Add object keys to the list
                object_keys.extend(d["Key"] for d in response["Contents"])

            # Get the continuation token for pagination if more objects exist
            continuation_token = response.get("NextContinuationToken")
            if not continuation_token:
                # Exit the loop if no more objects are left
                break

    except Exception as e:
        print(f"Error listing objects: {e}")

    return object_keys


def upload_object_s3(bucket_name: str, file_path: str, object_name: str):
    """
    Upload an object (file) to an S3 bucket.

    This function uploads a file to an S3 bucket. It uses multipart uploads for large files (over 8MB)
    to ensure the upload is efficient and can handle larger files without running into memory issues.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - file_path (str): The local path to the file to be uploaded.
    - object_name (str): The name of the object in the S3 bucket (including any prefix or folder structure).

    Returns:
    - None: Prints a success message or an error message based on the result of the upload.
    """
    # Create an S3 client to interact with AWS S3
    s3_client = boto3.client("s3")

    # Set up transfer configuration to use multipart uploads for large files
    config = TransferConfig(
        # File size threshold (8MB) for multipart upload (files larger than this will be uploaded in parts)
        multipart_threshold=8 * 1024 * 1024,
        # Chunk size per part for multipart upload (8MB)
        multipart_chunksize=8 * 1024 * 1024,
    )

    try:
        # Inform the user about the file being uploaded
        print(f"Uploading {file_path} to S3 bucket {bucket_name} as {object_name}...")

        # Perform the upload using the boto3 upload_file method with the configured transfer settings
        response = s3_client.upload_file(
            file_path, bucket_name, object_name, Config=config
        )

        # Print a success message upon successful upload
        print(f"File {file_path} uploaded successfully to {bucket_name}/{object_name}")
        print(f"Response: {response}")

    except Exception as e:
        print(f"Error uploading file: {e}")


def download_object_s3(bucket_name: str, object_name: str, download_path: str):
    """
    Download an object from an S3 bucket to a local path.

    This function downloads a specific object from an S3 bucket and saves it to a given local path.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - object_name (str): The name of the object in the S3 bucket to be downloaded.
    - download_path (str): The local path where the file will be saved.

    Returns:
    - None: Prints a success message or an error message based on the result of the download.
    """
    # Create an S3 client to interact with AWS S3
    s3_client = boto3.client("s3")

    try:
        # Inform the user about the file being downloaded
        print(f"Downloading {object_name} from bucket {bucket_name} to {download_path}...")

        # Perform the download using boto3's download_file method
        s3_client.download_file(bucket_name, object_name, download_path)

        # Print a success message when the download is complete
        print(f"File downloaded successfully to: {download_path}")

    except Exception as e:
        print(f"Exception downloading object: {e}")


def delete_object_s3(bucket_name: str, object_name: str):
    """
    Delete an object from an S3 bucket.

    This function deletes a specific object from an S3 bucket by its object key.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - object_name (str): The name of the object in the S3 bucket to be deleted.

    Returns:
    - None: Prints a success message or an error message based on the result of the deletion.
    """
    # Create an S3 client to interact with AWS S3
    s3_client = boto3.client("s3")

    try:
        # Inform the user about the object being deleted
        print(f"Attempting to delete {object_name} from bucket {bucket_name}...")

        # Perform the deletion using boto3's delete_object method
        s3_client.delete_object(Bucket=bucket_name, Key=object_name)

        # Print a success message upon successful deletion
        print(f"Object {object_name} deleted successfully from bucket {bucket_name}.")

    except Exception as e:
        print(f"Exception deleting object: {e}")


# Main function for testing the script
if __name__ == "__main__":
    # Replace with your actual S3 bucket name
    bucket_name = "*****"

    # #### Test list_objects_s3 function
    # Uncomment below to test listing objects with a prefix
    # prefix = "test-folder"
    # max_keys = 2
    # print(list_objects_s3(bucket_name, prefix, max_keys=max_keys))

    # #### Test upload_object_s3 function
    # Uncomment below to test uploading files
    # import joblib
    # import pandas as pd
    # import seaborn as sns

    # # Upload a small file
    # iris_df = sns.load_dataset("iris")
    # joblib.dump(iris_df, "iris.joblib")
    # file_path = "iris.joblib"
    # object_name = "new_test_folder_s3/iris.df"
    # upload_object_s3(bucket_name, file_path=file_path, object_name=object_name)

    # # Upload a large file using multipart upload
    # iris_large = pd.concat([iris_df] * 10000, ignore_index=True)
    # joblib.dump(iris_large, "iris_large.joblib")
    # file_size = os.path.getsize("iris_large.joblib")
    # print(f"File Size of iris_large.joblib: {file_size // 2**20} MB")
    # object_name = "new_test_folder_s3/iris_large.joblib"
    # file_path = "iris_large.joblib"
    # upload_object_s3(bucket_name, file_path=file_path, object_name=object_name)

    # #### Test download_object_s3 function
    # Uncomment below to test downloading an object
    # object_name = "new_test_folder_s3/iris_large.joblib"
    # download_path = "new_iris_large.joblib"
    # download_object_s3(bucket_name, object_name, download_path)

    # #### Test deleting an object
    object_name = "new_test_folder_s3/iris_large.joblib"  # Replace with an actual object key
    delete_object_s3(bucket_name, object_name)

    # List objects again after deletion to confirm it was deleted
    print(list_objects_s3(bucket_name, prefix=""))
