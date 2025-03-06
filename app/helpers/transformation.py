from fastapi import FastAPI, UploadFile, File, HTTPException
import os
import shutil
import tempfile
import zipfile
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient
from app.helpers.ADLSGen2 import ADLSGen2
from app.constants.transformation import *
from app.settings import *
from app.helpers.common import create_directory_if_not_exists
from datetime import datetime
import nbformat as nbf
from app.logger import logger


def indent_generator(indent_level):
    """
    This function writes a multiline string with indentation to a file.

    Args:
        filename: The path to the file where the text should be written.
        multiline_text: The text content as a multiline string.
        indent_level: The number of indentation levels (spaces) for each line.
    """
    indent = " " * indent_level * 4  # Use 4 spaces per indentation level
    return indent

# Function to upload a file to ADLS Gen2
def upload_file_to_adls(file_system_client, local_file_path, adls_target_path):
    file_name = os.path.basename(local_file_path)
    file_client = file_system_client.get_directory_client(adls_target_path).create_file(file_name)
    
    with open(local_file_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

def upload_to_blob_storage(unzipFilePath: str, recon_name: str, date: str):
    try:
        # Upload the zip file to ADLSGen2
        datalake_service = ADLSGen2()
        service_client = datalake_service.connect()
        file_system_client = service_client.get_file_system_client(file_system=AZURE_BLOB_CONTAINER_NAME)
        adls_directory_path = f"r2r/uploadFiles/{recon_name}/input/{date}"

        # Check if the directory exists
        try:
            directory_client = file_system_client.get_directory_client(adls_directory_path)
            directory_client.get_directory_properties()  # This will raise an exception if the directory doesn't exist
            # List and delete all files and subdirectories
            paths = directory_client.get_paths(recursive=True)
            for path in paths:
                file_client = file_system_client.get_file_client(path.name)
                file_client.delete_file()
        except Exception as e:
            # Directory does not exist, so create it
            directory_client = file_system_client.create_directory(adls_directory_path)
            logger.info(f"Directory '{adls_directory_path}' created in ADLSGen2.")

        # Traverse the local folder and upload files
        for root, dirs, files in os.walk(unzipFilePath):
            for file in files:
                if file != f'{date}.zip':
                    local_file_path = os.path.join(root, file)
                    # relative_path = os.path.relpath(local_file_path, unzipFilePath)
                    # adls_target_path = os.path.join(adls_directory_path, os.path.dirname(relative_path)).replace("\\", "/")
                    
                    # Ensure the directory exists in ADLS Gen2
                    directory_client = file_system_client.get_directory_client(adls_directory_path)
                    try:
                        directory_client.get_directory_properties()  # Check if directory exists
                    except Exception:
                        directory_client.create_directory()  # Create directory if it doesn't exist

                    # Upload the file
                    upload_file_to_adls(file_system_client, local_file_path, adls_directory_path)

        logger.info("Files uploaded to ADLS Storage")
        return adls_directory_path, True
    
    except Exception as e:
        return f"Failed to upload due to {str(e)}", False

    finally:
        # Clean up temporary directory
        shutil.rmtree(unzipFilePath)
        pass

def getAllFilesFromSFTPforRun(sftp, client, recon_id, file_path_on_sftp, filename):
    try:
        # Create a temporary directory
        temp_dir =  tempfile.mkdtemp()
        local_file_path = os.path.join(temp_dir, client, recon_id, filename)
        # local_file_path = os.path.join(temp_dir, client, recon_id, 'file.zip')
        local_path = os.path.dirname(local_file_path)
        create_directory_if_not_exists(local_path)
        # Download the zip file
        sftp.get(file_path_on_sftp, local_file_path)
        
        logger.info(f'File downloaded to {local_file_path}')

        # Unzip the file
        with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
            zip_ref.extractall(local_path)
            logger.info(f'File unzipped in {local_path}')

        return local_path
    
    except Exception as e:
        logger.info(f"error in helper class getAllFilesFromSFTPforRun : {str(e)}")
        return None

def getDate(date_input: str | None = None):
    try:
        if date_input:
            date_obj = datetime.strptime(date_input.date_str, '%Y-%m-%d')
            formatted_date = date_obj.strftime('%b-%Y')
        else:
            # Get the current date
            current_date = datetime.now()
            # Format the date as "MMM-YYYY"
            formatted_date = current_date.strftime('%b-%Y')

        return formatted_date
    except Exception as e:
        logger.info(f"error in helper transformation.getDate: {str(e)}")
        return None
    
def getFilePath(directory,
                contains=None,
                startsWith=None,
                endsWith=None,
                equals=None):
    try:
        matching_files = []
    
        for root, dirs, files in os.walk(directory):
            for filename in files:
                if contains and contains in filename:
                    matching_files.append(os.path.join(root, filename))
                elif startsWith and filename.startswith(startsWith):
                    matching_files.append(os.path.join(root, filename))
                elif endsWith and filename.endswith(endsWith):
                    matching_files.append(os.path.join(root, filename))
                elif equals and filename == equals:
                    matching_files.append(os.path.join(root, filename))
        
        # logger.info(matching_files)
        if len(matching_files) > 0:
            return matching_files[0]
        else:
            return None
    except Exception as e:
        logger.info(f"error in helpers getFilePath: {str(e)}")
        return None
    
def getFilePathOnADLS(directory,
                      contains=None,
                      startsWith=None,
                      endsWith=None,
                      equals=None):
    try:
        matching_files = []
        datalake_service = ADLSGen2()
        service_client = datalake_service.connect()

        # Get the file system client (which is equivalent to a container in ADLS Gen2)
        file_system_client = service_client.get_file_system_client(file_system=AZURE_BLOB_CONTAINER_NAME)

        # Walk through the directory and its subdirectories
        paths = file_system_client.get_paths(path=directory, recursive=True)

        for path in paths:
            filename = path.name.split('/')[-1]  # Extract the filename
            if filename.startswith("._") or filename.startswith("_") or filename.startswith("_MACOSX"):
                continue
            elif contains and contains in filename:
                matching_files.append(path.name)
            elif startsWith and filename.startswith(startsWith):
                matching_files.append(path.name)
            elif endsWith and filename.endswith(endsWith):
                matching_files.append(path.name)
            elif equals and filename == equals:
                matching_files.append(path.name)

        if len(matching_files) > 0:
            return matching_files[0]
        else:
            return None

    except Exception as e:
        pass

def zip_files_transformation(temp_dir, file_paths, date_format, name):
    zip_file_path = os.path.join(temp_dir, f"{name}_{date_format}_output.zip")
    
    # Create a ZipFile object and add each file to it
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for file_path in file_paths:
            # Add each file to the zip file, keeping its base name
            zipf.write(file_path, arcname=os.path.basename(file_path))
    
    # Return the path to the zipped file and the TemporaryDirectory object
    # logger.info(zip_file_path)
    return zip_file_path