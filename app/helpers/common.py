import uuid
from pathlib import Path
from app.settings import DATA_DIR
from datetime import datetime
import os
import zipfile
from fastapi import Request, HTTPException
from app.helpers.iw_utils import *

def sync_dir_structure(root_dir, dir_structure):
    dirs = dir_structure.split("/")
    current_dir = Path(root_dir)
    for dir_name in dirs:
        current_dir = current_dir / dir_name
        current_dir.mkdir(parents=True, exist_ok=True)

def create_directory_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

async def get_file_extension(file):
    filename = file.filename
    file_extension = Path(filename).suffix
    return file_extension


def generate_uuid():
    return uuid.uuid4()

def get_base_url():
    return "https://r2r-service.1gqz9hzpmoi9.eu-gb.codeengine.appdomain.cloud"


def get_current_timestamp_iso_format():
    current_datetime = datetime.now()
    iso_format = current_datetime.isoformat()
    return iso_format


def zip_directory(directory_path, output_zip_file):
    # Create a ZipFile object
    with zipfile.ZipFile(output_zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk the directory tree
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                # Create the full filepath by joining root and file
                full_path = os.path.join(root, file)
                # Write the file to the zip archive, using a relative path
                relative_path = os.path.relpath(full_path, directory_path)
                zipf.write(full_path, relative_path)

def validate_authorization_header(request: Request):
    authorization: str = request.headers.get("Authorization")
    if not authorization:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    requester_id = auth_validate(authorization)
    if not requester_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    return requester_id

def get_pg_id(request: Request):
    authorization: str = request.headers.get("Authorization")
    if not authorization:
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    programId = get_program_id(authorization)
    return programId