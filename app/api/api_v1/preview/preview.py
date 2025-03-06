from fastapi.responses import JSONResponse
from typing import Optional, Dict
import os
from enum import Enum
from pydantic import BaseModel
import json
from datetime import datetime
import logging
from io import BytesIO
from fastapi import APIRouter, Depends
from app.services.preview.preview import PreviewService
from app.services.recon.recon_mongo import DocumentService
from app.settings import *
from fastapi.security import APIKeyHeader, api_key
from app.helpers.recon import *
from app.helpers.preview import *
from app.helpers.common import *
from app.constants.mongodb import *
from app.logger import logger
import gc

header_scheme = APIKeyHeader(name="Authorization")

class MyException(Exception):
    def __init__(self, message):
        super().__init__(message)

router = APIRouter()

@router.get("/home")
async def home(authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    return PreviewService.home()

@router.get('/get-sftp-path')
def getSftpPath(client: str, reconciliationId: str, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        sftp_schema = MONGODB_CLIENT_DETAILS_COLLECTION
        document_service = DocumentService(db_name = db_name)
        sftp_details = document_service.get_document(collection_name=sftp_schema, document_id=client)
        recon_details = document_service.get_document(collection_name=MONGODB_CLIENT_RECONS_COLLECTION, document_id=reconciliationId)

        if sftp_details:
            if sftp_details['key_file']:
                sftp_details['password'] = None
            else:
                sftp_details['key_file'] = None

        sftp, transport = get_sftp(hostname=sftp_details['hostname'],
                        username=sftp_details['username'],
                        password=sftp_details['password'],
                        port=int(sftp_details['port']),
                        key_file=sftp_details['key_file'])
        previewFilePath = recon_details['previewFilePath']
        files = list_files_recursively(sftp=sftp, remote_path=previewFilePath)

        for i in range(len(files)):
            files[i] = files[i].replace(f'{previewFilePath}/', '')
        return JSONResponse(status_code=200,
                            content={'files': files})
    except Exception as e:
        logger.info(f"Error in controller getSftpPath: {str(e)}")
    finally:
        sftp.close()
        transport.close()

class GetPreviewInput(BaseModel):
    client: str
    reconciliationId: str
    transformation_description: Dict

@router.post("/get-cell-output")
async def getPreview(preview_payload: GetPreviewInput, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        sftp_schema = MONGODB_CLIENT_DETAILS_COLLECTION
        document_service = DocumentService(db_name = db_name)
        sftp_details = document_service.get_document(collection_name=sftp_schema, document_id=preview_payload.client)
        recon_details = document_service.get_document(collection_name=MONGODB_CLIENT_RECONS_COLLECTION, document_id=preview_payload.reconciliationId)
        
        # recon_name = document['reconciliationName']
        transformation_description = preview_payload.transformation_description
        if sftp_details:
            if sftp_details['key_file']:
                sftp_details['password'] = None
            else:
                sftp_details['key_file'] = None

        sftp, transport = get_sftp(hostname=sftp_details['hostname'],
                        username=sftp_details['username'],
                        password=sftp_details['password'],
                        port=int(sftp_details['port']),
                        key_file=sftp_details['key_file'])
        
        # sftp = sftp, sftp_details = sftp_details, document = recon_details, 
        output, status = PreviewService.generateCellAndExecute(sftp = sftp,
                                                               document = recon_details, 
                                                               transformation_description=transformation_description,
                                                               client=preview_payload.client,
                                                               recon_id=preview_payload.reconciliationId,
                                                               requester_id=requester_id)
        if status:
            if len(output) == 0:
                list_output = [{col: None for col in output.columns}]
                return {"output": list_output}
            
            output = output.head(100)
            
            json_data = output.to_json(orient = 'records')
            # Convert the JSON string list to a list of Python dictionaries
            # json_dict_list = [json.loads(item) for item in json_data]
            # Return the list of dictionaries as a JSON response
            return {'output': json.loads(json_data)}
        else:
            return {'output': [
                            {"Error": output}
                            ]
                    }
    except Exception as e:
        return {'output': [
                            {"Error": str(e)}
                            ]
                }
    
    finally:
        sftp.close()
        transport.close()
    
class ReadFilesPayload(BaseModel):
    fileConfigData: list
    client: str
    reconcilitationId: str

@router.post("/read-all-files")    
def readAllFiles(payload: ReadFilesPayload, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        sftp_schema = MONGODB_CLIENT_DETAILS_COLLECTION
        document_service = DocumentService(db_name = db_name)
        logger.info("Geeting SFTP Details")
        sftp_details = document_service.get_document(collection_name=sftp_schema, document_id=payload.client)
        logger.info("Done Geeting SFTP Details")
        logger.info("Geeting recon template Details")
        recon_details = document_service.get_document(collection_name=MONGODB_CLIENT_RECONS_COLLECTION, document_id=payload.reconcilitationId)
        logger.info("Done Geeting recon template Details")
        fileConfigData = payload.fileConfigData
        logger.info(fileConfigData)

        if sftp_details:
            if sftp_details['key_file']:
                sftp_details['password'] = None
            else:
                sftp_details['key_file'] = None

            sftp, transport = get_sftp(hostname=sftp_details['hostname'],
                            username=sftp_details['username'],
                            password=sftp_details['password'],
                            port=int(sftp_details['port']),
                            key_file=sftp_details['key_file'])
            
            if 'previewTempFilesFolder' in recon_details.keys():
                if os.path.isdir(recon_details['previewTempFilesFolder']):
                    shutil.rmtree(recon_details['previewTempFilesFolder'])
            logger.info("reading Preview File Path from recon_details")
            previewFilePath = recon_details['previewFilePath']

            for fileConfig in fileConfigData:
                fileConfig['sftpFilePath'] = os.path.join(previewFilePath, fileConfig['fileName'])

            temp_dir = getAllFilesfromSFTP(sftp = sftp, fileConfigData = fileConfigData)
            output = PreviewService.readDfs(temp_dir, fileConfigData, client = payload.client, recon_id = payload.reconcilitationId, requester_id = requester_id)
            extra_payload = {'previewTempFilesFolder': temp_dir}
            document_service.create_or_update(collection_name=payload.client,id=payload.reconcilitationId,recon_payload=extra_payload, current_user='user1')
            return {'status': output}

    except Exception as e:
        logger.error(f"Error in Controller - read all files: {str(e)}")
        sftp.close()
        transport.close()

    finally:
        shutil.rmtree(temp_dir)
        pass

class deletePreviewPayload(BaseModel):
    client: str
    reconcilitationId: str

@router.post("/delete-preview")
def delete_preview(payload: deletePreviewPayload,
                   authorization_token: api_key = Depends(header_scheme), 
                   requester_id: bool = Depends(validate_authorization_header),
                   pg_id: bool = Depends(get_pg_id)):
    try:
        status = PreviewService.deletePreview(payload = payload, requester_id = requester_id)
        if status:
            return {'status_code': 200,
                    'output': "Success"}
        else:
            return {
                'status_code': 200,
                'output': 'Failed'
            }
    except Exception as e:
        logger.error(str(e))
        return {
            'status_code': 200,
            'output': str(e)
        }