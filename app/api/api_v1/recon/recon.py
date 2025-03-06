from fastapi import APIRouter, Path, Body, File, UploadFile, Form, HTTPException
from fastapi.responses import JSONResponse
from app.services.recon.recon_mongo import DocumentService
from pydantic import BaseModel
from typing import Optional
import os
import requests
from app.logger import logger
from fastapi.security import APIKeyHeader, api_key
from fastapi import APIRouter, Request, HTTPException, Depends, BackgroundTasks, Header
from app.helpers.iw_utils import *
from app.helpers.recon import *
from app.constants.mongodb import *
import shutil
import json
from app.helpers.common import *
from app.settings import *

header_scheme = APIKeyHeader(name="Authorization")

router = APIRouter()

class ReconPayload(BaseModel):
    client: Optional[str] = None
    reconciliationName: Optional[str] = None
    description: Optional[str] = None
    transformations: Optional[list] = None
    fileConfigData: Optional[list] = None
    fileData: Optional[list] = None
    name: Optional[str] = None
    client_id: Optional[str] = None

@router.get("/home")
async def home(authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    pg_id = pg_id
    db_name = get_tenant_program_space_name(pg_id)
    recon_service = DocumentService(db_name = db_name)
    return recon_service.home()

@router.get("/test")
async def test(authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_RECONS_COLLECTION
        current_user = requester_id
        recon_name = "test_recon"
        recon_service = DocumentService(db_name = db_name)
        return recon_service.get_transformation_recon_by_name(collection_name, recon_name)
    except Exception as e:
        return {"status": False, "error": "controller-test - catch: " + str(e)}
    
@router.get("/get_documents")
async def get_documents(client: str, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_RECONS_COLLECTION
        recon_service = DocumentService(db_name = db_name)
        return recon_service.get_documents(collection_name, client_id=client)
    except Exception as e:
        return {"status": False, "error": f"controller-get_documents - catch: {str(e)}"}

@router.get("/get-ui-link")
async def get_ui_link(authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header)):
    return_data = {
        "message": "Data transformation UI configuration portal link",
        "link": "https://r2r-ui-r2r.poc-tok-1-bx2-4x16-1d4ccc0775a4ed7596c037528d22e24b-0000.jp-tok.containers.appdomain.cloud/"
    }
    return return_data
    
@router.get("/get_sftps")
async def get_sftps(authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        # print(db_name)
        collection_name = MONGODB_CLIENT_DETAILS_COLLECTION
        recon_service = DocumentService(db_name = db_name)
        return recon_service.get_documents(collection_name)
    except Exception as e:
        return {"status": False, "error": f"controller-get_sftps - catch: {str(e)}"}

@router.get("/get_sftp/{id}")
async def get_sftp_by_id(id: str, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_DETAILS_COLLECTION
        recon_service = DocumentService(db_name = db_name)
        data = recon_service.get_document(collection_name, id)
        return_response = {"status": True,
                           "message": "Document fetch Successful !!", 
                           "data": data}
        return return_response
    
    except Exception as e:
        return {"status": False, "error": f"controller-get_sftps - catch: {str(e)}"}

@router.get("/get-sftp-by-name/{name}")
async def get_sftp_by_name(name: str, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_DETAILS_COLLECTION
        clientName = name
        recon_service = DocumentService(db_name = db_name)
        response = recon_service.get_sftp_by_name(collection_name, clientName)
        return response
    except Exception as e:
        return {"status": False, "error": f"controller-get_document - catch: {str(e)}"}

@router.get("/get_documents_by_id/{recon_id}")
async def get_document(client: str, recon_id: str, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_RECONS_COLLECTION
        recon_service = DocumentService(db_name = db_name)
        response = recon_service.get_document(collection_name, recon_id)
        return response
    except Exception as e:
        return {"status": False, "error": f"controller-get_document - catch: {str(e)}"}

@router.post("/create-or-update-recons")
async def create_or_update(recon_payload: ReconPayload, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        recon_payload.name = recon_payload.reconciliationName
        recon_payload.client_id = recon_payload.client
        collection_name = MONGODB_CLIENT_RECONS_COLLECTION
        current_user = requester_id
        recon_service = DocumentService(db_name = db_name)
        return recon_service.create_or_update(current_user, collection_name, recon_payload)
    except Exception as e:
        return {"status": False, "error": f"controller-create_or_update - catch: {str(e)}"}

@router.post("/create-or-update-recons/{recon_id}")
async def create_or_update(recon_id: str, recon_payload: ReconPayload, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_RECONS_COLLECTION
        current_user = requester_id
        recon_service = DocumentService(db_name = db_name)
        return recon_service.create_or_update(current_user, collection_name, recon_payload, recon_id)
    except Exception as e:
        return {"status": False, "error": f"controller-create_or_update - catch: {str(e)}"}
    
class sftpPayload(BaseModel):
    clientName: Optional[str] = None
    hostname: Optional[str] = None
    port: Optional[str] = None
    username: Optional[str] = None
    inputfileLocation: Optional[str] = None
    outputfileLocation: Optional[str] = None
    password: Optional[str] = None
    key_file: Optional[str] = None
    name: Optional[str] = None

@router.post("/check_sftp_connection")
async def check_sftp_connection(payload: sftpPayload, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        if not payload.password and not payload.key_file:
            raise HTTPException(status_code=400, detail="Either password or key_file must be provided")
        else:
            pg_id = pg_id
            db_name = get_tenant_program_space_name(pg_id)
            recon_service = DocumentService(db_name = db_name)
            response = recon_service.checkSFTPConnection(payload.hostname, int(payload.port), payload.username, payload.password, payload.key_file)
            return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/create-or-update-client-sftp")
async def create_or_update_client(payload: sftpPayload, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection = MONGODB_CLIENT_DETAILS_COLLECTION
        payload.name = payload.clientName
        current_user = requester_id
        recon_service = DocumentService(db_name = db_name)
        return recon_service.create_or_update(current_user, collection, payload)
    except Exception as e:
        return JSONResponse(status_code = 500, content = {"error": f"An error occurred in controller in /create-or-update-client-sftp: {str(e)}"})

@router.post("/create-or-update-client-sftp/{id}")
async def create_or_update_client(id: str, payload: sftpPayload, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        collection_name = MONGODB_CLIENT_DETAILS_COLLECTION
        current_user = requester_id
        recon_service = DocumentService(db_name = db_name)
        return recon_service.create_or_update(current_user, collection_name, payload, id)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"An error occurred in controller in /create-or-update-client-sftp/id: {str(e)}"})
    
@router.post("/upload-zip")
async def upload_zip(reconciliationId: str = Form(...),client: str = Form(...), file: UploadFile = File(...), authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    if not file.filename.endswith('.zip'):
        raise HTTPException(status_code=400, detail="File is not a zip file")
    try:
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        document_service = DocumentService(db_name = db_name)
        response = document_service.getFileList(file)
        if response.status_code == requests.codes.ok:
            collection_name = MONGODB_CLIENT_DETAILS_COLLECTION
            sftp_details = document_service.get_document(collection_name=collection_name, document_id=client)
            recon_details = document_service.get_document(collection_name=MONGODB_CLIENT_RECONS_COLLECTION, document_id=reconciliationId)
            recon_name = recon_details['name']
            if sftp_details['key_file']:
                sftp, transport = get_sftp(hostname=sftp_details['hostname'],
                                port=int(sftp_details['port']),
                                username=sftp_details['username'],
                                key_file=sftp_details['key_file'].strip())
            else:
                sftp, transport = get_sftp(hostname=sftp_details['hostname'],
                                port=int(sftp_details['port']),
                                username=sftp_details['username'],
                                password=sftp_details['password'].strip())
            
            input_path, output_path = document_service.saveFilesForConfigRun(sftp=sftp, inputfileLocation=sftp_details['inputfileLocation'], outputfileLocation=sftp_details['outputfileLocation'])
            preview_file_path = document_service.save_and_extract_zip_to_sftp_1(sftp=sftp, file=file, remote_path=input_path, recon_name = recon_name)
            payload = {
                'fileData': json.loads(response.body.decode('utf-8')),
                'previewFilePath': preview_file_path,
                'previewOutputPath': output_path,
                'periodicFilePath': sftp_details['inputfileLocation']
            }

            current_user = requester_id
            document_service.create_or_update(collection_name=MONGODB_CLIENT_RECONS_COLLECTION, id=reconciliationId, recon_payload=payload, current_user=current_user)
            return response
        else:
            return JSONResponse(status_code=400, content={"error": f"An error occured: {str(e)}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"An error occured in Controller Service: {str(e)}"})
    finally:
        sftp.close()
        transport.close()

