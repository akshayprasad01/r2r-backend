from fastapi.responses import JSONResponse
import os
from pydantic import BaseModel
from datetime import datetime
from typing import Optional
import sys
from fastapi import APIRouter, Depends
from app.services.databricks.databricks import DatabricksService
from app.constants.transformation import *
from app.services.transformation.transformation import TransformationService
from app.services.jobs.jobs import JobsService
from app.services.recon.recon_mongo import DocumentService
from app.settings import *
import shutil
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from app.helpers.common import *
from app.helpers.transformation import *
from app.helpers.recon import *
from app.helpers.preview import *
from fastapi.security import APIKeyHeader, api_key
from app.constants.mongodb import *
from app.logger import logger

header_scheme = APIKeyHeader(name="Authorization")

# Initialize Azure Blob client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_BLOB_CONTAINER_NAME)

class MyException(Exception):
    def __init__(self, message):
        super().__init__(message)

router = APIRouter()


@router.get("/home")
async def home(authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    return TransformationService.home()

@router.post("/execute")
async def transform_execute(client: str, reconciliationId: str, authorization_token: api_key = Depends(header_scheme), requester_id: bool = Depends(validate_authorization_header), pg_id: bool = Depends(get_pg_id)):
    try:        
        # upload_folder = DATA_DIR
        # upload_folder = TRANSFORMATION_DIR
        job_service  = JobsService()
        transformation_job_id = job_service.create_job(client_id = client, recon_id = reconciliationId)
        pg_id = pg_id
        db_name = get_tenant_program_space_name(pg_id)
        sftp_schema = MONGODB_CLIENT_DETAILS_COLLECTION
        document_service = DocumentService(db_name = db_name)
        sftp_details = document_service.get_document(collection_name=sftp_schema, document_id=client)
        logger.info(f"Transformation.py:  {sftp_details}")
        response, status = TransformationService.readConfig(db_name = db_name, client_id = client,reconciliationId = reconciliationId)
        logger.info(f"Transformation.py status:  {status}")
        logger.info(f"Transformation.py response:  {response}")

        log_base_path = TRANSFORMATION_LOGS_DIR
        create_directory_if_not_exists(log_base_path)
        create_directory_if_not_exists(BIN_DIR)

        if sftp_details:
            if sftp_details['key_file']:
                sftp_details['password'] = None
            else:
                sftp_details['key_file'] = None
        else:
            exit()


        if status:
            notebook_path = response
            # Here you can process the uploaded file based on the specified parameters
            # For demonstration, let's just return the filename and parameters

            databricks_service = DatabricksService()
            databricks_notebook_path, status = databricks_service.uploadPythonNotebook(notebook_path)
            if status:
                databricks_job_id = databricks_service.postNotebookJob(notebookPathWorkspace=databricks_notebook_path,
                                                            env_vars={
                                                                "TRANSFORMATION_JOB_ID": transformation_job_id,
                                                                "AZURE_STORAGE_ACCOUNT_KEY": os.getenv("AZURE_ACCOUNT_KEY"),
                                                                "AZURE_STORAGE_ACCOUNT_NAME": os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
                                                                "AZURE_BLOB_CONTAINER_NAME": os.getenv("AZURE_BLOB_CONTAINER_NAME"),
                                                                "SFTP_KEY_FILE": sftp_details['key_file'],
                                                                "SFTP_PASSWORD": sftp_details['password'],
                                                                "APPLICATION_CALLBACK_URL": os.getenv("APPLICATION_CALLBACK_URL")
                                                            })
                # logger.info(os.system(f"python3 {BIN_DIR}/{python_file} 2>&1 | tee {log_base_path}/{pythonfile}.log"))
                databricks_run_id = databricks_service.runJob(jobId=databricks_job_id)
                
                job_service.update_job(transformation_job_id = transformation_job_id,
                                       status = JOB_STATUS_INPROGRESS,
                                       databricks_job_id=databricks_job_id,
                                       databricks_run_id=databricks_run_id,
                                       databricks_job_status=JOB_STATUS_INPROGRESS)

            if sftp_details:
                if sftp_details['key_file']:
                    sftp_details['password'] = None
                else:
                    sftp_details['key_file'] = None

            return {
                "status":True,
                "reconciliationId": reconciliationId,
                "transformationJobId": transformation_job_id
                }
        else:
            raise MyException(response)

    except MyException as e:
        return JSONResponse(status_code = 503, content = {"error": str(e)})
    finally:
        if os.path.exists(notebook_path):
            os.remove(notebook_path)

class DatabricksCallbackPayload(BaseModel):
    transformationJobId: str
    databricksJobStatus: bool
    databricksOutput: str

@router.post("/databricks/callback")
async def databricks_callback(payload: DatabricksCallbackPayload):
    jobs_service = JobsService()
    if payload.databricksJobStatus:
        status = JOB_STATUS_COMPLETED
    else:
        status = JOB_STATUS_EXCEPTION
    return_data = jobs_service.update_job(
        transformation_job_id = payload.transformationJobId, 
        status = status, 
        databricks_job_status = payload.databricksJobStatus,
        databricks_output = payload.databricksOutput
    )
    return JSONResponse(status_code = 200, content = {"Message": "Success"})
