from app.models.recon.document_model_mongo import DocumentModel
import paramiko
from app.settings import *
from app.helpers.preview import *
import os
from io import StringIO
import pandas as pd
from app.logger import logger
from fastapi import HTTPException
from fastapi.responses import JSONResponse
import zipfile
import shutil
from io import BytesIO

class DocumentService:
    def __init__(self, db_name):
        self.db_name = db_name
    
    @staticmethod
    def home():
        return_data = {
            "status": True,
            "message": "Welcome to R2R - Reconciliations"
        }
        return return_data
    
    def get_documents(self, collection_name, client_id = None):
        try:
            document_model = DocumentModel(db_name = self.db_name)
            if client_id:
                filter = {
                    'client_id': str(client_id)
                }
                documents = document_model.get_all_documents(collection_name, filter=filter)
            else:
                documents = document_model.get_all_documents(collection_name)
            
            if documents is None:
                documents = []

            return_data = {
                "status": True,
                "message": "Recon List",
                "data": documents
            }
            logger.info(return_data)
            return return_data
        except Exception as e:
            logger.info("DocumentService:get_documents - catch: " + str(e))
            return {"status": False, "error": str(e)}
        
    def get_document(self, collection_name, document_id):
        try:
            if document_id is None:
                return {"status": False, "message": "Recon ID is required"}
            else:
                document_model = DocumentModel(db_name = self.db_name)
                document = document_model.get_document(collection_name, document_id)

                if document:
                    document["_id"] = str(document["_id"])
                    document["reconId"] = str(document["_id"])
                    document["status"] = True
                    document["message"] = "Recon Detail"
                    return document
                else:
                    return {"status": False, "message": "Invalid Recon ID or Recon is not available"}
        except Exception as e:
            logger.info("DocumentService:get_document - catch: " + str(e))
            return {"status": False, "error": str(e)}
        
    def create_or_update(self, current_user, collection_name, recon_payload, id=None):
        try:
            document_model = DocumentModel(db_name = self.db_name)
            if id is None:
                return document_model.create_or_update(current_user, collection_name, recon_payload)
            else:
                filter = {}
                # TODO check the duplicate
                return document_model.create_or_update(current_user, collection_name, recon_payload, id)
        except Exception as e:
            logger.info("DocumentService:create_or_update - catch: " + str(e))
            return {"status": False, "error": str(e)}
    
    def get_document_by_name(self, collection_name, document_name):
        try:
            if document_name is None:
                return {"status": False, "message": "Recon name is required"}
            else:
                document_model = DocumentModel(db_name = self.db_name)

                filter = {
                    "name": document_name
                }

                document = document_model.search_document(
                    collection_name, filter
                )
                if document is not None:
                    document["_id"] = str(document["_id"])
                    document["reconId"] = str(document["_id"])
                    document["status"] = True
                    document["message"] = "Recon Detail"
                    # logger.info(document)
                    return document
                else:
                    return {"status": False, "message": "Invalid Recon name or Recon is not available"}
                
        except Exception as e:
            logger.info("DocumentService:get_document - catch: " + str(e))
            return {"status": False, "error": str(e)}
        
    def get_sftp_by_name(self, collection_name, client_name):
        try:
            if client_name is None:
                return {"status": False, "message": "Recon name is required"}
            else:
                document_model = DocumentModel(db_name = self.db_name)

                filter = {
                    "clientName": client_name
                }

                document = document_model.search_document(
                    collection_name, filter
                )
                if document is not None:
                    document["_id"] = str(document["_id"])
                    document["sftpId"] = str(document["_id"])
                    document["status"] = True
                    document["message"] = "SFTP Detail"
                    # logger.info(document)
                    return document
                else:
                    return {"status": False, "message": "Invalid Client name or SFTP is not available"}
                
        except Exception as e:
            logger.info("DocumentService:get_sftp_by_name - catch: " + str(e))
            return {"status": False, "error": str(e)}
        
    def get_transformation_recon_format(self, recon_payload):
        start_index = 0
        transformations = {}
        start_index = 0
        if recon_payload['transformations']:
            for entry in recon_payload['transformations']:
                transformations.update({str(start_index): entry})
                start_index += 1
        start_index = 0
        recon_payload['transformations'] = transformations

        return recon_payload
    
    def get_transformation_recon_by_name(self, collection_name, document_name):
        try:
            document = self.get_document_by_name(collection_name, document_name)
            # logger.info("Secured Document")
            if document["status"] == True:
                document = self.get_transformation_recon_format(document)
            return document
        except Exception as e:
            logger.info("DocumentService:get_document - catch: " + str(e))
            return {"status": False, "error": str(e)}
        
    def checkSFTPConnection(self, hostname, port, username, password, key_file):
        try:
            transport = paramiko.Transport((hostname, port))
            if key_file is not None:
                private_key_file = StringIO(key_file.strip())
                try:
                    private_key = paramiko.RSAKey.from_private_key(private_key_file)
                except paramiko.ssh_exception.SSHException:
                    private_key_file.seek(0)
                    try:
                        private_key = paramiko.ECDSAKey.from_private_key(private_key_file)
                    except paramiko.ssh_exception.SSHException:
                        private_key_file.seek(0)
                        private_key = paramiko.Ed25519Key.from_private_key(private_key_file)

                    # Connect to the SFTP server
                try:
                    transport.connect(username=username, pkey=private_key)
                except paramiko.SSHException as e:
                    logger.info(f"SSH connection failed: {e}")
            else:
                transport.connect(username=username, password=password)

            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.close()
            transport.close()
            logger.info("Connection successful")
            return JSONResponse(status_code=200,
                                content={"message":"Connection successful"})
        except Exception as e:
            logger.info(f"Connection failed: {str(e)}")
            return JSONResponse(status_code=500,
                                content={"message": f"Connection failed: {str(e)}"})
        
    def getFileList(self, file):
        try:
            id_counter = 1
            response = []
            temp_dir = '/tmp/uploaded_files'

            if not os.path.exists(temp_dir):
                os.makedirs(temp_dir)

            with zipfile.ZipFile(file.file, 'r') as zip_ref:
                for zip_info in zip_ref.infolist():
                    if zip_info.is_dir() or zip_info.filename.startswith('__MACOSX') or zip_info.filename.startswith('._'):
                        continue

                    with zip_ref.open(zip_info) as f:
                        file_name = zip_info.filename
                        if file_name.endswith('.csv'):
                            response.append({
                                "id": id_counter,
                                "type": "csv",
                                "fileName": file_name
                            })
                            id_counter += 1
                        elif file_name.endswith('.txt'):
                            response.append({
                                "id": id_counter,
                                "type": "txt",
                                "fileName": file_name
                            })
                            id_counter += 1
                        elif file_name.endswith(('.xls', '.xlsx')):
                            # Read excel file to get sheet names
                            in_memory_file = BytesIO(f.read())
                            excel_file = pd.ExcelFile(in_memory_file, engine='openpyxl')
                            sheet_names = excel_file.sheet_names
                            response.append({
                                "id": id_counter,
                                "type": "xlsx",
                                "fileName": file_name,
                                "sheetNames": sheet_names
                            })
                            id_counter += 1

                        else:
                            continue

            # Clean up the extracted files
            shutil.rmtree(temp_dir)

            return JSONResponse(status_code=200, content=response)

        except zipfile.BadZipFile:
            raise HTTPException(status_code=400, detail="Invalid ZIP file")
        
    def saveFilesForConfigRun(self, sftp, inputfileLocation, outputfileLocation):
        try:
            # Ensure the remote path exists, create if necessary
            remote_path_for_config_run_input = f'{inputfileLocation}/config_run/input'
            remote_path_for_config_run_output = f'{outputfileLocation}/config_run/output'

            sftp_mkdirs(sftp=sftp, remote_path=remote_path_for_config_run_input)
            sftp_mkdirs(sftp=sftp, remote_path=remote_path_for_config_run_output)

            return remote_path_for_config_run_input, remote_path_for_config_run_output
            
        except Exception as e:
            logger.info(f"An error occurred in Document Service: {str(e)}")
            return JSONResponse(status_code=500, content={"error": f"An error occurred: {str(e)}"})

    def save_and_extract_zip_to_sftp_1(self, sftp, file, remote_path, recon_name):
        try:
            try:
                logger.info(f" making dir {remote_path}/{recon_name}")
                # sftp_remove_dir(sftp=sftp, remote_path=f"{remote_path}/{recon_name}")
                logger.info("Step 2")
                sftp_mkdirs(sftp=sftp, remote_path=f"{remote_path}/{recon_name}/{file.filename.split('.')[0]}")
                logger.info("Step 3")
                # sftp.mkdir(f'{remote_path}/{recon_name}/input')
                logger.info(f"done creating dir {remote_path}/{recon_name}")
            except Exception as e:
                logger.info(f"error in making dir {remote_path}/{recon_name}")
                logger.info(f"error: {str(e)}")
            finally:
                logger.info("I'm here in save_and_extract_zip_to_sftp_1")
            with zipfile.ZipFile(file.file, 'r') as zip_ref:
                logger.info(zip_ref.infolist())
                for file_info in zip_ref.infolist():
                    file_path = os.path.join(remote_path, recon_name, file.filename.split('.')[0], file_info.filename)
                    logger.info(file_path)
                    if file_info.is_dir():
                        try:
                            logger.info('making dir')
                            sftp_mkdirs(sftp=sftp, remote_path=file_path)
                            # sftp.mkdir(file_path)
                            logger.info('done Making Dir')
                        except OSError:
                            logger.info('error in making dir')
                    else:
                        # logger.info(file_info.filename)
                        file_name_test = file_info.filename.split("/")[-1]
                        if file_name_test.startswith("._") or file_name_test.startswith("_MACOSX"):
                            pass
                        else:
                            with zip_ref.open(file_info) as source_file:
                                logger.info("inside Zipref")
                                with sftp.file(file_path, 'wb') as target_file:
                                    logger.info("writing to SFTP")
                                    target_file.write(source_file.read())
            return_path = f"{remote_path}/{recon_name}/{file.filename.split('.')[0]}"
            logger.info(return_path)
            return str(return_path)
        except Exception as e:
            logger.info(str(e))