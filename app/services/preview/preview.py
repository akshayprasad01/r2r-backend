from app.services.recon.recon_mongo import DocumentService
from app.services.repository.preview_repo import PreviewRepoService
from app.settings import *
from app.helpers.transformation import *
from app.helpers.common import *
import time
import json
import os
import re
import shutil
import tempfile
import pandas as pd
from app.logger import logger

default_files_dir = TRANSFORMATION_DEFAULTFILES_DIR
dump_files_dir = TRANSFORMATION_DUMPFILES_DIR
template_files_dir = TRANSFORMATION_TEMPLATEFILES_DIR

exec(PreviewRepoService.imports())

class MyException(Exception):
    pass

class PreviewService:
    @staticmethod
    def home():
        return_data = {
            "status": True,
            "message": "Welcome to R2R - Transformation"
        }
        return return_data
    
    def readDfs(tempdir: str, fileConfigData: list, client: str, recon_id: str, requester_id: str):
        try:
            output_store = []
            for config in fileConfigData:
                name = config['fileName']
                # logger.info(name)
                if name.endswith('csv') or name.endswith('txt'):
                    # logger.info('inside csv')
                    params = {
                        'fileName': name,
                        'delimiter': config['delimiter'] if config['delimiter'] else ',',
                        'df': f"{config['resultText']}_{client}_{recon_id}_{requester_id}",
                        'localFilePath': tempdir
                    }
                    logger.info(params)
                    output, status = PreviewRepoService.readCsv(**params)
                    logger.info(output)
                    if status:
                        exec(output)
                        globals()[params['df']] = eval(params['df'])
                        output_store.append(globals()[params['df']])
                    else:
                        output_store.append(output_store)
                elif name.endswith('xlsx') or name.endswith('xls'):
                    # logger.info('inside excel')
                    params = {
                        'fileName': name,
                        'sheetName': config['sheetName'],
                        'localFilePath': tempdir,
                        'df': f"{config['resultText']}_{client}_{recon_id}_{requester_id}"
                    }
                    output, status = PreviewRepoService.readExcel(**params)
                    # logger.info(output)
                    if status:
                        exec(output)
                        globals()[params['df']] = eval(params['df'])
                        output_store.append(globals()[params['df']])
                    else:
                        output_store.append(output_store)
                    # print("This is after the execution of read files")
                    # print(list(globals().keys()))
                else:
                    exit()
                
            return True
        except Exception as e:
            logger.info(f"error in services read DF's: {str(e)}")
    
    def generateCellAndExecute(sftp, document, transformation_description: dict, client: str, recon_id: str, requester_id: str):
        try:
            transformation_type = transformation_description['type']
            params = transformation_description['params']
            for key in params.keys():
                if key.lower().endswith("df"):
                    params[key] += f"_{client}_{recon_id}_{requester_id}"
            
            logger.info(params)
            
            # logger.info(transformation_type, params)

            if transformation_type == "getSumOfColumn":
                output, status = PreviewRepoService.getSumOfColumn(**params)
                logger.info(output)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "getAvgOfColumn":
                output, status = PreviewRepoService.getAvgOfColumn(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
            
            elif transformation_type == "getMinOfColumn":
                output, status = PreviewRepoService.getMinOfColumn(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "getMaxOfColumn":
                output, status = PreviewRepoService.getMaxOfColumn(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "getCountOfColumn":
                output, status = PreviewRepoService.getCountOfColumn(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "readExcel":
                params['localFilePath'] = dump_files_dir
                output, status = PreviewRepoService.readExcel(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
            
            elif transformation_type == "readCsv":
                params['localFilePath'] = dump_files_dir
                output, status = PreviewRepoService.readCsv(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False

            elif transformation_type == "selectCols":
                dropna = params['dropna']
                params.pop('dropna')
                output, status = PreviewRepoService.selectCols(dropna = dropna, **params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
            
            elif transformation_type == "concatWs":
                output, status = PreviewRepoService.concatWs(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
            
            elif transformation_type == "renameColumns":
                output, status = PreviewRepoService.renameColumns(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "exactVLookup":
                output, status = PreviewRepoService.exactVLookup(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "selectLeft":
                output, status = PreviewRepoService.selectLeft(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "selectRight":
                output, status = PreviewRepoService.selectRight(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "columnTrim":
                output, status = PreviewRepoService.columnTrim(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "getLength":
                output, status = PreviewRepoService.getLength(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "dropDuplicates":
                output, status = PreviewRepoService.dropDuplicates(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "createNewColumnWithDefaultValue":
                output, status = PreviewRepoService.createNewColumnWithDefaultValue(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "copyColToNewCol":
                output, status = PreviewRepoService.copyColToNewCol(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
            
            elif transformation_type == "dropnaSubset":
                output, status = PreviewRepoService.dropnaSubset(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "leadColumn":
                output, status = PreviewRepoService.leadColumn(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "lagColumn":
                output, status = PreviewRepoService.lagColumn(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "dateDiff":
                output, status = PreviewRepoService.dateDiff(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "approxVlookup":
                output, status = PreviewRepoService.approxVlookup(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "ifThenElse":
                output, status = PreviewRepoService.if_then_else(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
                
            # elif transformation_type == "ageing":
            #     output, status = PreviewRepoService.ageing(**params)
            #     if status:
            #         exec(output)
            #         globals()[params['masterDf']] = eval(params['masterDf'])
            #         return globals()[params['masterDf']], True
            #     else:
            #         return str(output), False
                
            elif transformation_type == "groupBy":
                output, status = PreviewRepoService.groupBy(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "pivot":
                output, status = PreviewRepoService.pivot(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "filter":
                output, status = PreviewRepoService.filterDf(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False

            elif transformation_type == "findAndReplaceInColumn":
                output, status = PreviewRepoService.findAndReplaceInColumn(**params)
                if status:        
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
            
            elif transformation_type == "multiplyColumns":
                output, status = PreviewRepoService.multiplyColumns(**params)
                if status:
                    exec(output)
                    globals()[params['oldDf']] = eval(params['oldDf'])
                    return globals()[params['oldDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "divideColumns":
                output, status = PreviewRepoService.divideColumns(**params)
                if status:
                    exec(output)
                    globals()[params['oldDf']] = eval(params['oldDf'])
                    return globals()[params['oldDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "addColumns":
                output, status = PreviewRepoService.addColumns(**params)
                if status:
                    exec(output)
                    globals()[params['oldDf']] = eval(params['oldDf'])
                    return globals()[params['oldDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "subtractColumns":
                output, status = PreviewRepoService.subtractColumns(**params)
                if status:
                    exec(output)
                    globals()[params['oldDf']] = eval(params['oldDf'])
                    return globals()[params['oldDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "sortDf":
                output, status = PreviewRepoService.sortDf(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
            
            elif transformation_type == "combineTables":
                params['client'] = client
                params['recon_id'] = recon_id
                params['requester_id'] = requester_id
                output, status = PreviewRepoService.combineTables(**params)
                if status:
                    exec(output)
                    globals()[params['resultDf']] = eval(params['resultDf'])
                    return globals()[params['resultDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "filterDelete":
                output, status = PreviewRepoService.filterDelete(**params)
                if status:
                    exec(output)
                    globals()[params['newDf']] = eval(params['newDf'])
                    return globals()[params['newDf']], True
                else:
                    return str(output), False
                
            elif transformation_type == "mid":
                output, status = PreviewRepoService.mid(**params)
                if status:
                    exec(output)
                    globals()[params['df']] = eval(params['df'])
                    return globals()[params['df']], True
                else:
                    return str(output), False
                
            elif transformation_type == "writeExcel":
                try:
                    sftp_target_dir = f"{document['previewFilePath']}/outputs"
                    try:
                        sftp.mkdir(sftp_target_dir)
                    except:
                        logger.info("error dir already exists")
                    finally:
                        pass 
                    logger.info("creating a temp directory")
                    temp_dir = tempfile.mkdtemp()
                    logger.info("done creating a temp directory")
                    logger.info(temp_dir)
                    logger.info(params)
                    params['temp_dir'] = temp_dir
                    params['client'] = client
                    params['recon_id'] = recon_id
                    params['requester_id'] = requester_id
                    logger.info(params)
                    output, fileName, status = PreviewRepoService.writeExcelPreview(**params)
                    logger.info(f"output for Write Excel: {output}")
                    logger.info(f"Status of write excel : {status}")
                    if status:
                        exec(output)
                        sftp.put(f'{temp_dir}/{fileName}', f'{sftp_target_dir}/{fileName}')
                        df = pd.DataFrame({'Output': ['Successfully executed Saving of Excel files']})
                        return df, True
                    else:
                        raise MyException(output)
                except Exception as e:
                    logger.info(f"error in preview Service : {str(e)}")
                finally:
                    shutil.rmtree(temp_dir)
                    pass
            
            elif transformation_type == "zipFiles":
                try:
                    temp_dir_zip_preview = tempfile.mkdtemp()
                    logger.info(temp_dir_zip_preview)
                    configList = params['configList']
                    logger.info(f"ConfigList: {configList}")
                    previewFilePath = document['previewFilePath']
                    logger.info(f"previewFilePath : {previewFilePath}")
                    for config in configList:
                        fullSFTPFilePath = os.path.join(previewFilePath, config['file'])
                        fullLocalFilePath = os.path.join(temp_dir_zip_preview, config['file'].split('/')[-1])
                        sftp.get(fullSFTPFilePath, fullLocalFilePath)
                    

                    # Zip the downloaded files
                    temp_dir_zip_preview_zipPath = tempfile.mkdtemp()
                    temp_dir_zip_preview_zipPath = f"{temp_dir_zip_preview_zipPath}/{document['name']}_output.zip"
                    logger.info(f"temp_dir_zip_preview_zipPath: {temp_dir_zip_preview_zipPath}")
                    zip_directory(directory_path=temp_dir_zip_preview, output_zip_file=temp_dir_zip_preview_zipPath)
                    sftp.put(temp_dir_zip_preview_zipPath, f"{document['previewOutputPath']}/{document['name']}_output.zip")
                    return pd.DataFrame({'Output': [f"Successfully Zipped Files and saved to SFTP location at {document['previewOutputPath']}/{document['name']}_output.zip"]}), True
                except Exception as e:
                    logger.info(f"error in Zipping file: {str(e)}")
            
            else:
                return "No such Transformation Exists", False

        except Exception as e:
            return str(e), False

    def deletePreview(payload, requester_id):
        try:
            for var in list(globals().keys()):
                if var.endswith(f"_{payload.client}_{payload.reconcilitationId}_{requester_id}"):
                    logger.info(f"deleteing Vars {var}")
                    del globals()[var]

            return True
        except Exception as e:
            return False