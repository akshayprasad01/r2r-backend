from app.services.recon.recon_mongo import DocumentService
from app.services.repository.config_run_repo import RepoService
from app.settings import *
from app.helpers.transformation import *
from app.constants.ADLS import *
from app.constants.mongodb import *
from app.helpers.recon import *
from app.helpers.common import *
import time
import nbformat as nbf

class MyException(Exception):
    pass

class TransformationService:
    @staticmethod
    def home():
        return_data = {
            "status": True,
            "message": "Welcome to R2R - Transformation"
        }
        return return_data
    
    def readConfig(db_name: str, client_id: str, reconciliationId: str, date: str | None = None):
        try:
            notebook = nbf.v4.new_notebook() # create a new notebook
            sftp_collection = MONGODB_CLIENT_DETAILS_COLLECTION
            recon_service = DocumentService(db_name = db_name)
            sftp_details = recon_service.get_document(sftp_collection, client_id) # dict of connection details
            
            if sftp_details['key_file']:
                sftp_details['password'] = None
            else:
                sftp_details['key_file'] = None

            sftp, transport = get_sftp(hostname=sftp_details['hostname'],
                            username=sftp_details['username'],
                            password=sftp_details['password'],
                            port=int(sftp_details['port']),
                            key_file=sftp_details['key_file'])
            
            # logger.info(sftp)
            
            indent_level = 1
            data = recon_service.get_document(MONGODB_CLIENT_RECONS_COLLECTION, reconciliationId)
            recon_name = data['name']
            logger.info(data)

            # prepare file path on SFTP to copy to local

            n1 = getDate(date)
            sftp_file_path = os.path.join(sftp_details['inputfileLocation'], f'{n1}.zip')

            # get all the files to local for reading and writing operations. 
            temp_dir = getAllFilesFromSFTPforRun(sftp=sftp,
                                                    client=client_id,
                                                    recon_id=reconciliationId,
                                                    file_path_on_sftp=sftp_file_path,
                                                    filename=f'{n1}.zip')
            
            adls_directory_path, status = upload_to_blob_storage(unzipFilePath=temp_dir,
                                    recon_name=recon_name,
                                    date = n1)
            
            if not status:
                raise MyException("Files not uploaded to ADLS Gen 2 Storage")
            
            # write all the imports to Python Notebook
            code_cell = nbf.v4.new_code_cell(RepoService.imports_transformation())
            notebook.cells.append(code_cell)

            code_cell = nbf.v4.new_code_cell(RepoService.getFilePathOnADLS())
            notebook.cells.append(code_cell)

            code_cell = nbf.v4.new_code_cell(RepoService.zipFilesOnADLS())
            notebook.cells.append(code_cell)

            # write all the required functions to Notebook
            code_cell = nbf.v4.new_code_cell(RepoService.sftpConn())
            notebook.cells.append(code_cell)

            code_cell = nbf.v4.new_code_cell(RepoService.saveDFToExcel())
            notebook.cells.append(code_cell)
            sftp_details_copy = sftp_details

            sftp_details_copy.pop("password")
            sftp_details_copy.pop("key_file")
            code_snippet = f"""
    sftp_details = {sftp_details_copy}
    """
            code_cell = nbf.v4.new_code_cell(code_snippet)
            notebook.cells.append(code_cell)
            
            if data is not None:
                python_file = f"{recon_name}_{time.time()}" + '.py'
                python_file_path = f"{BIN_DIR}/{python_file}"
                logger.info(python_file_path)

                with open(python_file_path, 'w') as f:
                    f.write(RepoService.startingBlock())
                    transformations = data['transformations']
                    config_data = data['fileConfigData']
                    logger.info(f"File Config Data :-- {config_data}")

                    # start reading file config data
                    for config in config_data:
                        periodicRunKey = config['periodicRunKey']
                        logger.info(periodicRunKey)
                        equals = None
                        startsWith = None
                        endsWith = None
                        contains = None
                        if 'contains' in periodicRunKey:
                            contains = config['fileNamePeriodicRun']
                        elif periodicRunKey == 'equals':
                            equals = config['fileNamePeriodicRun']
                        elif periodicRunKey == 'endsWith':
                            endsWith = config['fileNamePeriodicRun']
                        else:
                            startsWith = config['fileNamePeriodicRun']

                        adls_file_path = getFilePathOnADLS(directory=adls_directory_path,
                                                equals = equals,
                                                startsWith = startsWith,
                                                endsWith = endsWith,
                                                contains = contains)
                        logger.info(adls_file_path)
                        
                        adls_file_name_retrieved = os.path.basename(adls_file_path)
                        if adls_file_name_retrieved.endswith('xlsx') or adls_file_name_retrieved.endswith('xls'):
                            params = {
                                'sheetName': config['sheetName'],
                                'df': config['resultText'],
                                'localFilePath': os.path.dirname(adls_file_path),
                                'fileName': adls_file_name_retrieved
                            }
                            output, status = RepoService.readExcel(**params)
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if adls_file_name_retrieved.endswith('csv') or adls_file_name_retrieved.endswith('txt'):
                            params = {
                                'fileName': os.path.basename(adls_file_path),
                                'delimiter': ',',
                                'df': config['resultText'],
                                'localFilePath': os.path.dirname(adls_file_path)
                            }
                            output, status = RepoService.readCsv(**params)
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)

                    ######### this is for reading the transformation tab
                    for value in transformations:
                        if value['type'] == "getSumOfColumn":
                            output, status = RepoService.getSumOfColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "getAvgOfColumn":
                            output, status = RepoService.getAvgOfColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "getMinOfColumn":
                            output, status = RepoService.getMinOfColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "getMaxOfColumn":
                            output, status = RepoService.getMaxOfColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "getCountOfColumn":
                            output, status = RepoService.getCountOfColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "if":
                            output, status = RepoService.startIf(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                                indent_level += 1
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "elif":
                            output, status = RepoService.elIf(**value['params'])
                            if status:
                                elif_indent_generator = indent_level - 1
                                indent = indent_generator(indent_level=elif_indent_generator)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "else":
                            output, status = RepoService.elSe()
                            if status:
                                else_indent_generator = indent_level - 1
                                indent = indent_generator(indent_level=else_indent_generator)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)

                        if value['type'] == "endif":
                            if indent_level == 0:
                                indent_level = 0
                            else:
                                indent_level -=1
                            
                        if value['type'] == "readExcel":
                            value['params']['localFilePath'] = 'dump_files_dir'
                            output, status = RepoService.readExcel(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "readCsv":
                            output, status = RepoService.readCsv(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)

                        if value['type'] == "selectCols":
                            dropna = value['params']['dropna']
                            value['params'].pop('dropna')
                            output, status = RepoService.selectCols(dropna = dropna, **value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "concatWs":
                            output, status = RepoService.concatWs(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "renameColumns":
                            output, status = RepoService.renameColumns(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "exactVLookup":
                            output, status = RepoService.exactVLookup(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "selectLeft":
                            output, status = RepoService.selectLeft(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "selectRight":
                            output, status = RepoService.selectRight(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "columnTrim":
                            output, status = RepoService.columnTrim(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "getLength":
                            output, status = RepoService.getLength(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "dropDuplicates":
                            output, status = RepoService.dropDuplicates(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "createNewColumnWithDefaultValue":
                            output, status = RepoService.createNewColumnWithDefaultValue(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "copyColToNewCol":
                            output, status = RepoService.copyColToNewCol(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "dropnaSubset":
                            output, status = RepoService.dropnaSubset(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "leadColumn":
                            output, status = RepoService.leadColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "lagColumn":
                            output, status = RepoService.lagColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "dateDiff":
                            output, status = RepoService.dateDiff(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "approxVlookup":
                            output, status = RepoService.approxVlookup(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "writeExcel":
                            value['params']['temp_dir'] = adls_directory_path
                            output,status = RepoService.writeExcel(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "writeCsv":
                            output,status = RepoService.writeCsv(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                                f.write(output)
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "ifThenElse":
                            output, status = RepoService.if_then_else(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "ageing":
                            output, status = RepoService.ageing(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "groupBy":
                            output, status = RepoService.groupBy(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "pivot":
                            output, status = RepoService.pivot(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "filter":
                            output, status = RepoService.filterDf(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "findAndReplaceInColumn":
                            output, status = RepoService.findAndReplaceInColumn(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "divideColumns":
                            output, status = RepoService.divideColumns(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "addColumns":
                            output, status = RepoService.addColumns(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "subtractColumns":
                            output, status = RepoService.subtractColumns(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "multiplyColumns":
                            output, status = RepoService.multiplyColumns(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "sort":
                            output, status = RepoService.sortDf(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "combineTables":
                            output, status = RepoService.combineTables(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "filterDelete":
                            output, status = RepoService.filterDelete(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                        
                        if value['type'] == "mid":
                            output, status = RepoService.mid(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        if value['type'] == "zipFiles":
                            config_list = value['params']['configList']
                            date_output = n1.split('-')
                            date_output[-1] = date_output[-1][-2:]
                            date = '-'.join(date_output)
                            output, status = RepoService.zipFiles(temp_dir=adls_directory_path,
                                                                    config_list=config_list,
                                                                    sftp_details=sftp_details,
                                                                    n1=date,
                                                                    recon_name=recon_name)
                            
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)
                            
                        '''    
                        if value['type'] == "":
                            output, status = RepoService.(**value['params'])
                            if status:
                                indent = indent_generator(indent_level=indent_level)
                                for line in output.splitlines():
                                    f.write(f'{indent}{line}\n')
                            else:
                                raise MyException(output)'''

                    output, status = RepoService.lastBlock()
                    f.write(output)
                try:
                    with open(python_file_path, 'r') as f:
                        python_code = f.read()
                except:
                    logger.info("Python file does not exist")

                code_cell = nbf.v4.new_code_cell(python_code)
                notebook.cells.append(code_cell)

                code_cell = nbf.v4.new_code_cell("print('hello World !!')")
                notebook.cells.append(code_cell)

                # Write the notebook to a file
                logger.info("Creating notebook")
                notebook_filename = f"{BIN_DIR}/notebook_{recon_name}_{n1}.ipynb"
                logger.info(notebook_filename)

                with open(notebook_filename, 'w') as f:
                    nbf.write(notebook, f)
                    logger.info("Notebook written")

                return notebook_filename, True
            else:
                return data, False
        
        except MyException as e:
            return str(e), False
        
        finally:
            sftp.close()
            transport.close()
            if os.path.exists(python_file_path):
                os.remove(python_file_path)
            # shutil.rmtree(temp_dir)
