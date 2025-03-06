import os
import time
import base64
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from databricks.sdk.service.workspace import ExportFormat, Language
from databricks.sdk.service.jobs import SparkPythonTask, Task, NotebookTask
from app.logger import logger

class DatabricksService:
    def __init__(self):
        self.host = os.getenv("DATABRICKS_HOST")
        self.client_id = os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        self.instance_pool_id = os.getenv("DATABRICKS_INSTANCE_POOL_ID")
        self.num_workers = 4
        self.spark_version = os.getenv("DATABRICKS_SPARK_VERSION")

    def __connect(self):
        # Hidden Method
        client = WorkspaceClient(host=self.host,
                                 client_id=self.client_id,
                                 client_secret=self.client_secret)
        return client
    
    def postPythonJob(self, pythonFilePathDBFS, env_vars):
        try:
            client = self.__connect()
            new_cluster = compute.ClusterSpec(
                instance_pool_id=self.instance_pool_id,
                num_workers=self.num_workers,
                spark_version = self.spark_version,
                spark_env_vars= env_vars
            )

            task = Task(
                task_key="R2R-Transformation-python-task-key",
                new_cluster=new_cluster,
                timeout_seconds=3600,
                spark_python_task=SparkPythonTask(
                    python_file=pythonFilePathDBFS # Path to the Python file in DBFS
                ),
                libraries=[
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="paramiko")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="openpyxl")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="fastapi")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="azure-storage-file-datalake")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="azure-identity")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="python-dotenv")),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="com.crealytics:spark-excel_2.12:3.3.2_0.19.0"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.hadoop:hadoop-azure:3.3.6"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.hadoop:hadoop-common:3.3.6"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.iceberg:iceberg-hive-runtime:1.5.2"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.iceberg:iceberg-azure-bundle:1.5.2"
                    )) 
                ]
            )

            response = client.jobs.create(name="R2R-Trasformation-python-task", tasks=[task])
            job_id = response.job_id
            logger.info(f"Job created with ID: {job_id}")
            return job_id
        except Exception as e:
            logger.info(f"Error in Databricks Service - Monitor Job : {str(e)}")
            return None
    
    def postNotebookJob(self, notebookPathWorkspace, env_vars):
        try:
            client = self.__connect()  # Connect to Databricks Workspace
            new_cluster = compute.ClusterSpec(
                instance_pool_id=self.instance_pool_id,
                num_workers=self.num_workers,
                spark_version = self.spark_version,
                spark_env_vars= env_vars
            )

            task = Task(
                task_key="R2R-Transformation-notebook-task-key",
                new_cluster=new_cluster,
                timeout_seconds=3600,
                notebook_task=NotebookTask(
                    notebook_path=notebookPathWorkspace  # Path to the Python Notebook in DBFS
                ),
                libraries=[
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="paramiko")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="openpyxl")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="fastapi")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="azure-storage-file-datalake")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="azure-identity")),
                    compute.Library(pypi=compute.PythonPyPiLibrary(package="python-dotenv")),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="com.crealytics:spark-excel_2.12:3.3.2_0.19.0"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.5.2"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.hadoop:hadoop-azure:3.3.6"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.hadoop:hadoop-common:3.3.6"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.iceberg:iceberg-hive-runtime:1.5.2"
                    )),
                    compute.Library(maven=compute.MavenLibrary(
                        coordinates="org.apache.iceberg:iceberg-azure-bundle:1.5.2"
                    )) 
                ]
            )

            response = client.jobs.create(name="R2R-Transformation-notebook-task", tasks=[task])
            job_id = response.job_id
            logger.info(f"Notebook job created with ID: {job_id}")
            return job_id
        except Exception as e:
            logger.info(f"Error in Databricks Service - Monitor Job: {str(e)}")
            return None
        
    def runJob(self, jobId):
        try:
            client = self.__connect()
            # Run the job asynchronously
            run_response = client.jobs.run_now(job_id=jobId)
            run_id = run_response.run_id
            logger.info(f"Job run initiated with run ID: {run_id}")
            return run_id
        except Exception as e:
            logger.info(f"Error in Databricks Service - Run Job : {str(e)}")
            return None

    def monitorJob(self, runId):
        try:
            client = self.__connect()
            while True:
                run_info = client.jobs.get_run(run_id=runId)
                state = run_info.state.life_cycle_state
                result_state = run_info.state.result_state
                logger.info(f"Job run status: {state}")
                if state.name in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                    if result_state == "SUCCESS":
                        logger.info("Job completed successfully.")
                    else:
                        logger.info(f"Job failed with result state: {result_state}")
                    break
                time.sleep(60)  # Wait 30 seconds before checking the status again
        except Exception as e:
            logger.info(f"Error in Databricks Service - Monitor Job : {str(e)}")
            return None

    def uploadPythonFile(self, pythonFilePath, DBFSFileName):
        try:
            client = self.__connect()
            dbfs_path = f"/FileStore/wheels_akshay/{DBFSFileName}"
            with open(pythonFilePath, 'r') as f:
                file_content = f.read()
            # Base64-encode the file content
            encoded_content = base64.b64encode(file_content.encode("utf-8")).decode("utf-8")

            # Write the content to a file on DBFS
            client.dbfs.put(path = dbfs_path, contents=encoded_content, overwrite=True)

            logger.info(f"File successfully written to {dbfs_path}")

            return True
        except Exception as e:
            logger.info(f"Error in Databricks Service - upload Python File : {str(e)}")
            return None
    
    def uploadPythonNotebook(self, local_notebook_path):
        try:
            db = self.__connect()
            # Read the notebook file
            with open(local_notebook_path, 'r', encoding='utf-8') as file:
                notebook_content = file.read()

            # Encode the notebook content to base64
            notebook_content_encoded = base64.b64encode(notebook_content.encode('utf-8')).decode('utf-8')
            databricks_notebook_path = f"/Workspace/Shared/{os.path.basename(local_notebook_path)}"
            # Upload the notebook
            response = db.workspace.import_(
                path=databricks_notebook_path,
                format=ExportFormat.JUPYTER,
                language=Language.PYTHON,
                content=notebook_content_encoded,
                overwrite=True
            )

            # logger.info the response
            logger.info("Notebook uploaded successfully.")
            logger.info(response)

            return databricks_notebook_path, True
        except Exception as e:
            return str(e), False