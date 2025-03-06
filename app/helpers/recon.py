import paramiko
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from io import StringIO
from app.settings import *
import os
import shutil
from app.logger import logger

def get_sftp(hostname: str,
    port: str,
    username: str,
    password: str | None = None,
    key_file: str | None = None):

    try:
        if not password and not key_file:
            raise JSONResponse(status_code=400, detail="Either password or key_file must be provided")
        else:
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
            logger.info("created sftp")
            return sftp, transport
        
    except Exception as e:
        logger.info(f"Connection failed: {str(e)}")
        return JSONResponse(status_code=500,
                            content={"message": f"Connection failed: {str(e)}"})
    
def create_local_path(path: str):
    if not os.path.exists(path):
        os.makedirs(path)
    
def save_zip_to_local(file):
    try:
        # Define the local path where the file will be saved
        local_zip_path = TRANSFORMATION_ZIP_DIR
        create_local_path(local_zip_path)
        local_file_path = f"{local_zip_path}/uploads/{file.filename}"
        
        # Ensure the uploads directory exists
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Save the file to the local filesystem
        with open(local_file_path, "wb") as local_file:
            shutil.copyfileobj(file.file, local_file)
        
        # return {"message": "File uploaded successfully", "file_path": local_file_path}
        return local_file_path
    except Exception as e:
        return {"error": f"An error occurred: {str(e)}"}
    
def unzip_on_sftp(hostname: str,
    port: str,
    username: str,
    remote_zip_path: str,
    remote_extract_path: str,
    password: str | None = None,
    key_file: str | None = None
    ):
    try:
        # Create an SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Load the SSH key from the string
        if key_file:
            key_file = StringIO(key_file)
            key = paramiko.RSAKey.from_private_key(key_file)
            
            
            # Connect to the server using the key
            ssh.connect(hostname=hostname,
                        port=int(port),
                        username=username,
                        pkey=key)
        else:
            ssh.connect(hostname=hostname,
                        port=int(port),
                        username=username,
                        password=password)
        
        # Command to unzip the file
        unzip_command = f'unzip -o {remote_zip_path} -d {remote_extract_path}'
        
        # Execute the command
        stdin, stdout, stderr = ssh.exec_command(unzip_command)
        
        # Read the command output
        stdout_output = stdout.read().decode('utf-8')
        stderr_output = stderr.read().decode('utf-8')
        
        # logger.info output and errors (if any)
        logger.info("STDOUT:")
        logger.info(stdout_output)
        logger.info("STDERR:")
        logger.info(stderr_output)
        
        # Close the connection
        ssh.close()
        
    except Exception as e:
        logger.info(f"An error occurred: {e}")