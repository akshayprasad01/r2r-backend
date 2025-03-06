import tempfile
import os
import stat
from app.logger import logger

def getAllFilesfromSFTP(sftp, fileConfigData):
    temp_dir = tempfile.mkdtemp()
    try:
        # Download files to the temporary directory
        for configs in fileConfigData:
            file_path = configs['sftpFilePath']
            local_path = os.path.join(temp_dir, os.path.basename(file_path))
            sftp.get(file_path, local_path)
        return temp_dir
    except Exception as e:
        logger.info(f"error in helper preview.getAllFIles: {str(e)}")

def list_files_recursively(sftp, remote_path):
    files = []
    
    for entry in sftp.listdir_attr(remote_path):
        remote_file_path = os.path.join(remote_path, entry.filename)
        if stat.S_ISDIR(entry.st_mode):
            files.extend(list_files_recursively(sftp, remote_file_path))
        else:
            files.append(remote_file_path)
    # for i in range(len(files)):
    #     files[i] = files[i].replace(f'{remote_path}', '')
    return files

def sftp_mkdirs(sftp, remote_path, mode=511):
    dirs = remote_path.split('/')
    path = ''
    for directory in dirs:
        if directory:  # to handle leading '/'
            path += '/' + directory
            try:
                sftp.mkdir(path, mode=mode)
            except IOError:  # Directory already exists
                pass

def sftp_remove_dir(sftp, remote_path):
    for file_attr in sftp.listdir_attr(remote_path):
        full_path = remote_path + '/' + file_attr.filename
        if stat.S_ISDIR(file_attr.st_mode):
            sftp_remove_dir(sftp, full_path)  # Recursive call
        else:
            sftp.remove(full_path)
    sftp.rmdir(remote_path) 