import os
# Log level
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG').upper()

HOST_TENANCY = os.getenv('HOST_TENANCY', 'https://services-aira.f1e5037eee6d40ea9a20.westeurope.aksapp.io/tenancy-api')

from app.settings import *
from app.helpers.common import create_directory_if_not_exists
create_directory_if_not_exists(TRANSFORMATION_LOGS_DIR)
create_directory_if_not_exists(TRANSFORMATION_ZIP_DIR)
create_directory_if_not_exists(TRANSFORMATION_DEFAULTFILES_DIR)
create_directory_if_not_exists(TRANSFORMATION_OUTPUT_DIR)
create_directory_if_not_exists(TRANSFORMATION_DUMPFILES_DIR)
create_directory_if_not_exists(TRANSFORMATION_TEMPLATEFILES_DIR)