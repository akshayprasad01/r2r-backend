import os

HOST_TENANCY=os.getenv('HOST_TENANCY')
# Retry attempt for Tenancy URL
INITIAL_TENANCY_RETRY_DELAY_IN_SECONDS = 20