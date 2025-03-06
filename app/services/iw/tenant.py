import os
import requests
import time
from app.logger import logger

class TenantService:
    def __init__(self):
        pass

    def get_tenancy(self, url):
        retries = 1
        max_retries = 3
        tenancy_fetch = False
        while not tenancy_fetch:
            try:
                # Make the GET request to the specified URL
                response = requests.get(url,  verify=False)
                print(response)
                logger.info(f"Response from Tenancy Url: {response.json()}")
                # logger.info(f"Status Code for response: {response.status_code}")
                if 'spaceContexts' in response.json():
                    logger.info("Success for tenancy")
                    tenancy_fetch = True

            except requests.exceptions.HTTPError as http_err:
                logger.critical(f"HTTP error occurred: {http_err}")
                if retries > max_retries:
                    return None
                time.sleep(retries * 75)
                retries += 1

            except Exception as err:
                logger.critical(f"An error occurred: {err}")
                if retries > max_retries:
                    return None
                time.sleep(retries * 45)
                retries += 1
                
        # Raise an HTTPError if the HTTP request returned an unsuccessful status code
        response.raise_for_status()
        # Print the response status code and content
        logger.debug(f"Status Code: {response.status_code}")
        # Return the response content
        return response.json()

    def get_tenants(self):
        # Host will be changed as per env
        tenancy_host = os.getenv('HOST_TENANCY')

        url = tenancy_host + '/onb/tenancy/v1/spacecontexts'
        logger.debug(f"tenancy host url: {url}")
        response_content = self.get_tenancy(url)
        logger.debug(f"response_content: {response_content}")
        space_contexts = response_content["spaceContexts"]

        # Keep only tenant having at least one program
        select_tenants = [tenant for tenant in space_contexts
                          if tenant['programSpaces']]

        return select_tenants

    def get_tenant(self, program_id):
        tenant_info = {}
        selected_tenants = self.get_tenants()
        for tenant in selected_tenants:
            program_spaces = tenant["programSpaces"]
            for program_space in program_spaces:
                if program_space["programId"] == program_id:
                    tenant_info["programSpace"] = program_space
                    tenant_info["tenancyId"] = tenant["tenancyId"]
                    tenant_info["accountName"] = tenant["accountName"]
                    tenant_info["accountId"] = tenant["accountId"]
                    tenant_info["accountSpace"] = tenant["accountSpace"]
                    break
        return tenant_info