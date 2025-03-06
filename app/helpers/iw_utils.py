import os
import json
import base64
import requests
# import jwt
# from cryptography.x509 import load_pem_x509_certificate
# from cryptography.hazmat.backends import default_backend
# from cryptography.hazmat.primitives import serialization
# from jwt.exceptions import InvalidTokenError, ExpiredSignatureError, InvalidSignatureError
from app.logger import logger
from app.services.app import AppService
from app.helpers.iw_cache import get_value
import app.constants.iw as iw_constants


KEYCLOAK_URL = os.getenv("TRANSFORMATION_KC_URL")
KEYCLOAK_REALM = os.getenv("TRANSFORMATION_KC_REALM")
KEYCLOAK_PUBLIC_KEY_URL = f"{KEYCLOAK_URL}/realms/{KEYCLOAK_REALM}/protocol/openid-connect/certs"

def header_authorization_encode(data):
    # Convert the JSON object to a string
    json_string = json.dumps(data)

    # Encode the string to bytes using UTF-8 encoding
    json_bytes = json_string.encode('utf-8')

    # Encode the bytes to a Base64 string
    base64_bytes = base64.b64encode(json_bytes)

    # Convert the Base64 bytes to a string
    base64_string = base64_bytes.decode('utf-8')

    return base64_string


def header_authorization_decode(data):
    decoded_json_bytes = base64.b64decode(data)
    decoded_string = decoded_json_bytes.decode('utf-8')
    decoded_data = json.loads(decoded_string)
    return decoded_data

def auth_validate(authorization):
    try:
        decoded_data = header_authorization_decode(authorization)
        if "requesterId" in decoded_data and decoded_data["requesterId"]:
            return str(decoded_data["requesterId"])
        else:
            return None
    except Exception as e:
        logger.info(f"auth_validate:authorization string: {authorization}, error: {e}")
        return None

def get_program_id(authorization):
    try:
        decoded_data = header_authorization_decode(authorization)
        if "programId" in decoded_data and decoded_data["programId"]:
            return str(decoded_data["programId"])
        else:
            return None
    except Exception as e:
        logger.info(f"auth_validate:authorization string: {authorization}, error: {e}")
        return None
    
def get_tenants():
    tenants = get_value(iw_constants.IW_TENANTS)
    if tenants:
        return tenants
    else:
        app_service = AppService()
        app_service.fetch_tenants_and_set_cache()
        tenants = get_value(iw_constants.IW_TENANTS)
        return tenants

def get_tenant(program_id):
    tenant_info = {}
    selected_tenants = get_tenants()
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

def get_tenant_program_space_name(program_id):
    tenant_info = {}
    selected_tenants = get_tenant(program_id)
    tenant_program_space_name = selected_tenants["accountSpace"]
    return tenant_program_space_name