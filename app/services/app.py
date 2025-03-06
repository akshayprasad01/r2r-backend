from app.services.iw.tenant import TenantService
import app.constants.iw as iw_constants
from app.helpers.iw_cache import set_value

class AppService:
  def __init__(self):
    pass

  def fetch_tenants_and_set_cache(self):
    tenant_service = TenantService()
    tenants = tenant_service.get_tenants()
    set_value(iw_constants.IW_TENANTS, tenants)