from app.models.jobs.jobs import JobsModel
from app.logger import logger

class JobsService:
    def __init__(self):
        self.standard_response = "This is Job Service"

    def create_job(self, client_id, recon_id):
        try:
            JOB_STATUS_PENDING="PENDING"
            create_data = {
                "clientId": client_id,
                "reconId": recon_id,
                "status": JOB_STATUS_PENDING
            }
            jobs_model = JobsModel()
            job_data = jobs_model.create_job(create_data)
            job_id = str(job_data.inserted_id)
            return job_id
        except Exception as e:
            logger.info(f"Error in create Job Job service :: {str(e)}")


    def update_job(self, transformation_job_id, status, databricks_job_id=None, databricks_run_id=None, databricks_job_status=None, databricks_output=None):
        
        update_data = {
            "status": status,
        }
        if databricks_job_id:
            update_data.update({"databricksJobId": databricks_job_id})
        if databricks_job_status:
            update_data.update({"databricksJobStatus": databricks_job_status})
        if databricks_run_id:
            update_data.update({"databricksRunId": databricks_run_id})
        if databricks_output:
            update_data.update({"databricksOutput": databricks_output})
        jobs_model = JobsModel()
        updated_job_data = jobs_model.update_job(transformation_job_id, update_data)
        return {
            "status": True,
            "Job Data": updated_job_data,
            "message": "Job status updated"
        }

    