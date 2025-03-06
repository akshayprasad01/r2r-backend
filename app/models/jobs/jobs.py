import os
from app.logger import logger
from app.models.db.mongodb import MongoClient
from pymongo import ReturnDocument
from bson.objectid import ObjectId
from app.constants.mongodb import *

class JobsModel:
    def __init__(self):
        self.current_db_name = os.getenv("MONGODB_TRANSFORMATION_SCHEMA")
        mongo_client_service = MongoClient()
        self.client = mongo_client_service.get_connection()
        self.db = self.client[self.current_db_name]
        self.transformation_jobs = self.db.get_collection(MONGODB_TRANSFORMATION_JOBS_COLLECTION)
        
    def create_job(self, job_data):
        db_job = self.transformation_jobs.insert_one(job_data)
        return db_job
    
    def update_job(self, transformation_job_id, new_job_data):
        filter = {"_id": ObjectId(transformation_job_id)}
        update_job_data = {"$set":new_job_data}
        updated_record = self.transformation_jobs.update_one(
            filter=filter,
            update=update_job_data
        )
        # return_document=ReturnDocument.AFTER
        return updated_record