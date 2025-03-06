import os
from app.logger import logger
from app.models.db.mongodb import MongoClient
from pymongo.errors import PyMongoError
from app.helpers.common import get_current_timestamp_iso_format
import json
from bson import json_util
from bson import ObjectId

class DocumentModel:
    def __init__(self, db_name):
        self.mongo_client = MongoClient()
        self.client = self.mongo_client.get_connection()
        self.current_db = self.client[db_name]
        self.collection = None

    def create_or_update(self, current_user, collection_name, document_data, id=None):
        try:
            current_timestamp = get_current_timestamp_iso_format()

            if type(document_data) is not dict:
                document_data = document_data.dict()

            self.collection = self.current_db[collection_name]

            doc = {}
            doc["Status"] = "ACTIVE"
            doc["updatedBy"] = current_user
            doc["updatedAt"] = current_timestamp

            if id is None:
                if not self.search_document(collection_name=collection_name, filter={'name': document_data['name']}):    
                    doc.update(document_data)
                    
                    doc["createdBy"] = current_user
                    doc["createdAt"] = current_timestamp
                    logger.info(doc)
                    create_document_response = self.collection.insert_one(doc)
                    logger.info(create_document_response)
                    inserted_id = create_document_response.inserted_id
                    return_data = {
                            "status": True,
                            "message": f"Successfully created a document '{document_data['name']}'.!!",
                            "Id": f"{inserted_id}",
                            "_id": f"{inserted_id}"
                    }
                else:
                    return_data = {
                            "status": False,
                            "message": f"Document with name already exists !!",
                            "Id": None,
                            "_id": None
                    }
            else:
                update_doc = self.get_document(collection_name, id)
                for key, value in document_data.items():
                    if value is not None:
                        update_doc[key] = document_data[key]
                
                filter = {
                    "_id": ObjectId(id)
                }
                new_values = {"$set": update_doc}
                updated_doc = self.collection.update_one(filter, new_values)
                return_data = {
                    "status": True,
                    "message": f"Successfully updated a reconciliation !!",
                    "Id": id,
                    "_id": id
                }
            # logger.info(return_data)
            return return_data
        except Exception as e:
            logger.info("DocumentModel-create_or_update - catch: " + str(e))
            return {"status": False, "error": str(e)}

    def get_document(self, collection_name, document_id, client_id = None):
        try:
            filter = {
                "_id": ObjectId(document_id)
            }
            document = self.search_document(collection_name, filter)
            if document:
                return document
            else:
                return {}
        except Exception as e:
            logger.info("DocumentModel-get_document - catch: " + str(e))
            return None

    def get_all_documents(self, collection_name, filter = None):
        try:
            if filter:
                documents = self.search_document(collection_name, filter=filter)
            else:
                documents = self.search_document(collection_name)
            logger.info(f"Document Model: {documents}")
            return documents
        except Exception as e:
            logger.info("DocumentModel-get_document - catch: " + str(e))
            return None

    def search_document(self, collection_name, filter = None):
        try:
            logger.info("Inside Search Document")
            self.collection = self.current_db[collection_name]
            logger.info(self.collection)
            selector_list = {}

            if filter is not None:
                if "_id" in  filter:
                    id_filter = {"_id": filter["_id"]}
                    selector_list.update(id_filter)
                    found_document = self.collection.find_one(selector_list)
                    if found_document:
                        return found_document
                else:
                    selector_list.update(filter)
                
                found_document = self.collection.find(selector_list)
                logger.info(found_document)
                doc_list = [doc for doc in found_document]
                logger.info(doc_list)
                if doc_list:
                    for i in range(len(doc_list)):
                        doc_list[i]['_id'] = str(doc_list[i]['_id'])
                    return doc_list
                else:
                    return None
            else:
                found_document = self.collection.find()
                doc_list = [doc for doc in found_document]
                logger.info(doc_list)
                if doc_list:
                    for i in range(len(doc_list)):
                        doc_list[i]['_id'] = str(doc_list[i]['_id'])
                    return doc_list
                else:
                    return None
        except Exception as e:
            logger.info(f"search_document - catch: {str(e)}")
            return {"error": str(e)}