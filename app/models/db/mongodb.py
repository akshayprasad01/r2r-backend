import os
import pymongo
from app.logger import logger

class MongoClient:
    def __init__(self):
        self.MONGO_HOST = os.getenv("MONGO_HOST")
        self.MONGO_PORT = os.getenv("MONGO_PORT")
        self.MONGO_USER = os.getenv("MONGO_USER")
        self.MONGO_PWD = os.getenv("MONGO_PWD")
        self.MONGO_AUTHENTICATIONDB = os.getenv("MONGO_AUTHENTICATIONDB")
        self.MONGO_AUTHMECHANISM = os.getenv("MONGO_AUTHMECHANISM")
        self.MONGO_DIRECTCONNECTION = os.getenv("MONGO_DIRECTCONNECTION")

        self.connection_string = f"mongodb://{self.MONGO_USER}:{self.MONGO_PWD}@{self.MONGO_HOST}/?authSource={self.MONGO_AUTHENTICATIONDB}&authMechanism={self.MONGO_AUTHMECHANISM}&directConnection={self.MONGO_DIRECTCONNECTION}"
        self.local_connection_string = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.2.12"
    def get_connection(self):
        try:
            client = pymongo.MongoClient(self.connection_string)
            return client
        except pymongo.errors.ConnectionFailure as e:
            logger.info(f"Failed to connect to MongoDB: {e}")
            raise
    def get_connection_local(self):
        try:
            client = pymongo.MongoClient(self.local_connection_string)
            return client
        except pymongo.errors.ConnectionFailure as e:
            logger.info(f"Failed to connect to MongoDB: {e}")
            raise 