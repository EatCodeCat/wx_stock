# coding=utf-8
__author__ = 'think'

import pymongo
from bson.objectid import ObjectId


class MClient:
    def __init__(self, db, collection):
        self.client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.client[db]
        self.collection = self.db[collection]

    def insert(self, entity):
        return self.collection.insert_many(entity)

    def insert_one(self, entity):
        return self.collection.insert_one(entity)

    def ObjectId(self, _id):
        return ObjectId(_id)

    def find(self, *args, **kwargs):
        return self.collection.find()

    def find_one(self, **filter):
        return self.collection.find_one(filter)

    def replace_one(self, repacement, **filter):
        return self.collection.replace_one(filter, repacement, True)

    def update(self, update, **filter):
        return self.collection.update_many(filter, update)

    def search(self, index, page, **filter):
        count = self.collection.count(filter)
        cursor = self.collection.find(filter, skip=page * index, limit=page)
        return cursor, count

    def delete(self, _id):
        return self.collection.delete_one({"_id": _id})

    def insert_find_one(self):
        self.collection.insert_one()

    def aggregate(self, pipeline, **kwargs):
        return self.collection.aggregate(pipeline, **kwargs)

    @property
    def _id(self):
        return self.collection._id
