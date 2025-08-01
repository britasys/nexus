from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult, UpdateResult, DeleteResult
from bson import ObjectId
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from nosql_client import NoSQLClient


class MongoDBClient(NoSQLClient):
    def __init__(self, connection_string: str, database_name: str):
        self.client = MongoClient(connection_string)
        self.db: Database = self.client[database_name]

    def get_collection(self, collection_name: str) -> Collection:
        return self.db[collection_name]

    def insert_one(self, collection_name: str, document: Dict[str, Any]) -> InsertOneResult:
        return self.get_collection(collection_name).insert_one(document)

    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]], ordered: bool = True) -> InsertManyResult:
        return self.get_collection(collection_name).insert_many(documents, ordered=ordered)

    def find_one(self, collection_name: str, query: Dict[str, Any] = {}, projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        return self.get_collection(collection_name).find_one(query, projection)

    def find(self, collection_name: str, query: Dict[str, Any] = {}, projection: Optional[Dict[str, Any]] = None,
             sort: Optional[List[tuple]] = None, limit: int = 0, skip: int = 0) -> List[Dict[str, Any]]:
        cursor = self.get_collection(collection_name).find(query, projection)
        if sort:
            cursor = cursor.sort(sort)
        if skip:
            cursor = cursor.skip(skip)
        if limit:
            cursor = cursor.limit(limit)
        return list(cursor)

    def update_one(self, collection_name: str, query: Dict[str, Any], update: Dict[str, Any],
                   upsert: bool = False) -> UpdateResult:
        return self.get_collection(collection_name).update_one(query, update, upsert=upsert)

    def update_many(self, collection_name: str, query: Dict[str, Any], update: Dict[str, Any],
                    upsert: bool = False) -> UpdateResult:
        return self.get_collection(collection_name).update_many(query, update, upsert=upsert)

    def replace_one(self, collection_name: str, query: Dict[str, Any], replacement: Dict[str, Any],
                    upsert: bool = False) -> UpdateResult:
        return self.get_collection(collection_name).replace_one(query, replacement, upsert=upsert)

    def delete_one(self, collection_name: str, query: Dict[str, Any]) -> DeleteResult:
        return self.get_collection(collection_name).delete_one(query)

    def delete_many(self, collection_name: str, query: Dict[str, Any]) -> DeleteResult:
        return self.get_collection(collection_name).delete_many(query)

    def count_documents(self, collection_name: str, query: Dict[str, Any] = {}) -> int:
        return self.get_collection(collection_name).count_documents(query)

    def aggregate(self, collection_name: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return list(self.get_collection(collection_name).aggregate(pipeline))

    def create_index(self, collection_name: str, keys: Union[str, List[tuple]], unique: bool = False, **kwargs) -> str:
        return self.get_collection(collection_name).create_index(keys, unique=unique, **kwargs)

    def drop_index(self, collection_name: str, index_name: str) -> None:
        self.get_collection(collection_name).drop_index(index_name)

    def list_indexes(self, collection_name: str) -> List[Dict[str, Any]]:
        return list(self.get_collection(collection_name).list_indexes())

    def create_collection(self, collection_name: str, **kwargs) -> Collection:
        return self.db.create_collection(collection_name, **kwargs)

    def drop_collection(self, collection_name: str) -> None:
        self.db.drop_collection(collection_name)

    def list_collections(self) -> List[Dict[str, Any]]:
        return list(self.db.list_collection_names())

    def bulk_write(self, collection_name: str, operations: List[Any], ordered: bool = True) -> Any:
        return self.get_collection(collection_name).bulk_write(operations, ordered=ordered)

    def watch(self, collection_name: str, pipeline: Optional[List[Dict[str, Any]]] = None,
              full_document: Optional[str] = None) -> Any:
        return self.get_collection(collection_name).watch(pipeline, full_document=full_document)

    def close(self) -> None:
        self.client.close()

    def get_database(self, database_name: str) -> Database:
        return self.client[database_name]

    def start_session(self, **kwargs) -> Any:
        return self.client.start_session(**kwargs)

    def with_transaction(self, callback: callable, **kwargs) -> Any:
        with self.start_session() as session:
            return session.with_transaction(callback, **kwargs)

    def rename_collection(self, collection_name: str, new_name: str, dropTarget: bool = False) -> None:
        self.get_collection(collection_name).rename(
            new_name, dropTarget=dropTarget)

    def distinct(self, collection_name: str, field: str, query: Dict[str, Any] = {}) -> List[Any]:
        return self.get_collection(collection_name).distinct(field, query)

    def find_one_and_update(self, collection_name: str, query: Dict[str, Any], update: Dict[str, Any],
                            projection: Optional[Dict[str, Any]] = None, upsert: bool = False,
                            return_document: bool = False) -> Optional[Dict[str, Any]]:
        return self.get_collection(collection_name).find_one_and_update(
            query, update, projection=projection, upsert=upsert, return_document=return_document)

    def find_one_and_replace(self, collection_name: str, query: Dict[str, Any], replacement: Dict[str, Any],
                             projection: Optional[Dict[str, Any]] = None, upsert: bool = False,
                             return_document: bool = False) -> Optional[Dict[str, Any]]:
        return self.get_collection(collection_name).find_one_and_replace(
            query, replacement, projection=projection, upsert=upsert, return_document=return_document)

    def find_one_and_delete(self, collection_name: str, query: Dict[str, Any],
                            projection: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        return self.get_collection(collection_name).find_one_and_delete(query, projection=projection)
