from elasticsearch import Elasticsearch, helpers
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
import time

from nosql_client import NoSQLClient


class ElasticsearchClient(NoSQLClient):
    def __init__(self, hosts: Union[str, List[str]], **kwargs):
        self.client = Elasticsearch(hosts, **kwargs)

    def index(self, index: str, document: Dict[str, Any], id: Optional[str] = None) -> Dict[str, Any]:
        return self.client.index(index=index, document=document, id=id)

    def bulk(self, actions: List[Dict[str, Any]]) -> tuple:
        return helpers.bulk(self.client, actions)

    def get(self, index: str, id: str) -> Dict[str, Any]:
        return self.client.get(index=index, id=id)

    def search(self, index: str, query: Dict[str, Any], size: int = 10, from_: int = 0) -> Dict[str, Any]:
        return self.client.search(index=index, query=query, size=size, from_=from_)

    def update(self, index: str, id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.update(index=index, id=id, body=body)

    def update_by_query(self, index: str, query: Dict[str, Any], script: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.client.update_by_query(index=index, query=query, script=script)

    def delete(self, index: str, id: str) -> Dict[str, Any]:
        return self.client.delete(index=index, id=id)

    def delete_by_query(self, index: str, query: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.delete_by_query(index=index, query=query)

    def create_index(self, index: str, mappings: Optional[Dict[str, Any]] = None, settings: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = {}
        if mappings:
            body['mappings'] = mappings
        if settings:
            body['settings'] = settings
        return self.client.indices.create(index=index, body=body)

    def delete_index(self, index: str) -> Dict[str, Any]:
        return self.client.indices.delete(index=index)

    def exists_index(self, index: str) -> bool:
        return self.client.indices.exists(index=index)

    def get_mapping(self, index: str) -> Dict[str, Any]:
        return self.client.indices.get_mapping(index=index)

    def put_mapping(self, index: str, mapping: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.indices.put_mapping(index=index, body=mapping)

    def get_settings(self, index: str) -> Dict[str, Any]:
        return self.client.indices.get_settings(index=index)

    def put_settings(self, index: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.indices.put_settings(index=index, body=settings)

    def reindex(self, source_index: str, dest_index: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        body = body or {"source": {"index": source_index},
                        "dest": {"index": dest_index}}
        return self.client.reindex(body=body)

    def count(self, index: str, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.client.count(index=index, body={"query": query} if query else None)

    def mget(self, index: str, ids: List[str]) -> Dict[str, Any]:
        return self.client.mget(index=index, body={"ids": ids})

    def exists(self, index: str, id: str) -> bool:
        return self.client.exists(index=index, id=id)

    def scroll(self, scroll_id: str, scroll: str = "5m") -> Dict[str, Any]:
        return self.client.scroll(scroll_id=scroll_id, scroll=scroll)

    def clear_scroll(self, scroll_id: str) -> Dict[str, Any]:
        return self.client.clear_scroll(scroll_id=scroll_id)

    def create_alias(self, index: str, alias: str) -> Dict[str, Any]:
        return self.client.indices.put_alias(index=index, name=alias)

    def delete_alias(self, index: str, alias: str) -> Dict[str, Any]:
        return self.client.indices.delete_alias(index=index, name=alias)

    def get_aliases(self, index: Optional[str] = None) -> Dict[str, Any]:
        return self.client.indices.get_alias(index=index)

    def refresh(self, index: str) -> Dict[str, Any]:
        return self.client.indices.refresh(index=index)

    def watch(self, index: str, query: Optional[Dict[str, Any]] = None, interval: float = 1.0,
              timestamp_field: str = "@timestamp") -> List[Dict[str, Any]]:
        last_time = datetime.utcnow().isoformat()
        while True:
            query_body = {
                "query": {
                    "bool": {
                        "filter": [
                            {"range": {timestamp_field: {"gt": last_time}}}
                        ]
                    }
                }
            }
            if query:
                query_body["query"]["bool"]["filter"].append(query)
            result = self.search(
                index=index, query=query_body["query"], size=100)
            hits = result.get("hits", {}).get("hits", [])
            if hits:
                last_time = max(hit["_source"].get(
                    timestamp_field, last_time) for hit in hits)
                yield from hits
            time.sleep(interval)

    def close(self) -> None:
        self.client.close()
