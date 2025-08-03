import json
from typing import List, Dict, Any
from etl import ETL, Extract, Transfer, Load, EndpointETL, WatchETL, FetchETL


def create_etl(etl_data: Dict[str, Any]) -> ETL:
    etl_type = etl_data['type']

    # Create extract instances
    extracts = []
    for extract_data in etl_data.get('extracts', []):
        extracts.append(Extract(
            type=extract_data['type'],
            host=extract_data['host'],
            port=extract_data['port'],
            username=extract_data['username'],
            password=extract_data['password'],
            token=extract_data['token']
        ))

    # Create transfer instances
    transfers = []
    for transfer_data in etl_data.get('transfers', etl_data.get('Transfers', [])):
        transfers.append(Transfer(operation=transfer_data))

    # Create load instances
    loads = []
    for load_data in etl_data.get('loads', []):
        loads.append(Load(
            type=load_data['type'],
            host=load_data['host'],
            port=load_data['port'],
            username=load_data['username'],
            password=load_data['password'],
            token=load_data['token'],
            bucket=load_data.get('bucket'),
            endpoint=load_data.get('endpoint'),
            topic=load_data.get('topic')
        ))

    # Map ETL types to classes
    etl_classes = {
        'Watch': WatchETL,
        'Fetch': FetchETL,
        'Endpoint': EndpointETL
    }

    # Create ETL instance
    etl_class = etl_classes.get(etl_type, ETL)
    return etl_class(
        extracts=extracts,
        transfers=transfers,
        loads=loads
    )


def create_etls(json_file: str) -> List[ETL]:
    with open(json_file, 'r') as file:
        data = json.load(file)

    etls = []
    for etl_data in data['ETLs']:
        etl = create_etl(etl_data)
        etls.append(etl)

    return etls
