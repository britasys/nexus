import json
from typing import List, Dict, Any
from etl import ETL, Extract, Transform, Load


def create_etl(etl_data: Dict[str, Any]) -> ETL:

    # Create extract instances
    extract_classes = {
        'MongoDB': Extract,
        'PostgresSQL': Extract,
        'MySQL': Extract,
        'Elasticsearch': Extract,
        'ApacheKafka': Extract
    }

    extracts = []
    for extract_data in etl_data.get('extracts', []):
        extract_class = extract_classes.get(extract_data['source'], Extract)
        extracts.append(extract_class(
            type=extract_data['type'],
            source=extract_data['source'],
            host=extract_data['host'],
            port=extract_data['port'],
            username=extract_data['username'],
            password=extract_data['password'],
            token=extract_data['token']
        ))

    # Create transfer instances
    transforms = []
    for transfer_data in etl_data.get('transforms', etl_data.get('transforms', [])):
        transforms.append(Transform(operation=transfer_data))

    # Create load instances
    load_classes = {
        'MongoDB': Load,
        'PostgresSQL': Load,
        'MySQL': Load,
        'Elasticsearch': Load,
        'ApacheKafka': Load
    }

    loads = []
    for load_data in etl_data.get('loads', []):
        load_class = load_classes.get(load_data['target'], Load)
        loads.append(load_class(
            target=load_data['target'],
            host=load_data['host'],
            port=load_data['port'],
            username=load_data['username'],
            password=load_data['password'],
            token=load_data['token'],
            bucket=load_data.get('bucket'),
            endpoint=load_data.get('endpoint'),
            topic=load_data.get('topic')
        ))

    # Create ETL instance
    return ETL(
        extracts=extracts,
        transforms=transforms,
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
