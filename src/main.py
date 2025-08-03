
from etl_helper import create_etls


if __name__ == "__main__":
    json_file = "pipeline.json"  # Replace with the path to your JSON file
    etls_array = create_etls(json_file)

    # Print the resulting ETL instances for verification
    for etl in etls_array:
        print("Extracts:", [vars(extract) for extract in etl.extracts])
        print("Transfers:", [vars(transfer) for transfer in etl.transfers])
        print("Loads:", [vars(load) for load in etl.loads])
        etl.run()
        print("--------------------------------------------------------")

    print("END")
