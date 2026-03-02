"""
NYC Taxi Data Pipeline using dlt REST API Source

Fetches paginated taxi data from the Data Engineering Zoomcamp API
and loads it into DuckDB.
"""

import dlt
from dlt.sources.rest_api import rest_api_source


def create_taxi_source():
    """
    Create a REST API source for NYC taxi data.
    
    API returns 1,000 records per page and pagination stops
    when an empty page is returned.
    """
    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            },
            "resources": [
                {
                    "name": "rides",
                    "endpoint": {
                        "path": "/",
                        "paginator": {
                            "type": "page_number",
                            "page_param": "page",
                            "base_page": 1,
                            "total_path": None,  # Stop when empty response
                        },
                    },
                },
            ],
        }
    )


def load_taxi_data():
    """Load NYC taxi data into DuckDB."""
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
    )

    # Create the source
    taxi_source = create_taxi_source()

    # Run the pipeline
    load_info = pipeline.run(taxi_source)
    print(load_info)

    return load_info


if __name__ == "__main__":
    load_taxi_data()
