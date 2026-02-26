from __future__ import annotations

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_resources


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/"
ENDPOINT_PATH = "data_engineering_zoomcamp_api"


@dlt.source
def nyc_taxi_rest_api_source():
    """REST API source for NYC taxi data."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": BASE_URL,
        },
        "resource_defaults": {
            "endpoint": {
                # Use page-number pagination and stop when an empty page is returned.
                "paginator": {
                    "type": "page_number",
                    "base_page": 1,
                    "page_param": "page",
                    # This API does not return total pages; paginate until an empty page.
                    "total_path": None,
                    "stop_after_empty_page": True,
                }
            }
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    "path": ENDPOINT_PATH,
                    # API returns a JSON array per page
                    "data_selector": "$",
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)

