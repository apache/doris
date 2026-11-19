# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Register the checked-in Delta reader fixtures as external tables in local OSS UC."""

import json
import os
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen


API = os.environ["UC_TEST_SERVER"].rstrip("/") + "/api/2.1/unity-catalog"
CATALOG = "doris_delta"
FIXTURES = Path(__file__).resolve().with_name("delta_reader_edges")


def fetch_or_create(resource, full_name, payload):
    try:
        with urlopen(f"{API}/{resource}/{full_name}", timeout=15) as response:
            return json.load(response)
    except HTTPError as error:
        if error.code != 404:
            raise
    request = Request(f"{API}/{resource}", data=json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
    with urlopen(request, timeout=15) as response:
        return json.load(response)


def latest_metadata(table):
    for version in sorted((table / "_delta_log").glob("*.json"), reverse=True):
        for line in version.read_text(encoding="utf-8").splitlines():
            action = json.loads(line)
            if "metaData" in action:
                return action["metaData"]
    raise ValueError(f"No Delta schema in {table}")


def columns(table):
    metadata = latest_metadata(table)
    partitions = metadata["partitionColumns"]
    result = []
    for position, field in enumerate(json.loads(metadata["schemaString"])["fields"]):
        name, field_type = field["name"], field["type"]
        type_name = "INT" if field_type == "integer" else field_type.partition("(")[0].upper()
        column = dict(name=name, type_name=type_name, type_json=json.dumps(field),
                      type_text="bigint" if field_type == "long" else field_type,
                      position=position, nullable=field["nullable"])
        if name in partitions:
            column["partition_index"] = partitions.index(name)
        result.append(column)
    return result


schema = FIXTURES.name
fetch_or_create("schemas", f"{CATALOG}.{schema}", dict(catalog_name=CATALOG, name=schema))
for log in sorted(FIXTURES.glob("*/_delta_log")):
    table = log.parent
    full_name = f"{CATALOG}.{schema}.{table.name}"
    location = table.as_uri()
    payload = dict(catalog_name=CATALOG, schema_name=schema, name=table.name,
                   table_type="EXTERNAL", data_source_format="DELTA",
                   storage_location=location, columns=columns(table))
    result = fetch_or_create("tables", full_name, payload)
    if (result["table_type"], result["data_source_format"], result["storage_location"].rstrip("/")) != (
            "EXTERNAL", "DELTA", location):
        raise ValueError(f"{full_name} is already registered with a different type or location")
    print(f"{full_name} -> {location}")
