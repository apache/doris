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

import importlib
import importlib.resources
import pkgutil
from importlib import resources
from pkgutil import get_data


RESOURCE_SUM = (
    int(pkgutil.get_data("uuid", "data.txt").splitlines()[-1])
    + int(get_data("uuid", "data.txt").splitlines()[-1])
    + int(importlib.import_module("pkgutil").get_data("uuid", "data.txt").splitlines()[-1])
)

# files() was added in Python 3.9; read_text() also covers Python 3.8 workers.
for resource_api in (
    importlib.resources,
    resources,
    importlib.import_module("importlib.resources"),
):
    assert int(resource_api.read_text("uuid", "data.txt").splitlines()[-1]) == 7
    if hasattr(resource_api, "files"):
        assert int(resource_api.files("uuid").joinpath("data.txt").read_text().splitlines()[-1]) == 7


def evaluate_resources(x):
    if x is None:
        return None
    return x + RESOURCE_SUM
