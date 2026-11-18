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
from . import logging


RESOURCE_SUM = (
    int(pkgutil.get_data("udfpkg", "data.txt").splitlines()[-1])
    + int(get_data("udfpkg", "data.txt").splitlines()[-1])
    + int(importlib.import_module("pkgutil").get_data("udfpkg", "data.txt").splitlines()[-1])
)
for resource_api in (importlib.resources, resources, importlib.import_module("importlib.resources")):
    assert int(resource_api.read_text("udfpkg", "data.txt").splitlines()[-1]) == 7
    if hasattr(resource_api, "files"):
        assert int(resource_api.files("udfpkg").joinpath("data.txt").read_text().splitlines()[-1]) == 7


def evaluate_resources(value):
    if importlib.import_module(".logging", "udfpkg") is not logging:
        raise RuntimeError("udfpkg.logging was loaded more than once")
    return None if value is None else value + RESOURCE_SUM
