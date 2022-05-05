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

##############################################################
# This script is used to compile Apache Doris(incubating).
# Usage:
#    sh build.sh --help
#
# You need to make sure all thirdparty libraries have been
# compiled and installed correctly.
##############################################################

from setuptools import setup

packages = ["dbt", "dbt.adapters.doris", "dbt.include.doris"]

package_data = {
    "": ["*"],
    "dbt.include.doris": [
        "macros/adapters/*",
        "macros/get_custom_name/*",
        "macros/materializations/incremental/*",
        "macros/materializations/partition/*",
        "macros/materializations/seed/*",
        "macros/materializations/snapshot/*",
        "macros/materializations/table/*",
        "macros/materializations/view/*",
    ],
}

install_requires = ["dbt-core", "mysqlclient"]

setup_kwargs = {
    "name": "dbt-doris",
    "version": "0.1.0",
    "description": "The doris adapter plugin for dbt",
    "long_description": None,
    "maintainer": None,
    "maintainer_email": None,
    "url": None,
    "packages": packages,
    "package_data": package_data,
    "install_requires": install_requires,
    "python_requires": ">=3.7,<4.0",
}


setup(**setup_kwargs)
