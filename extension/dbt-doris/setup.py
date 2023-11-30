#!/usr/bin/env python
# encoding: utf-8

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

from setuptools import find_namespace_packages, setup

package_name = "dbt-doris"
# make sure this always matches dbt/adapters/{adapter}/__version__.py
package_version = "0.3.4"
dbt_core_version = "1.5.0"
description = """The doris adapter plugin for dbt """

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="long2ice,catpineapple,JNSimba",
    author_email="1391869588@qq.com",
    url="https://github.com/apache/doris/tree/master/extension/dbt-doris",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        "dbt-core~={}".format(dbt_core_version),
        "mysql-connector-python>=8.0.0,<8.1",
        "urllib3~=1.0",
    ],
    python_requires=">=3.7.2",
)
