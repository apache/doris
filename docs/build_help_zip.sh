#!/usr/bin/env bash
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
# This script is used to build help doc zip file
##############################################################

#!/bin/bash

set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

BUILD_DIR=build
HELP_DIR=contents
HELP_ZIP_FILE=help-resource.zip
SQL_REF_DOC_DIR=zh-CN/sql-reference/

cd $ROOT
rm -rf $BUILD_DIR $HELP_DIR $HELP_ZIP_FILE
mkdir -p $BUILD_DIR $HELP_DIR

cp -r $SQL_REF_DOC_DIR/* $HELP_DIR/

zip -r $HELP_ZIP_FILE $HELP_DIR
mv $HELP_ZIP_FILE $BUILD_DIR/


