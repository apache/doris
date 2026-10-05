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

# This file is to make all generated file needed by fe and be.

BUILD_DIR = ${CURDIR}/build/

all: subdirs
.PHONY: all

# build all subdir
SUBDIR = script proto thrift
subdirs: ${SUBDIR}
.PHONY: subdirs ${SUBDIR}
${SUBDIR}:
	$(MAKE) -C $@
# script will product new thrift file.
thrift: script

clean:
	rm -rf ${BUILD_DIR}
.PHONY: clean
