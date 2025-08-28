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

# This file compile all protobuf files.

BUILD_DIR = ${CURDIR}/../build/proto
PROTOC = ${DORIS_THIRDPARTY}/installed/bin/protoc

SOURCES = $(wildcard *.proto)
OBJECTS = $(patsubst %.proto, ${BUILD_DIR}/%.pb.cc, ${SOURCES})
HEADERS = $(patsubst %.proto, ${BUILD_DIR}/%.pb.h, ${SOURCES})

all: prepare ${OBJECTS} ${HEADERS}
.PHONY: all prepare

prepare:
	mkdir -p ${BUILD_DIR}

${BUILD_DIR}/%.pb.h ${BUILD_DIR}/%.pb.cc: %.proto
	${PROTOC} --proto_path=. --cpp_out=${BUILD_DIR} $<

clean:
	rm -rf ${BUILD_DIR}
.PHONY: clean