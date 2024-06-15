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

# This file used to compile all thrift files.
BUILD_DIR = ${CURDIR}/../build/

THRIFT = ${DORIS_THIRDPARTY}/installed/bin/thrift

SOURCES = $(shell find ${CURDIR} -name "*.thrift")
OBJECTS = $(patsubst ${CURDIR}/%.thrift, ${BUILD_DIR}/gen_cpp/%_types.cpp, ${SOURCES})

GEN_SOURCES = $(shell find ${THRIFT} -name "*.thrift")
GEN_OBJECTS = $(patsubst ${BUILD_DIR}/thrift/%.thrift, ${BUILD_DIR}/gen_cpp/%_types.cpp, ${GEN_SOURCES})

all: ${GEN_OBJECTS} ${OBJECTS}
.PHONY: all

$(shell mkdir -p ${BUILD_DIR}/gen_java)

THRIFT_CPP_ARGS = -I ${CURDIR} -I ${BUILD_DIR}/thrift/ --gen cpp:moveable_types -out ${BUILD_DIR}/gen_cpp --allow-64bit-consts -strict
THRIFT_JAVA_ARGS = -I ${CURDIR} -I ${BUILD_DIR}/thrift/ --gen java:fullcamel -out ${BUILD_DIR}/gen_java --allow-64bit-consts -strict

${BUILD_DIR}/gen_cpp:
	mkdir -p $@
# handwrite thrift
${BUILD_DIR}/gen_cpp/%_types.cpp: ${CURDIR}/%.thrift | ${BUILD_DIR}/gen_cpp
	${THRIFT} ${THRIFT_CPP_ARGS} $<
	${THRIFT} ${THRIFT_JAVA_ARGS} $<

# generated thrift
${BUILD_DIR}/gen_cpp/%_types.cpp: ${BUILD_DIR}/thrift/%.thrift | ${BUILD_DIR}/gen_cpp
	${THRIFT} ${THRIFT_CPP_ARGS} $<
	${THRIFT} ${THRIFT_JAVA_ARGS} $<
