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

DORIS_INC = ${CURDIR}/../../../thirdparty/installed/include
DORIS_LIB = ${CURDIR}/../../../thirdparty/installed/lib64
CXX = g++
CPPFLAGS += -I${DORIS_INC}
CXXFLAGS += -std=c++11

LDFLAGS += -I. -Wl,--start-group ${DORIS_LIB}/libglog.a ${DORIS_LIB}/libevent.a ${DORIS_LIB}/libz.a ${DORIS_LIB}/libgflags.a ${DORIS_LIB}/libbrpc.a  ${DORIS_LIB}/libprotobuf.a ${DORIS_LIB}/libssl.a  ${DORIS_LIB}/libcrypto.a ${DORIS_LIB}/libleveldb.a -Wl,--end-group  -static-libstdc++ -static-libgcc -pthread -ldl -lc

PROTOC = ${CURDIR}/../../../thirdparty/installed/bin/protoc


PROTOS_PATH = ${CURDIR}/proto

vpath %.proto $(PROTOS_PATH)

all: system-check function_server_demo 

function_server_demo: types.pb.o function_service.pb.o cpp_function_service_demo.o
	$(CXX) $^ $(LDFLAGS) -o $@

.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

clean:
	rm -f *.o *.pb.cc *.pb.h function_server_demo

PROTOC_CHECK_CMD = $(PROTOC) --version | grep -q libprotoc.3
HAS_PROTOC = $(shell $(PROTOC) > /dev/null && echo true || echo false)
ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC = $(shell $(PROTOC_CHECK_CMD) 2> /dev/null && echo true || echo false)
endif

SYSTEM_OK = false
ifeq ($(HAS_VALID_PROTOC),true)
SYSTEM_OK = true
endif

system-check:
ifneq ($(HAS_VALID_PROTOC),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.14.0 installed in your path."
	@echo "Please install Google protocol buffers 3.14.0 and its compiler."
	@echo "You can find it here:"
	@echo
	@echo "   https://github.com/protocolbuffers/protobuf/releases/tag/v3.14.0"
	@echo
	@echo "Here is what I get when trying to evaluate your version of protoc:"
	@echo
	-$(PROTOC) --version
	@echo
	@echo
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif
