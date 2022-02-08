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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/gen_cpp")

set (GEN_CPP_DIR ${GENSRC_DIR}/gen_cpp)
set(SRC_FILES
    ${GEN_CPP_DIR}/AgentService_constants.cpp
    ${GEN_CPP_DIR}/AgentService_types.cpp
    ${GEN_CPP_DIR}/BackendService_constants.cpp
    ${GEN_CPP_DIR}/BackendService.cpp
    ${GEN_CPP_DIR}/BackendService_types.cpp
    ${GEN_CPP_DIR}/PaloBrokerService_types.cpp
    ${GEN_CPP_DIR}/TDorisExternalService.cpp
    ${GEN_CPP_DIR}/DorisExternalService_types.cpp
    ${GEN_CPP_DIR}/DorisExternalService_constants.cpp
    ${GEN_CPP_DIR}/QueryPlanExtra_types.cpp
    ${GEN_CPP_DIR}/QueryPlanExtra_constants.cpp
    ${GEN_CPP_DIR}/TPaloBrokerService.cpp
    ${GEN_CPP_DIR}/HeartbeatService_constants.cpp
    ${GEN_CPP_DIR}/HeartbeatService.cpp
    ${GEN_CPP_DIR}/HeartbeatService_types.cpp
    ${GEN_CPP_DIR}/PaloInternalService_constants.cpp
    ${GEN_CPP_DIR}/PaloInternalService_types.cpp
    ${GEN_CPP_DIR}/PaloExternalDataSourceService_constants.cpp
    ${GEN_CPP_DIR}/PaloExternalDataSourceService_types.cpp
    ${GEN_CPP_DIR}/TExtDataSourceService.cpp
    ${GEN_CPP_DIR}/FrontendService.cpp
    ${GEN_CPP_DIR}/FrontendService_constants.cpp
    ${GEN_CPP_DIR}/FrontendService_types.cpp
    ${GEN_CPP_DIR}/PaloService_constants.cpp
    ${GEN_CPP_DIR}/PaloService_types.cpp
    ${GEN_CPP_DIR}/Data_constants.cpp
    ${GEN_CPP_DIR}/Data_types.cpp
    ${GEN_CPP_DIR}/DataSinks_constants.cpp
    ${GEN_CPP_DIR}/DataSinks_types.cpp
    ${GEN_CPP_DIR}/Ddl_constants.cpp
    ${GEN_CPP_DIR}/Ddl_types.cpp
    ${GEN_CPP_DIR}/Descriptors_constants.cpp
    ${GEN_CPP_DIR}/Descriptors_types.cpp
    ${GEN_CPP_DIR}/Exprs_constants.cpp
    ${GEN_CPP_DIR}/Exprs_types.cpp
    ${GEN_CPP_DIR}/MasterService_constants.cpp
    ${GEN_CPP_DIR}/MasterService_types.cpp
    ${GEN_CPP_DIR}/MetricDefs_constants.cpp
    ${GEN_CPP_DIR}/MetricDefs_types.cpp
    ${GEN_CPP_DIR}/Metrics_constants.cpp
    ${GEN_CPP_DIR}/Metrics_types.cpp
    ${GEN_CPP_DIR}/NetworkTest_constants.cpp
    ${GEN_CPP_DIR}/NetworkTest_types.cpp
    ${GEN_CPP_DIR}/NetworkTestService.cpp
    ${GEN_CPP_DIR}/Opcodes_constants.cpp
    ${GEN_CPP_DIR}/Opcodes_types.cpp
    ${GEN_CPP_DIR}/PlanNodes_constants.cpp
    ${GEN_CPP_DIR}/PlanNodes_types.cpp
    ${GEN_CPP_DIR}/Partitions_constants.cpp
    ${GEN_CPP_DIR}/Partitions_types.cpp
    ${GEN_CPP_DIR}/Planner_constants.cpp
    ${GEN_CPP_DIR}/Planner_types.cpp
    ${GEN_CPP_DIR}/RuntimeProfile_constants.cpp
    ${GEN_CPP_DIR}/RuntimeProfile_types.cpp
    ${GEN_CPP_DIR}/Status_constants.cpp
    ${GEN_CPP_DIR}/Status_types.cpp
    ${GEN_CPP_DIR}/Types_constants.cpp
    ${GEN_CPP_DIR}/Types_types.cpp
    ${GEN_CPP_DIR}/olap_common.pb.cc
    ${GEN_CPP_DIR}/olap_file.pb.cc
    ${GEN_CPP_DIR}/column_data_file.pb.cc
    ${GEN_CPP_DIR}/data.pb.cc
    ${GEN_CPP_DIR}/descriptors.pb.cc
    ${GEN_CPP_DIR}/internal_service.pb.cc
    ${GEN_CPP_DIR}/function_service.pb.cc
    ${GEN_CPP_DIR}/types.pb.cc
    ${GEN_CPP_DIR}/segment_v2.pb.cc
    #$${GEN_CPP_DIR}/opcode/functions.cc
    #$${GEN_CPP_DIR}/opcode/vector-functions.cc
    #$${GEN_CPP_DIR}/opcode/opcode-registry-init.cc
)

# keep everything in one library, the object files reference 
# each other
add_library(DorisGen STATIC ${SRC_FILES})

# Setting these files as code-generated lets make clean and incremental builds work
# correctly
set_source_files_properties(${SRC_FILES} PROPERTIES GENERATED TRUE)

#add_dependencies(DorisGen thrift-cpp)
#add_dependencies(Opcode function)

