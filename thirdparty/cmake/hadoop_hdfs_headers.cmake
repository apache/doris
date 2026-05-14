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

# hadoop_hdfs_headers: expose hadoop hdfs headers from source tree
# Doris uses #include <hadoop_hdfs_3_4/hdfs.h>
set(HADOOP_34_SRC "${TP_SOURCE_DIR}/doris-thirdparty-hadoop-3.4.2.2-for-doris/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs")
set(HADOOP_33_SRC "${TP_SOURCE_DIR}/doris-thirdparty-hadoop-3.3.6.6-for-doris/hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfs")
set(HDFS_NESTED_DIR "${CMAKE_CURRENT_BINARY_DIR}/hadoop_hdfs_headers/include")
file(MAKE_DIRECTORY "${HDFS_NESTED_DIR}/hadoop_hdfs_3_4")
file(MAKE_DIRECTORY "${HDFS_NESTED_DIR}/hadoop_hdfs")

# Copy hdfs.h from hadoop 3.4 source
if(EXISTS "${HADOOP_34_SRC}/include/hdfs/hdfs.h")
    file(COPY "${HADOOP_34_SRC}/include/hdfs/hdfs.h" DESTINATION "${HDFS_NESTED_DIR}/hadoop_hdfs_3_4")
endif()
# Copy hdfs.h from hadoop 3.3 source (for hadoop_hdfs/)
if(EXISTS "${HADOOP_33_SRC}/include/hdfs/hdfs.h")
    file(COPY "${HADOOP_33_SRC}/include/hdfs/hdfs.h" DESTINATION "${HDFS_NESTED_DIR}/hadoop_hdfs")
endif()

add_library(hadoop_hdfs_headers INTERFACE)
target_include_directories(hadoop_hdfs_headers SYSTEM INTERFACE ${HDFS_NESTED_DIR})
