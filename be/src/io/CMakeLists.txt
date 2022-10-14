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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/io")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/io")

set(IO_FILES
    broker_reader.cpp
    broker_writer.cpp
    buffered_reader.cpp
    file_factory.cpp
    hdfs_builder.cpp
    hdfs_file_reader.cpp
    hdfs_reader_writer.cpp
    hdfs_writer.cpp
    local_file_reader.cpp
    local_file_writer.cpp
    s3_reader.cpp
    s3_writer.cpp
    fs/file_system_map.cpp
    fs/local_file_reader.cpp
    fs/local_file_system.cpp
    fs/local_file_writer.cpp
    fs/s3_file_reader.cpp
    fs/s3_file_system.cpp
    cache/dummy_file_cache.cpp
    cache/file_cache.cpp
    cache/file_cache_manager.cpp
    cache/sub_file_cache.cpp
    cache/whole_file_cache.cpp
)

add_library(IO STATIC
    ${IO_FILES}
)
