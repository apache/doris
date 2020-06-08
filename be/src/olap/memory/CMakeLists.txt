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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/olap/memory")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/olap/memory")
  
add_library(Memory STATIC
    buffer.cpp
    column.cpp
    column_block.cpp
    column_delta.cpp
    delta_index.cpp
    hash_index.cpp
    mem_tablet.cpp
    mem_tablet_scan.cpp
    mem_sub_tablet.cpp
    partial_row_batch.cpp
    row_block.cpp
    schema.cpp
    write_txn.cpp
)
