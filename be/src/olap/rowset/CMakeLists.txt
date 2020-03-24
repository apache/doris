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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/olap/rowset")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/olap/rowset")
  
add_library(Rowset STATIC
    bit_field_reader.cpp
    bit_field_writer.cpp
    column_data.cpp
    column_reader.cpp
    column_writer.cpp
    column_data_writer.cpp
    segment_group.cpp
    run_length_byte_reader.cpp
    run_length_byte_writer.cpp
    run_length_integer_reader.cpp
    run_length_integer_writer.cpp
    unique_rowset_id_generator.cpp
    segment_reader.cpp
    segment_writer.cpp
    rowset.cpp
    rowset_factory.cpp
    rowset_converter.cpp
    rowset_meta_manager.cpp
    alpha_rowset.cpp
    alpha_rowset_reader.cpp
    alpha_rowset_writer.cpp
    alpha_rowset_meta.cpp
    beta_rowset.cpp
    beta_rowset_reader.cpp
    beta_rowset_writer.cpp)
