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

# Collect all source files in this directory
file(GLOB_RECURSE VECTOR_SEARCH_TEST_FILES CONFIGURE_DEPENDS *.cpp)

# Create a static library for vector search tests
add_library(vector_search_test STATIC ${VECTOR_SEARCH_TEST_FILES})

# Suppress warnings for this library to avoid compilation failures from third-party dependencies
target_compile_options(vector_search_test PRIVATE 
    -Wno-everything             # Suppress all warnings
    -fno-access-control         # Disable access control error
)

# Link necessary libraries
target_link_libraries(vector_search_test 
    PUBLIC 
        ann_index                  # Vector library that contains faiss integration
        gtest                   # Google Test library
        gmock                   # Google Mock library
)

# Add dependencies to ensure proper build order
add_dependencies(vector_search_test ann_index faiss)