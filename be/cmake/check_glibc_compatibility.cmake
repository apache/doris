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

if (NOT DEFINED ARTIFACT OR NOT EXISTS "${ARTIFACT}")
    message(FATAL_ERROR "Cannot check glibc compatibility: '${ARTIFACT}' does not exist")
endif()

if (NOT DEFINED BASELINE OR BASELINE STREQUAL "")
    message(FATAL_ERROR "Cannot check glibc compatibility: BASELINE is not set")
endif()

if (NOT DEFINED OBJDUMP OR OBJDUMP STREQUAL "" OR NOT EXISTS "${OBJDUMP}")
    find_program(OBJDUMP NAMES objdump llvm-objdump REQUIRED)
endif()

execute_process(
    COMMAND "${OBJDUMP}" -T "${ARTIFACT}"
    RESULT_VARIABLE objdump_result
    OUTPUT_VARIABLE dynamic_symbol_table
    ERROR_VARIABLE objdump_error)

if (NOT objdump_result EQUAL 0)
    message(FATAL_ERROR
        "Cannot read the dynamic symbol table from '${ARTIFACT}': ${objdump_error}")
endif()

string(REPLACE "\n" ";" dynamic_symbol_lines "${dynamic_symbol_table}")
set(incompatible_symbols)

foreach (symbol_line IN LISTS dynamic_symbol_lines)
    # Only undefined symbols are runtime requirements on the target system.
    if (symbol_line MATCHES "\\*UND\\*")
        string(REGEX MATCH "GLIBC_[0-9]+(\\.[0-9]+)+" symbol_version "${symbol_line}")
        if (NOT "${symbol_version}" STREQUAL "")
            string(REGEX REPLACE "^GLIBC_" "" numeric_version "${symbol_version}")
            if ("${numeric_version}" VERSION_GREATER "${BASELINE}")
                string(REGEX MATCH "[^ \t]+$" symbol_name "${symbol_line}")
                list(APPEND incompatible_symbols "${symbol_version} ${symbol_name}")
            endif()
        endif()
    endif()
endforeach()

if (NOT "${incompatible_symbols}" STREQUAL "")
    list(REMOVE_DUPLICATES incompatible_symbols)
    list(SORT incompatible_symbols)
    string(JOIN "\n  " formatted_symbols ${incompatible_symbols})
    message(FATAL_ERROR
        "${ARTIFACT} requires glibc symbols newer than GLIBC_${BASELINE}:\n"
        "  ${formatted_symbols}\n"
        "The production BE must remain runnable on CentOS 7 (glibc ${BASELINE}).")
endif()

message(STATUS
    "glibc compatibility check passed: ${ARTIFACT} requires no symbols newer than "
    "GLIBC_${BASELINE}")
