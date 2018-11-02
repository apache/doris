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
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/codegen")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/codegen")

# Generated C files for IR
set(IR_SSE_C_FILE ${GENSRC_DIR}/doris_ir/doris_sse_ir.cpp)
set(IR_NO_SSE_C_FILE ${GENSRC_DIR}/doris_ir/doris_no_sse_ir.cpp)

add_library(CodeGen STATIC
    codegen_anyval.cpp
    llvm_codegen.cpp
    subexpr_elimination.cpp
    ${IR_SSE_C_FILE}
    ${IR_NO_SSE_C_FILE}
)

add_dependencies(CodeGen gen_ir_descriptions compile_to_ir_sse compile_to_ir_no_sse)

# output cross compile to ir metadata
set(IR_DESC_GEN_OUTPUT
    ${GENSRC_DIR}/doris_ir/doris_ir_names.h
    ${GENSRC_DIR}/doris_ir/doris_ir_functions.h
)

add_custom_command(
    OUTPUT ${IR_DESC_GEN_OUTPUT}
    COMMAND $ENV{PYTHON} ${CMAKE_CURRENT_SOURCE_DIR}/gen_ir_descriptions.py
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/gen_ir_descriptions.py
    COMMENT "Generating ir cross compile metadata."
    VERBATIM
)
add_custom_target(gen_ir_descriptions ALL DEPENDS ${IR_DESC_GEN_OUTPUT})

set(IR_INPUT_FILES ${CMAKE_CURRENT_SOURCE_DIR}/doris_ir.cpp)
set(IR_SSE_TMP_OUTPUT_FILE "${GENSRC_DIR}/doris_ir/doris_sse_tmp.bc")
set(IR_NO_SSE_TMP_OUTPUT_FILE "${GENSRC_DIR}/doris_ir/doris_no_sse_tmp.bc")
set(IR_SSE_OUTPUT_FILE "${GENSRC_DIR}/doris_ir/doris_sse.bc")
set(IR_NO_SSE_OUTPUT_FILE "${GENSRC_DIR}/doris_ir/doris_no_sse.bc")
set(IR_SSE_TMP_C_FILE ${IR_SSE_C_FILE}.tmp)
set(IR_NO_SSE_TMP_C_FILE ${IR_NO_SSE_C_FILE}.tmp)

# Run the clang compiler to generate IR.  Then run their opt tool to remove
# unnamed instr.  This makes the IR verifiable and more readable.
# We need to compile to IR twice, once with sse enabled and one without.  At runtime
# impala will pick the correct file to load.
add_custom_command(
  OUTPUT ${IR_SSE_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} "-msse4.2" ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_SSE_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} --instnamer < ${IR_SSE_TMP_OUTPUT_FILE} > ${IR_SSE_OUTPUT_FILE}
  COMMAND rm ${IR_SSE_TMP_OUTPUT_FILE}
  DEPENDS Exprs Udf ${IR_INPUT_FILES}
)

# Compile without sse enabled.
add_custom_command(
  OUTPUT ${IR_NO_SSE_OUTPUT_FILE}
  COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${CLANG_INCLUDE_FLAGS} ${IR_INPUT_FILES} -o ${IR_NO_SSE_TMP_OUTPUT_FILE}
  COMMAND ${LLVM_OPT_EXECUTABLE} --instnamer < ${IR_NO_SSE_TMP_OUTPUT_FILE} > ${IR_NO_SSE_OUTPUT_FILE}
  COMMAND rm ${IR_NO_SSE_TMP_OUTPUT_FILE}
  DEPENDS Exprs Udf ${IR_INPUT_FILES}
)

add_custom_target(compile_to_ir_sse DEPENDS ${IR_SSE_OUTPUT_FILE})
add_custom_target(compile_to_ir_no_sse DEPENDS ${IR_NO_SSE_OUTPUT_FILE})

# Convert LLVM bytecode to C array.
add_custom_command(
  OUTPUT ${IR_SSE_C_FILE}
  COMMAND $ENV{DORIS_HOME}/gensrc/script/file2array.sh -n -v doris_sse_llvm_ir ${IR_SSE_OUTPUT_FILE} > ${IR_SSE_TMP_C_FILE}
  COMMAND mv ${IR_SSE_TMP_C_FILE} ${IR_SSE_C_FILE}
  DEPENDS $ENV{DORIS_HOME}/gensrc/script/file2array.sh
  DEPENDS ${IR_SSE_OUTPUT_FILE}
)

# Convert LLVM bytecode to C array.
add_custom_command(
  OUTPUT ${IR_NO_SSE_C_FILE}
  COMMAND $ENV{DORIS_HOME}/gensrc/script/file2array.sh -n -v doris_no_sse_llvm_ir ${IR_NO_SSE_OUTPUT_FILE} > ${IR_NO_SSE_TMP_C_FILE}
  COMMAND mv ${IR_NO_SSE_TMP_C_FILE} ${IR_NO_SSE_C_FILE}
  DEPENDS $ENV{DORIS_HOME}/gensrc/script/file2array.sh
  DEPENDS ${IR_NO_SSE_OUTPUT_FILE}
)

