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

# proto_utils.cmake — common protobuf/grpc code generation functions
#
# Usage:
#   tp_protobuf_generate(SRCS HDRS
#       PROTO_PATH /path/to/proto/root
#       OUT_DIR    /path/to/output
#       PROTOS     foo.proto bar/baz.proto ...)
#
#   tp_grpc_generate(SRCS HDRS
#       PROTO_PATH /path/to/proto/root
#       OUT_DIR    /path/to/output
#       PROTOS     service.proto ...)
#
# Both functions:
#   - Use the in-tree protoc target ($<TARGET_FILE:protoc>)
#   - Include protobuf well-known types from protobuf-21.11/src
#   - Create output subdirectories automatically
#   - Return generated .cc files in SRCS and .h files in HDRS

set(_TP_PROTOBUF_INCLUDE "${TP_SOURCE_DIR}/protobuf-21.11/src")



# ============================================================================
# tp_protobuf_generate — compile .proto → .pb.cc + .pb.h
# ============================================================================
function(tp_protobuf_generate OUT_SRCS OUT_HDRS)
    cmake_parse_arguments(_PG "" "PROTO_PATH;OUT_DIR" "PROTOS" ${ARGN})

    set(_srcs)
    set(_hdrs)
    foreach(_proto_rel ${_PG_PROTOS})
        string(REGEX REPLACE "\\.proto$" "" _stem "${_proto_rel}")
        set(_pb_cc "${_PG_OUT_DIR}/${_stem}.pb.cc")
        set(_pb_h  "${_PG_OUT_DIR}/${_stem}.pb.h")
        set(_proto_abs "${_PG_PROTO_PATH}/${_proto_rel}")

        get_filename_component(_out_subdir "${_pb_cc}" DIRECTORY)
        file(MAKE_DIRECTORY "${_out_subdir}")

        add_custom_command(
            OUTPUT  "${_pb_cc}" "${_pb_h}"
            COMMAND $<TARGET_FILE:protoc>
                    -I${_PG_PROTO_PATH}
                    -I${_TP_PROTOBUF_INCLUDE}
                    --cpp_out=${_PG_OUT_DIR}
                    "${_proto_abs}"
            DEPENDS "${_proto_abs}" protoc
            COMMENT "[protoc] ${_proto_rel}"
            VERBATIM
        )
        list(APPEND _srcs "${_pb_cc}")
        list(APPEND _hdrs "${_pb_h}")
    endforeach()

    set(${OUT_SRCS} ${_srcs} PARENT_SCOPE)
    set(${OUT_HDRS} ${_hdrs} PARENT_SCOPE)
endfunction()

# ============================================================================
# tp_grpc_generate — compile .proto → .pb.cc + .pb.h + .grpc.pb.cc + .grpc.pb.h
# ============================================================================
function(tp_grpc_generate OUT_SRCS OUT_HDRS)
    cmake_parse_arguments(_GG "" "PROTO_PATH;OUT_DIR" "PROTOS" ${ARGN})

    set(_srcs)
    set(_hdrs)
    foreach(_proto_rel ${_GG_PROTOS})
        string(REGEX REPLACE "\\.proto$" "" _stem "${_proto_rel}")
        set(_pb_cc      "${_GG_OUT_DIR}/${_stem}.pb.cc")
        set(_pb_h       "${_GG_OUT_DIR}/${_stem}.pb.h")
        set(_grpc_cc    "${_GG_OUT_DIR}/${_stem}.grpc.pb.cc")
        set(_grpc_h     "${_GG_OUT_DIR}/${_stem}.grpc.pb.h")
        set(_proto_abs  "${_GG_PROTO_PATH}/${_proto_rel}")

        get_filename_component(_out_subdir "${_pb_cc}" DIRECTORY)
        file(MAKE_DIRECTORY "${_out_subdir}")

        add_custom_command(
            OUTPUT  "${_pb_cc}" "${_pb_h}" "${_grpc_cc}" "${_grpc_h}"
            COMMAND $<TARGET_FILE:protoc>
                    -I${_GG_PROTO_PATH}
                    -I${_TP_PROTOBUF_INCLUDE}
                    --cpp_out=${_GG_OUT_DIR}
                    "${_proto_abs}"
            COMMAND $<TARGET_FILE:protoc>
                    -I${_GG_PROTO_PATH}
                    -I${_TP_PROTOBUF_INCLUDE}
                    --grpc_out=${_GG_OUT_DIR}
                    --plugin=protoc-gen-grpc=$<TARGET_FILE:grpc_cpp_plugin>
                    "${_proto_abs}"
            DEPENDS "${_proto_abs}" protoc grpc_cpp_plugin
            COMMENT "[protoc+grpc] ${_proto_rel}"
            VERBATIM
        )
        list(APPEND _srcs "${_pb_cc}" "${_grpc_cc}")
        list(APPEND _hdrs "${_pb_h}" "${_grpc_h}")
    endforeach()

    set(${OUT_SRCS} ${_srcs} PARENT_SCOPE)
    set(${OUT_HDRS} ${_hdrs} PARENT_SCOPE)
endfunction()
