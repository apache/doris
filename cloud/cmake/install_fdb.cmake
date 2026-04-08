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

# ============================================================================
# FDB Library Configuration
# ============================================================================
# Configuration format:
#   FDB_CONFIG_${version}_${arch}_${os}_FILE - Tarball filename
#   FDB_CONFIG_${version}_${arch}_${os}_MD5  - MD5 checksum
#   FDB_CONFIG_${version}_${arch}_${os}_URL  - Base download URL
#   FDB_CONFIG_${version}_API_VERSION        - FDB API version (e.g., 710)
#
# Supported combinations:
#   - version: 7_1_23, 7_1_57
#   - arch: AMD64, AARCH64
#   - os: UBUNTU, CENTOS (all uppercase)
# ============================================================================

# Version 7.1.23 Configuration
set(FDB_CONFIG_7_1_23_API_VERSION "710")

# Version 7.1.23 - AMD64 - UBUNTU
set(FDB_CONFIG_7_1_23_AMD64_UBUNTU_FILE "fdb_lib_7_1_23.tar.xz")
set(FDB_CONFIG_7_1_23_AMD64_UBUNTU_MD5 "a00fe45da95cfac4e0caffa274bb2b30")
set(FDB_CONFIG_7_1_23_AMD64_UBUNTU_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/ubuntu/")

# Version 7.1.23 - AMD64 - CENTOS
set(FDB_CONFIG_7_1_23_AMD64_CENTOS_FILE "fdb_lib_7_1_23.tar.xz")
set(FDB_CONFIG_7_1_23_AMD64_CENTOS_MD5 "f9839a564849c0232a351143b4340de0")
set(FDB_CONFIG_7_1_23_AMD64_CENTOS_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/centos/")

# Version 7.1.57 Configuration
set(FDB_CONFIG_7_1_57_API_VERSION "710")

# Version 7.1.57 - AARCH64 - CENTOS
set(FDB_CONFIG_7_1_57_AARCH64_CENTOS_FILE "fdb_lib_7_1_57.aarch64.tar.xz")
set(FDB_CONFIG_7_1_57_AARCH64_CENTOS_MD5 "2d01a431b7a7465077e4ae5520f89693")
set(FDB_CONFIG_7_1_57_AARCH64_CENTOS_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/aarch64/")

# Version 7.1.57 - AMD64 - UBUNTU
set(FDB_CONFIG_7_1_57_AMD64_UBUNTU_FILE "fdb_lib_7_1_57.tar.xz")
set(FDB_CONFIG_7_1_57_AMD64_UBUNTU_MD5 "5a4aec35de0e041b952a3e39078f327a")
set(FDB_CONFIG_7_1_57_AMD64_UBUNTU_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/amd64/")

# Version 7.1.57 - AMD64 - CENTOS
set(FDB_CONFIG_7_1_57_AMD64_CENTOS_FILE "fdb_lib_7_1_57.tar.xz")
set(FDB_CONFIG_7_1_57_AMD64_CENTOS_MD5 "5a4aec35de0e041b952a3e39078f327a")
set(FDB_CONFIG_7_1_57_AMD64_CENTOS_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/amd64/")

# Version 7.3.69 Configuration
set(FDB_CONFIG_7_3_69_API_VERSION "730")

# Version 7.3.69 - AARCH64 - CENTOS
set(FDB_CONFIG_7_3_69_AARCH64_CENTOS_FILE "fdb_lib_7_3_69.tar.xz")
set(FDB_CONFIG_7_3_69_AARCH64_CENTOS_MD5 "7c6c676b41c70ef31eca617de7879364")
set(FDB_CONFIG_7_3_69_AARCH64_CENTOS_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/aarch64/")

# Version 7.3.69 - AMD64 - UBUNTU
set(FDB_CONFIG_7_3_69_AMD64_UBUNTU_FILE "fdb_lib_7_3_69.tar.xz")
set(FDB_CONFIG_7_3_69_AMD64_UBUNTU_MD5 "5454db8a8aaa4bcc580c1a40d1171f61")
set(FDB_CONFIG_7_3_69_AMD64_UBUNTU_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/amd64/")

# Version 7.3.69 - AMD64 - CENTOS
set(FDB_CONFIG_7_3_69_AMD64_CENTOS_FILE "fdb_lib_7_3_69.tar.xz")
set(FDB_CONFIG_7_3_69_AMD64_CENTOS_MD5 "5454db8a8aaa4bcc580c1a40d1171f61")
set(FDB_CONFIG_7_3_69_AMD64_CENTOS_URL "https://doris-build.oss-cn-beijing.aliyuncs.com/thirdparty/fdb/amd64/")

# ============================================================================
# Detect OS (run once)
# ============================================================================

if (NOT DEFINED FDB_OS_DETECTED)
    file(GLOB RELEASE_FILE_LIST LIST_DIRECTORIES false "/etc/*release*")
    execute_process(COMMAND "cat" ${RELEASE_FILE_LIST}
                    RESULT_VARIABLE CAT_RET_CODE
                    OUTPUT_VARIABLE CAT_RET_CONTENT)
    string(TOUPPER "${CAT_RET_CONTENT}" CAT_RET_CONTENT)

    if ("${CAT_RET_CONTENT}" MATCHES "UBUNTU")
        set(FDB_OS_RELEASE "UBUNTU" CACHE INTERNAL "Detected OS for FDB")
    else()
        # If it is not ubuntu, it is regarded as centos by default
        set(FDB_OS_RELEASE "CENTOS" CACHE INTERNAL "Detected OS for FDB")
    endif()

    set(FDB_OS_DETECTED TRUE CACHE INTERNAL "OS detection flag")
    message(STATUS "Detected OS: ${FDB_OS_RELEASE}")
endif()

# ============================================================================
# Function: Download and Setup FDB Library
# ============================================================================
# Downloads, verifies, and extracts a specific version of FDB library
#
# Parameters:
#   version - FDB version in format like "7_1_23" or "7_1_57"
#
# Exports:
#   FDB_INSTALL_DIR_${version} - Installation directory for this version
#   FDB_LIB_SO_${version}      - Path to libfdb_c.so for this version
# ============================================================================

function(download_and_setup_fdb version)
    # Determine architecture
    if (ARCH_AARCH64)
        set(fdb_arch "AARCH64")
    else()
        set(fdb_arch "AMD64")
    endif()

    # Construct configuration variable names
    set(config_prefix "FDB_CONFIG_${version}_${fdb_arch}_${FDB_OS_RELEASE}")
    set(fdb_file_var "${config_prefix}_FILE")
    set(fdb_md5_var "${config_prefix}_MD5")
    set(fdb_url_var "${config_prefix}_URL")
    set(fdb_api_version_var "FDB_CONFIG_${version}_API_VERSION")

    # Check if all required configurations exist
    set(missing_configs "")
    if (NOT DEFINED ${fdb_file_var})
        list(APPEND missing_configs ${fdb_file_var})
    endif()
    if (NOT DEFINED ${fdb_md5_var})
        list(APPEND missing_configs ${fdb_md5_var})
    endif()
    if (NOT DEFINED ${fdb_url_var})
        list(APPEND missing_configs ${fdb_url_var})
    endif()
    if (NOT DEFINED ${fdb_api_version_var})
        list(APPEND missing_configs ${fdb_api_version_var})
    endif()

    if (missing_configs)
        message(FATAL_ERROR "FDB version ${version} is missing required configurations for "
            "architecture ${fdb_arch} and OS ${FDB_OS_RELEASE}:\n"
            "  Missing variables: ${missing_configs}\n"
            "  Please add the following configurations:\n"
            "    ${fdb_file_var}\n"
            "    ${fdb_md5_var}\n"
            "    ${fdb_url_var}\n"
            "    ${fdb_api_version_var}")
    endif()

    # Get configuration values
    set(fdb_lib ${${fdb_file_var}})
    set(fdb_md5 ${${fdb_md5_var}})
    set(fdb_url ${${fdb_url_var}})

    string(APPEND fdb_url "${fdb_lib}")

    # Set version-specific installation directory
    string(REPLACE "_" "." version_dotted ${version})
    set(install_dir "${THIRDPARTY_DIR}/lib/fdb/${version_dotted}")
    set(lib_so "${install_dir}/lib64/libfdb_c.so")

    # Download FDB library if not exists
    if (NOT EXISTS "${THIRDPARTY_SRC}/${fdb_lib}")
        message(STATUS "Downloading FDB library from ${fdb_url}")
        file(MAKE_DIRECTORY ${THIRDPARTY_SRC})
        execute_process(COMMAND curl --retry 10 --retry-delay 2 --retry-max-time 30 ${fdb_url}
                                -o ${THIRDPARTY_SRC}/${fdb_lib} -k
                        RESULTS_VARIABLE download_ret)
        if (NOT ${download_ret} STREQUAL "0")
            execute_process(COMMAND "rm" "-rf" "${THIRDPARTY_SRC}/${fdb_lib}")
            message(FATAL_ERROR "Failed to download dependency of fdb ${fdb_url}, remove it")
        endif ()
    endif ()

    if (NOT EXISTS ${install_dir}/include/foundationdb)
        execute_process(COMMAND "md5sum" "${THIRDPARTY_SRC}/${fdb_lib}"
                        RESULT_VARIABLE md5sum_ret_code
                        OUTPUT_VARIABLE md5sum_content)
        if (NOT "${md5sum_content}" MATCHES "${fdb_md5}")
            execute_process(COMMAND "rm" "-rf" "${THIRDPARTY_SRC}/${fdb_lib}")
            message(FATAL_ERROR "${THIRDPARTY_SRC}/${fdb_lib} md5sum check failed, remove it")
        endif ()

        make_directory(${install_dir})
        execute_process(COMMAND tar xf ${THIRDPARTY_SRC}/${fdb_lib} -C ${install_dir})
    endif ()

    # Export version-specific API version to parent scope
    set(api_version_var "FDB_CONFIG_${version}_API_VERSION")
    if (DEFINED ${api_version_var})
        set(FDB_API_VERSION_${version} "${${api_version_var}}" PARENT_SCOPE)
    endif()

    # Export to parent scope
    set(FDB_INSTALL_DIR_${version} "${install_dir}" PARENT_SCOPE)
    set(FDB_LIB_SO_${version} "${lib_so}" PARENT_SCOPE)

    message(STATUS "FDB ${version_dotted} installation directory: ${install_dir}")
endfunction()

# ============================================================================
# Function: Setup Default FDB Library
# ============================================================================
# Sets up the default FDB library for static linking
#
# Parameters:
#   version - FDB version to use as default (e.g., "7_1_23")
#
# Exports:
#   FDB_INSTALL_DIR - Installation directory for default version
#   FDB_LIB_SO      - Path to libfdb_c.so for default version
# ============================================================================

function(setup_default_fdb version)
    string(REPLACE "_" "." version_dotted ${version})

    # Ensure the version has been downloaded and set up
    if (NOT DEFINED FDB_INSTALL_DIR_${version})
        message(FATAL_ERROR "FDB version ${version_dotted} has not been downloaded. Call download_and_setup_fdb first.")
    endif()

    set(FDB_INSTALL_DIR "${FDB_INSTALL_DIR_${version}}")
    set(FDB_LIB_SO "${FDB_LIB_SO_${version}}")

    # Set default FDB paths to parent scope
    set(FDB_INSTALL_DIR "${FDB_INSTALL_DIR_${version}}" PARENT_SCOPE)
    set(FDB_LIB_SO "${FDB_LIB_SO_${version}}" PARENT_SCOPE)

    # Install the default FDB library
    execute_process(COMMAND "rm" "-rf" "${THIRDPARTY_DIR}/include/foundationdb")
    execute_process(COMMAND "rm" "-rf" "${THIRDPARTY_DIR}/lib64/libfdb_c.so")
    execute_process(COMMAND "cp" "-r" "${FDB_INSTALL_DIR}/include/foundationdb" "${THIRDPARTY_DIR}/include/foundationdb")
    execute_process(COMMAND "cp" "${FDB_INSTALL_DIR}/lib64/libfdb_c.so" "${THIRDPARTY_DIR}/lib64/libfdb_c.so")

    # Set FDB API version for the default version
    set(api_version_var "FDB_CONFIG_${version}_API_VERSION")
    if (DEFINED ${api_version_var})
        set(fdb_api_version "${${api_version_var}}")
        add_definitions(-DFDB_API_VERSION=${fdb_api_version})
        message(STATUS "Default FDB API version: ${fdb_api_version}")
    else()
        message(WARNING "FDB API version not defined for version ${version_dotted}")
    endif()
endfunction()

function(install_fdb_library target_dir)
    install(FILES
        ${THIRDPARTY_DIR}/lib/libfdb_c.so
        PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE
        GROUP_READ GROUP_WRITE GROUP_EXECUTE
        WORLD_READ WORLD_EXECUTE
        DESTINATION ${OUTPUT_DIR}/lib)
endfunction()

# ============================================================================
# Main Execution
# ============================================================================

# Download and setup all specified FDB versions
foreach (version IN LISTS FDB_VERSIONS)
    string(REPLACE "." "_" version_underscored ${version})
    download_and_setup_fdb(${version_underscored})
endforeach()

# Select the default FDB version
string(REPLACE "." "_" FDB_DEFAULT_VERSION_UNDERSCORED ${FDB_DEFAULT_VERSION})
setup_default_fdb(${FDB_DEFAULT_VERSION_UNDERSCORED})

# Note: To add more FDB versions, call download_and_setup_fdb with other versions:
# Example:
#   download_and_setup_fdb("7_1_57")
#   # Access via: ${FDB_INSTALL_DIR_7_1_57} and ${FDB_LIB_SO_7_1_57}
