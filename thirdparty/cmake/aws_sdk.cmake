# AWS SDK for C++
# Build S3/STS/transfer components from source
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_ONLY "s3;s3-crt;transfer;sts;identity-management" CACHE STRING "" FORCE)
set(CUSTOM_MEMORY_MANAGEMENT OFF CACHE BOOL "" FORCE)
set(AUTORUN_UNIT_TESTS OFF CACHE BOOL "" FORCE)
set(CPP_STANDARD 20 CACHE STRING "" FORCE)
set(ENABLE_ZLIB_REQUEST_COMPRESSION ON CACHE BOOL "" FORCE)
set(OPENSSL_ROOT_DIR ${CMAKE_CURRENT_BINARY_DIR}/openssl CACHE PATH "" FORCE)

# curl is built from source via add_subdirectory. Set CURL variables so AWS SDK's
# find_package(CURL) finds the source-built curl headers and library.
set(CURL_FOUND TRUE)
set(CURL_INCLUDE_DIR "${TP_SOURCE_DIR}/curl-8.2.1/include")
set(CURL_INCLUDE_DIRS "${TP_SOURCE_DIR}/curl-8.2.1/include")
set(CURL_LIBRARIES libcurl)
set(CURL_LIBRARY libcurl)

# AWS SDK bundles its own aws-c-* dependencies, build all from source
add_subdirectory(${TP_SOURCE_DIR}/aws-sdk-cpp-1.11.219 ${CMAKE_CURRENT_BINARY_DIR}/aws-sdk EXCLUDE_FROM_ALL)

# aws-c-* sub-libraries are built as part of the AWS SDK if not already available
# They are bundled inside the SDK source tree
foreach(_awslib aws-c-common aws-c-event-stream aws-checksums aws-c-io aws-c-http
                aws-c-cal aws-c-auth aws-c-compression aws-c-mqtt aws-c-s3 aws-c-sdkutils
                aws-crt-cpp)
    if(NOT TARGET ${_awslib})
        message(STATUS "[contrib] ${_awslib} not found as target, may be built by aws-sdk")
    endif()
endforeach()
