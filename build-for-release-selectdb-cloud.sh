#!/bin/bash

echo "Begin to build for selectdb cloud with $*"

export BUILD_JDBC_DRIVER=ON

sh build.sh "$@"

ret=$?
if [[ ${ret} -ne 0 ]]; then
    echo "Failed to build selectdb cloud, build.sh $* ret: ${ret}"
    exit 1
fi

set -e
echo "Succeed to build for selectdb cloud with $*"

echo "Do some extra packaging work"

DORIS_THIRDPARTY=thirdparty

function md5sum_func() {
    local FILENAME="$1"
    local MD5SUM="$2"
    local md5

    md5="$(md5sum "${FILENAME}")"
    if [[ "${md5}" != "${MD5SUM}  ${FILENAME}" ]]; then
        echo "${FILENAME} md5sum check failed!"
        echo -e "except-md5 ${MD5SUM} \nactual-md5 ${md5}"
        return 1
    fi
    return 0
}

function build_jdbc_driver() {
    echo "Build jdbc driver"
    # download jdbc rivers
    JDBC_DRIVER_URL=${JDBC_DRIVER_URL:-"https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/jdbc_driver"}
    JDBC_DRIVERS_DIR="${DORIS_THIRDPARTY}/src/jdbc_drivers/"
    mkdir -p "${JDBC_DRIVERS_DIR}"

    # jar names and md5sum
    local driver_array=( \
    "clickhouse-jdbc-0.3.2-patch11-all.jar 9be22a93267dc4b066e0a3aefc2dd024" \
    "clickhouse-jdbc-0.4.2-all.jar         45d7c4b5d17a8d7ac81b9e6200b78adb" \
    "mssql-jdbc-11.2.0.jre8.jar            b204274eb02a848ac405961e6f43e7bd" \
    "mssql-jdbc-11.2.3.jre8.jar            5d53b0cb64ec2e5268a5f7a349889d35" \
    "mysql-connector-java-8.0.25.jar       fdf55dcef04b09f2eaf42b75e61ccc9a" \
    "ojdbc6.jar                            621a393d7be9ff0f2fec6fbba2c8f9b6" \
    "ojdbc8.jar                            21dd8c3d05648be958184f5e9288f69e" \
    "postgresql-42.5.0.jar                 20c8228267b6c9ce620fddb39467d3eb" \
    "postgresql-42.5.1.jar                 378f8a2ddab2564a281e5f852800e2e9" \
    )

    local i=0
    for ((i=0; i<${#driver_array[@]}; i++)); do
        local driver
        local driver_md5
        driver=$(echo "${driver_array[i]}" | awk '{print $1}')
        driver_md5=$(echo "${driver_array[${i}]}" | awk '{print $2}')

        # echo "$i ==== $driver ==== $driver_md5"

        # driver path in local file system
        local driver_path=${DORIS_THIRDPARTY}/src/jdbc_drivers/${driver}
        local driver_url=${JDBC_DRIVER_URL}/${driver}
        echo "index: ${i}, dirver: ${driver} md5: ${driver_md5}"

        local status=1
        for attemp in 1 2; do
            if [[ -r "${driver_path}" ]]; then
                if md5sum_func "${driver_path}" "${driver_md5}"; then
                    echo "Archive ${driver} already exist."
                    status=0
                    break
                fi
                echo "Archive ${driver} will be removed and download again."
                rm -f "${driver_path}"
            else
                echo "Downloading ${driver} from ${driver_url} to ${driver_path}"
                if wget --no-check-certificate -q "${driver_url}" -O "${driver_path}"; then
                     if md5sum_func "${driver_path}" "${driver_md5}"; then
                        status=0
                        echo "Success to download ${driver}"
                        break
                    fi
                    echo "Archive ${driver} will be removed and download again."
                    rm -f "${driver_path}"
                else
                    echo "Failed to download ${driver}. attemp: ${attemp}"
                fi
            fi
        done

        if [[ "${status}" -ne 0 ]]; then
            echo "Failed to download ${driver}"
            exit 1
        fi
    done
}


# build and copy jdbc drivers
BUILD_JDBC_DRIVER=${BUILD_JDBC_DRIVER:-"ON"}
if [[ "${BUILD_JDBC_DRIVER}" = "ON" ]]; then
    build_jdbc_driver
fi
export DORIS_OUTPUT=output
rm -rf "${DORIS_OUTPUT}/be/jdbc_drivers"
if [[ "${BUILD_JDBC_DRIVER}" = "ON" ]]; then
    cp -rf "${DORIS_THIRDPARTY}/src/jdbc_drivers" "${DORIS_OUTPUT}/be"/
fi
rm -rf "${DORIS_OUTPUT}/fe/jdbc_drivers"
if [[ "${BUILD_JDBC_DRIVER}" = "ON" ]]; then
    cp -rf "${DORIS_THIRDPARTY}/src/jdbc_drivers" "${DORIS_OUTPUT}/fe"/
fi

# copy FE audit plugin
echo "Copy audit log plugin if necessary"
mkdir -p output/fe/plugins
if [[ -f output/audit_loader/auditloader.zip ]]; then
  cp output/audit_loader/auditloader.zip output/fe/plugins/
fi
