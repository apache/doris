#!/bin/bash

echo "Begin to build for selectdb cloud with $*"

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}"

# Check args
usage() {
    echo "
Usage: $0 --vendor <vendor>
  Optional options:
    --help
    --vendor

  Eg.
    $0 --vendor selectdb      build with vendor selectdb
    $0 --vendor velodb        build with vendor velodb
    $0 --help                 print this help msg
  "
}

VENDOR=
HELP=0
args_remain=
while [[ $# -gt 1 ]]; do
    case "$1" in
    --vendor)
        VENDOR="$2"
        shift 2
        ;;
    --help)
        HELP=1
        shift
        ;;
    --)
        shift
        args_remain="${args_remain} $@"
        break
        ;;
    *)
        args_remain="${args_remain} $1"
        shift
        ;;
    esac
done

if [[ ${HELP} -eq 1 ]]; then
    usage
    exit 0
fi

# if [[ "${VENDOR}" == "" ]]; then
#     echo "--vendor must be specified, chose one of selectdb, velodb"
#     usage
#     exit -1
# fi

export BUILD_JDBC_DRIVER=ON
export BUILD_TRINO_CONNECTOR=ON

export DISABLE_BUILD_UI=ON
export ENABLE_HDFS_STORAGE_VAULT=OFF

# these env vars are used by gensrc/script/gen_build_version.sh
export DORIS_BUILD_VERSION_PREFIX=${VENDOR:-selectdb}
export DORIS_BUILD_VERSION_MAJOR=4
export DORIS_BUILD_VERSION_MINOR=1
export DORIS_BUILD_VERSION_PATCH=0
export DORIS_BUILD_VERSION_HOTFIX=0
export DORIS_BUILD_VERSION_RC_VERSION=""

echo "build.sh with args_remain: ${args_remain}"
sh build.sh ${args_remain}

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

function build_trino_connectors() {
    echo "Build Trino connectors"
    # download trino connectors
    TRINO_CONNECTOR_URL=${TRINO_CONNECTOR_URL:-"https://selectdb-doris-1308700295.cos.ap-beijing.myqcloud.com/release/trino_connectors"}
    TRINO_CONNECTOR_DIR="${DORIS_THIRDPARTY}/src/trino_connectors/"
    mkdir -p "${TRINO_CONNECTOR_DIR}"

    # jar names and md5sum
    local connector_array=( \
    "trino-kudu-435.tar.gz        2d58bfcac5b84218c5d1055af189e30c" \
    "trino-tpcds-435.tar.gz       d1216e7060dad621ceeb3480732182ef" \
    "trino-tpch-435.tar.gz        dffb4aef95c9ebe34a92326528ff14a8" \
    )

    local i=0
    for ((i=0; i<${#connector_array[@]}; i++)); do
        local connector
        local connector_md5
        connector=$(echo "${connector_array[i]}" | awk '{print $1}')
        connector_md5=$(echo "${connector_array[${i}]}" | awk '{print $2}')

        # echo "$i ==== $connector ==== $connector_md5"

        # connector path in local file system
        local connector_path=${DORIS_THIRDPARTY}/src/trino_connectors/${connector}
        local connector_url=${TRINO_CONNECTOR_URL}/${connector}
        echo "index: ${i}, connector: ${connector} md5: ${connector_md5}"

        local status=1
        for attemp in 1 2; do
            if [[ -r "${connector_path}" ]]; then
                if md5sum_func "${connector_path}" "${connector_md5}"; then
                    echo "Archive ${connector} already exist."
                    status=0
                    break
                fi
                echo "Archive ${connector} will be removed and download again."
                rm -f "${connector_path}"
            else
                echo "Downloading ${connector} from ${connector_url} to ${connector_path}"
                if wget --no-check-certificate -q "${connector_url}" -O "${connector_path}"; then
                     if md5sum_func "${connector_path}" "${connector_md5}"; then
                        status=0
                        echo "Success to download ${connector}"
                        break
                    fi
                    echo "Archive ${connector} will be removed and download again."
                    rm -f "${connector_path}"
                else
                    echo "Failed to download ${connector}. attemp: ${attemp}"
                fi
            fi
        done

        if [[ "${status}" -ne 0 ]]; then
            echo "Failed to download ${connector}"
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

# build and copy trino connectors
BUILD_TRINO_CONNECTOR=${BUILD_TRINO_CONNECTOR:-"ON"}
if [[ "${BUILD_TRINO_CONNECTOR}" = "ON" ]]; then
    build_trino_connectors
fi
export DORIS_OUTPUT=output
rm -rf "${DORIS_OUTPUT}/be/connectors"
mkdir "${DORIS_OUTPUT}/be/connectors"
if [[ "${BUILD_TRINO_CONNECTOR}" = "ON" ]]; then
    tar xzf "${DORIS_THIRDPARTY}/src/trino_connectors/trino-kudu-435.tar.gz" -C "${DORIS_OUTPUT}/be/connectors"/
    tar xzf "${DORIS_THIRDPARTY}/src/trino_connectors/trino-tpch-435.tar.gz" -C "${DORIS_OUTPUT}/be/connectors"/
    tar xzf "${DORIS_THIRDPARTY}/src/trino_connectors/trino-tpcds-435.tar.gz" -C "${DORIS_OUTPUT}/be/connectors"/
fi
rm -rf "${DORIS_OUTPUT}/fe/connectors"
mkdir "${DORIS_OUTPUT}/fe/connectors"
if [[ "${BUILD_TRINO_CONNECTOR}" = "ON" ]]; then
    tar xzf "${DORIS_THIRDPARTY}/src/trino_connectors/trino-kudu-435.tar.gz" -C "${DORIS_OUTPUT}/fe/connectors"/
    tar xzf "${DORIS_THIRDPARTY}/src/trino_connectors/trino-tpch-435.tar.gz" -C "${DORIS_OUTPUT}/fe/connectors"/
    tar xzf "${DORIS_THIRDPARTY}/src/trino_connectors/trino-tpcds-435.tar.gz" -C "${DORIS_OUTPUT}/fe/connectors"/
fi

# copy FE audit plugin
echo "Copy audit log plugin if necessary"
mkdir -p output/fe/plugins
audit_plugin_path=output/fe/plugins/auditloader.zip
if [[ -f output/audit_loader/auditloader.zip ]]; then
    cp output/audit_loader/auditloader.zip ${audit_plugin_path}
else
    echo "force download audit plugin for compatibility reason: cloud-4.0.x needs this plugin if it is upgrading from cloud-3.0.x"
    wget --no-check-certificate -q https://apache-doris-releases.oss-cn-beijing.aliyuncs.com/doris-plugins/doris-2.0.13-auditloader.zip -O ${audit_plugin_path}
    if md5sum_func "${audit_plugin_path}" "3abdddfabcf6aeae04a69114864f6cbd"; then
        echo "Success to download audit plugin"
    else
        echo "failed to download audit plugin, md5 not match"
    fi
fi

# process ms conf, script and binary for compatibilities
pushd $DORIS_OUTPUT/ms/lib
ln -srf doris_cloud selectdb_cloud
popd
cp $DORIS_HOME/cloud/script/custom_start.sh $DORIS_OUTPUT/ms/bin/
