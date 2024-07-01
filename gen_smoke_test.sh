#!/usr/bin/env bash
set -eo pipefail

#ATTN: make smoke test success need right mvn, java env
# just build java lib, ignore run case
echo "You need correct configuration JAVA_HOME(export). Under normal conditions, You will see 'done' when finish it"
echo "build smoke test ..."
./run-regression-test.sh --clean --run -h 2>&1 > /dev/null

echo "mkdir smoke_test dir ..."
# generate smoke-test framework
if [ -d ./smoke_test ];then
    rm -fr ./smoke_test
fi

mkdir ./smoke_test
cd ./smoke_test

echo "genrate script ..."
# generate start.sh
cat>start.sh<<EOF
#!/bin/sh

echo "You need put smoke-test/tpch/sf0.1/customer.tbl.gz on S3 first"

PWD="\$(pwd)"

CONF_FILE=./smoke.conf
CLOUD_VERSION=""

total=\$#
if [ \$total -eq 1 ];then
    CONF_FILE=\$1
fi

if [ \$total -eq 2 ];then
    CONF_FILE=\$1
    CLOUD_VERSION=\$2
fi

JAVA_HOME=\`sed -n '/^JAVA_HOME/p' \$CONF_FILE\`
ISVALID_JAVA_HOME=\`sed -n '/^JAVA_HOME/p' \$CONF_FILE|cut -c11-\`"x"
if [ \$ISVALID_JAVA_HOME == "x" ];then
    echo "please set a valid JAVA_HOME"
    exit -1
fi

# delete custom conf
rm -fr ./bin/regression-test/conf/regression-conf-custom.groovy
# change ./bin/regression-test/conf/regression-conf-custom.groovy by \$CONF_FILE


host=\`sed -n '/^host/p' \$CONF_FILE | cut  -c6-\`
queryPort=\`sed -n '/^queryPort/p' \$CONF_FILE | cut  -c11-\`
httpPort=\`sed -n '/^httpPort/p' \$CONF_FILE | cut -c10-\`

jdbcUrl="jdbc:mysql://\$host:\$queryPort/?"

jdbcUser=\`sed -n '/^user/p' \$CONF_FILE | cut -c6-\`
jdbcPassword=\`sed -n '/^password/p' \$CONF_FILE | cut -c10-\`

feCloudHttpAddress=\$host:\$httpPort
feHttpAddress=\$host:\$httpPort

feCloudHttpUser=\$jdbcUser
feCloudHttpPassword=\$jdbcPassword

new_line="jdbcUrl=\"\$jdbcUrl\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="jdbcUser=\"\$jdbcUser\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy


new_line="jdbcPassword=\"\$jdbcPassword\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

PWD="\$(pwd)"
new_line="suitePath = \"\$PWD/bin/regression-test/suites\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy


new_line="dataPath = \"\$PWD/bin/regression-test/data\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="defaultDb = \"smoke_test\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

echo "// add cloud mode conf" >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="feCloudHttpAddress=\"\$feCloudHttpAddress\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="feCloudHttpUser=\"\$feCloudHttpUser\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="feCloudHttpPassword=\"\$feCloudHttpPassword\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy
echo "isSmokeTest = true" >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^smokeEnv/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

echo "// for external stage" >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^ak/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^sk/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^s3Endpoint/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^s3Region/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^s3BucketName/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^s3Prefix/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="\$(sed -n '/^s3Provider/p' \$CONF_FILE)"
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="cloudVersion=\"\$CLOUD_VERSION\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="feHttpAddress=\"\$feHttpAddress\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="feHttpUser=\"\$feCloudHttpUser\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="feHttpPassword=\"\$feCloudHttpPassword\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="excludeSuites=\"test_stage_ram\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

new_line="pluginPath=\"bin/regression-test/plugins\""
echo \$new_line >> ./bin/regression-test/conf/regression-conf-custom.groovy

# start smoke test

export \$JAVA_HOME
sh ./bin/run-regression-test.sh --run -d cloud/smoke
EOF
chmod +x start.sh

# generate smoke.conf
cat>smoke.conf<<EOF
JAVA_HOME=/Users/dengxin/Library/Java/JavaVirtualMachines/semeru-16.0.2/Contents/Home
#smoke env 
smokeEnv="dx-smoke-test"

#ATTN: don't use "", don't add more blank space, The following fields use 'cut' split
host=82.157.8.202
queryPort=8877
httpPort=8876
user=admin
password=

# for external stage
# You need put some data set on S3 first
# smoke-test/tpch/sf0.1/customer.tbl.gz
# smoke-test/tpch/sf0.1/lineitem.tbl.gz
# smoke-test/tpch/sf0.1/nation.tbl.gz
# smoke-test/tpch/sf0.1/orders.tbl.gz
# smoke-test/tpch/sf0.1/part.tbl.gz
# smoke-test/tpch/sf0.1/partsupp.tbl.gz
# smoke-test/tpch/sf0.1/region.tbl.gz
# smoke-test/tpch/sf0.1/supplier.tbl.gz
ak="use your ak"
sk="use your sk"
s3Endpoint="cos.ap-beijing.myqcloud.com"
s3Region="ap-beijing"
s3BucketName="justtmp-bj-1308700295"
EOF

echo "copy files ..."
# copy regression framework to bin
mkdir -p ./bin
mkdir -p ./bin/regression-test
cp ../run-regression-test.sh ./bin/
cp -r ../regression-test ./bin/
mkdir ./bin/output
cp -r ../output/regression-test ./bin/output/

echo "remove mvn require ..."

SYSTEM=$(uname -s)
if [ "$SYSTEM" = "Darwin" ]; then
    SEDOPT=".bak"
fi


begin=`sed -n '/# set maven/=' ./bin/run-regression-test.sh`
end=`sed -n '/export MVN_CMD/=' ./bin/run-regression-test.sh`
sed -i $SEDOPT "$begin,$end"'d' ./bin/run-regression-test.sh

cd ..
echo "done"
