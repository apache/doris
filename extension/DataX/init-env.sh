#/bin/bash
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

####################################################################
# This script is used to initial environment of DataX
####################################################################

set -eo pipefail

DATAX_EXTENSION_HOME=`dirname "$0"`
DATAX_EXTENSION_HOME=`cd "$DATAX_EXTENSION_HOME"; pwd`

export DATAX_EXTENSION_HOME

DATAX_GITHUB=https://github.com/alibaba/DataX.git

DORISWRITER_DIR=$DATAX_EXTENSION_HOME/doriswriter
DATAX_GIT_DIR=$DATAX_EXTENSION_HOME/DataX/
DATAX_POM=$DATAX_EXTENSION_HOME/DataX/pom.xml
DATAX_PACKAGE=$DATAX_EXTENSION_HOME/DataX/package.xml
DATAX_CORE_POM=$DATAX_EXTENSION_HOME/DataX/core/pom.xml


USE_MACOS=0
# check OS type
if [[ ! -z "$OSTYPE" ]]; then
    if [[ ${OSTYPE:0:6} == "darwin" ]]; then
        USE_MACOS=1
    fi
fi

if [ ! -d $DATAX_GIT_DIR ]; then
    echo "Clone DataX from $DATAX_GITHUB"
    git clone $DATAX_GITHUB $DATAX_GIT_DIR
    ln -s $DORISWRITER_DIR $DATAX_GIT_DIR/doriswriter
else
    echo "DataX code repo exists in $DATAX_GIT_DIR"
fi

if [ ! -f "$DATAX_POM" ]; then
    echo "$DATAX_POM does not exist, exit"
    exit 1
fi

if [ `grep -c "doriswriter" $DATAX_POM` -eq 0 ]; then
    echo "No doriswriter module in $DATAX_POM, add it"
    if [[ USE_MACOS -eq 0 ]];then
        cp $DATAX_POM ${DATAX_POM}.orig
        sed -i "s/<\/modules>/    <module>doriswriter<\/module>\n    <\/modules>/g"  $DATAX_POM
    else
        sed -i '.orig' "s/<\/modules>/    <module>doriswriter<\/module>\n    <\/modules>/g"  $DATAX_POM
    fi
else
    echo "doriswriter module exists in $DATAX_POM"  
fi

if [ `grep -c "doriswriter" $DATAX_PACKAGE` -eq 0 ]; then
    echo "No doriswriter module in $DATAX_PACKAGE, add it"
    if [[ USE_MACOS -eq 0 ]];then
        cp $DATAX_PACKAGE ${DATAX_PACKAGE}.orig
        sed -i "s/<\/fileSets>/    <fileSet>\n            <directory>doriswriter\/target\/datax\/<\/directory>\n            <includes>\n                <include>**\/*.*<\/include>\n            <\/includes>\n            <outputDirectory>datax<\/outputDirectory>\n        <\/fileSet>\n    <\/fileSets>/g"  $DATAX_PACKAGE
    else
        sed -i '.orig' "s/<\/fileSets>/    <fileSet>\n            <directory>doriswriter\/target\/datax\/<\/directory>\n            <includes>\n                <include>**\/*.*<\/include>\n            <\/includes>\n            <outputDirectory>datax<\/outputDirectory>\n        <\/fileSet>\n    <\/fileSets>/g"  $DATAX_PACKAGE
    fi
else
    echo "doriswriter module exists in $DATAX_PACKAGE"  
fi

if [ `grep -c "4.5.13" $DATAX_CORE_POM` -eq 0 ]; then
    echo "No httpclient 4.5.13 in $DATAX_CORE_POM, add it"
    if [[ USE_MACOS -eq 0 ]];then
        cp $DATAX_CORE_POM ${DATAX_CORE_POM}.orig
        sed -i ":a;N;s/<artifactId>httpclient<\/artifactId>\n            <version>4.5<\/version>/<artifactId>httpclient<\/artifactId>\n            <version>4.5.13<\/version>/g" $DATAX_CORE_POM
    else
        sed -i '.orig' -e "N" -e "s/<artifactId>httpclient<\/artifactId>\n            <version>4.5<\/version>/<artifactId>httpclient<\/artifactId>\n            <version>4.5.13<\/version>/g" $DATAX_CORE_POM
    fi
    
else
    echo "httpclient 4.5.13 exists in $DATAX_CORE_POM"  
fi

echo "Finish DataX environment initialization"
