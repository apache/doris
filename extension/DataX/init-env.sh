#!/usr/bin/env bash
# Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
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
    cp $DATAX_POM ${DATAX_POM}.orig
    sed -i "s/<\/modules>/    <module>doriswriter<\/module>\n    <\/modules>/g"  $DATAX_POM 
else
    echo "doriswriter module exists in $DATAX_POM"  
fi

echo "Finish DataX environment initialization"
