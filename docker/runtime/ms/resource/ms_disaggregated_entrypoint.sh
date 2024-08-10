#!/bin/bash
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

#

#get from env
FDB_ENDPOINT=${FDB_ENDPOINT}
CONFIGMAP_PATH=${CONFIGMAP_PATH:="/etc/doris"}
DORIS_HOME=${DORIS_HOME:="/opt/apache-doris"}

echo "fdb_cluster=$FDB_ENDPOINT" >> $DORIS_HOME/ms/conf/doris_cloud.conf
if [[ -d $CONFIGMAP_PATH ]]; then
    for file in `ls $CONFIGMAP_PATH`
        do
            if [[ "$file" == "doris_cloud.conf" ]] ; then
                mv -f $DORIS_HOME/ms/conf/$file $DORIS_HOME/ms/conf/$file.bak
                cp $CONFIGMAP_PATH/$file $DORIS_HOME/ms/conf/$file
                echo "fdb_cluster=$FDB_ENDPOINT" >> $DORIS_HOME/ms/conf/doris_cloud.conf
                continue
            fi

            if test -e $DORIS_HOME/ms/conf/$file ; then
                mv -f $DORIS_HOME/ms/conf/$file $DORIS_HOME/ms/conf/$file.bak
            fi
            ln -sfT $CONFIGMAP_PATH/$file $DORIS_HOME/ms/conf/$file
       done
fi

$DORIS_HOME/ms/bin/start.sh --$1
