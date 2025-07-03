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

DIR=$(
    cd $(dirname $0)
    pwd
)

source $DIR/common.sh

RUN_USER=root

create_host_user() {
    if [ -z ${HOST_USER} ]; then
        health_log "no specific run user, run as root"
        return
    fi
    id ${HOST_USER}
    if [ $? -eq 0 ]; then
        health_log "contain user ${HOST_USER}, no create new user"
        RUN_USER=${HOST_USER}
        return
    fi
    id ${HOST_UID}
    if [ $? -eq 0 ]; then
        health_log "contain uid ${HOST_UID}, no create new user"
        return
    fi
    addgroup --gid ${HOST_GID} ${HOST_USER}
    if [ $? -eq 0 ]; then
        health_log "create group ${HOST_USER} with gid ${HOST_GID} succ"
    else
        health_log "create group ${HOST_USER} with gid ${HOST_GID} failed"
        return
    fi
    adduser --disabled-password --shell /bin/bash --gecos "" --uid ${HOST_UID} --gid ${HOST_GID} ${HOST_USER}
    if [ $? -eq 0 ]; then
        health_log "create user ${HOST_USER} with uid ${HOST_UID} succ"
        RUN_USER=${HOST_USER}
    else
        health_log "create user ${HOST_USER} with uid ${HOST_UID} failed"
    fi
}

create_host_user

if command -v gosu 2>&1 >/dev/null; then
    if [ -f ${LOG_FILE} ]; then
        chown ${RUN_USER}:${RUN_USER} ${LOG_FILE}
    fi
    gosu ${RUN_USER} bash ${DIR}/${1} ${@:2}
else
    bash ${DIR}/${1} ${@:2}
fi
