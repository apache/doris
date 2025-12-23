#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GITHUB_USER=$1
URL=https://github.com/$GITHUB_USER/orc.git
BRANCH=$2

CLONE="git clone $URL -b $BRANCH"
MAKEDIR="mkdir orc/build && cd orc/build"
VOLUME="--volume m2cache:/root/.m2/repository"
mkdir -p logs

function failure {
    echo "Failed tests"
    grep -h "FAILED " logs/*-test.log
    exit 1
}
rm -f logs/pids.txt logs/*.log

start=`date`

for build in `cat os-list.txt`; do
    ./run-one.sh $1 $2 $build > logs/$build-test.log 2>&1 &
    echo "$!" >> logs/pids.txt
    echo "Launching $build as $!"
done

for job in `cat logs/pids.txt`; do
    echo "Waiting for $job"
    wait $job || failure
done

echo ""
echo "Test start: $start"
echo "End:" `date`
