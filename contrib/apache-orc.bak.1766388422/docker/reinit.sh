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

start=`date`

TARGET=${@:-`cat os-list.txt`}
echo "Target:" $TARGET

for build in $TARGET; do
  OS=$(echo "$build" | cut -d '_' -f1)
  REST=$(echo "$build" | cut -d '_' -f2- -s)
  if [ -z "$REST" ]; then
    ARGS=""
  else
    ARGS=$(echo "$REST" | sed -e 's/^/--build-arg /' -e 's/_/ --build-arg /g')
  fi
  TAG=$(echo "apache/orc-dev:$build" | sed -e 's/=/-/g')
  echo "Re-initialize $TAG"
  ( cd $OS && docker build --no-cache -t "$TAG" $ARGS . )
done
echo "Start: $start"
echo "End:" `date`
