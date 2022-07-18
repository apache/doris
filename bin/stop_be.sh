#!/usr/bin/env bash
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

# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done

PRGDIR=`dirname "$PRG"`

export DORIS_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`
export PID_DIR=`cd "$PRGDIR" >/dev/null; pwd`

signum=9
if [ x"$1" = x"--grace" ]; then
    signum=15
fi

while read line; do
    envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*="`
    envline=`eval "echo $envline"`
    if [[ $envline == *"="* ]]; then
        eval 'export "$envline"'
    fi
done < $DORIS_HOME/conf/be.conf

pidfile=$PID_DIR/be.pid

if [ -f $pidfile ]; then
    pid=`cat $pidfile`
    pidcomm=`ps -p $pid -o comm=`
    if [ "doris_be"x != "$pidcomm"x ]; then
        echo "ERROR: pid process may not be be. "
        exit 1
    fi

    if kill -0 $pid; then
        if kill -${signum} $pid > /dev/null 2>&1; then
            echo "stop $pidcomm, and remove pid file. "
            rm $pidfile
            exit 0
        else
            exit 1
        fi
    else
        echo "Backend already exit, remove pid file. "
        rm $pidfile
    fi
else
    echo "$pidfile does not exist"
    exit 1
fi

