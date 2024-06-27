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

set -exuo pipefail

TICKET_LIFETIME='30m'

kinit -l "$TICKET_LIFETIME" -f -c /etc/trino/conf/presto-server-krbcc \
      -kt /etc/trino/conf/presto-server.keytab presto-server/$(hostname -f)@LABS.TERADATA.COM

kinit -l "$TICKET_LIFETIME" -f -c /etc/trino/conf/hive-presto-master-krbcc \
      -kt /etc/trino/conf/hive-presto-master.keytab hive/$(hostname -f)@LABS.TERADATA.COM

kinit -l "$TICKET_LIFETIME" -f -c /etc/trino/conf/hdfs-krbcc \
      -kt /etc/hadoop/conf/hdfs.keytab hdfs/hadoop-master@LABS.TERADATA.COM

kinit -l "$TICKET_LIFETIME" -f -c /etc/trino/conf/hive-krbcc \
      -kt /etc/hive/conf/hive.keytab hive/hadoop-master@LABS.TERADATA.COM
