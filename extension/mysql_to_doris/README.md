<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

How to do?

1、To configure mysql.conf and doris.conf in the conf directory,the conf including
host、port and password

2、To configure mysql_tables and doris_tables in the conf directory,the conf is user need to synchronization tables and want to get table name

3、To execute e_mysql_to_doris.sh by sh e_mysql_to_doris.sh

4、To execute e_auto.sh by nohup sh e_auto.sh &

What do you get?

A simple configuration synchronizes all configured tables and Monitor Mysql metadata changes in real time

