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
# The shell is used to make data type conversion(pgsql -> doris)
####################################################################
#!/bin/bash
path=$1
sed -i 's/integer/int/g' $path
sed -i 's/character varying([^)]*)/varchar(65533)/g' $path
sed -i 's/character varying/varchar(65533)/g' $path
sed -i 's/numeric/decimal/g' $path
sed -i 's/timestamp([^)]*)/varchar(255)/g' $path
sed -i 's/timestamp/varchar(255)/g' $path
sed -i 's/text/string/g' $path
sed -i 's/without time zone//g' $path
#sed -i 's/DEFAULT.*/,/g' $path
