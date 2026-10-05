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

This tool is used to get the progress of all current table transitions 
during the online `segment_2` function.

Currently, you can specify 3 dimensions (in the conf file) to view the 
results, you can specify one of them individually, or you can customize 
the combination (that is, specify multiple at the same time).

# Note
We use MySQLdb python lib to fetch meta from FE, so you must install it.

You can get MySQLdb lib from https://pypi.python.org/pypi/MySQL-python, 
then you can install it as follows:
```
$ tar zxvf MySQL-python-*.tar.gz
$ cd MySQL-python-*
$ python setup.py build
$ python setup.py install
``` 

# Steps
1. Fill in the conf according to your cluster configuration, and specify 
   the table or be you want to watch.
2. Execute `python show_segment_status.py`

# Example
1. If you want to watch the process of a table named `xxxx`, you can specify 
   `table_name = xxxx` in conf file

2. If you want to watch the process on be whose be_id is `xxxx`, you can specify 
   `be_id = xxxx` in conf file

# Output Example Format

The first number is about segment v2, the latter one is the total count.

```
==========SUMMARY()===========
rowset_count: 289845 / 289845
rowset_disk_size: 84627551189 / 84627551189
rowset_row_count: 1150899153 / 1150899153
===========================================================
==========SUMMARY(table=xxxx)===========
rowset_count: 289845 / 289845
rowset_disk_size: 84627551189 / 84627551189
rowset_row_count: 1150899153 / 1150899153
===========================================================
==========SUMMARY(be=10003 )===========
rowset_count: 79650 / 79650
rowset_disk_size: 24473921575 / 24473921575
rowset_row_count: 331449328 / 331449328
===========================================================
```
