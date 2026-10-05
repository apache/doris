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

Steps of row-to-column conversion:

1. Modify the corresponding cluster configuration in `conf`.

2. Run `python convert_row_to_column.py` to generate all row storage to column storage commands.

3. If the script runs without any results, it means that there is no row storage table in the cluster, so nothing need to be done.

4. If there is a table in the script that fails to obtain information.

    Please use `show create table` in the cluster to see the `storage_type` of the table. If it is row storage, please manually convert it.

5. After the schema change is completed, the old schema needs to be deleted after 10 minutes.

    Please wait for a while to make sure that the data of the old schema has been deleted, and then upgrade.
    Otherwise, the BE process will exit with an error when it is automatically detected when it starts.

6. Run `python convert_row_to_column.py` again to verify that there are no rows in the table before upgrading.
