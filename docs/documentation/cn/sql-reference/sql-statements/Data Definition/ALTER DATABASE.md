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

# ALTER DATABASE
## description
    该语句用于设置指定数据库的属性。（仅管理员使用）
    语法：
        1) 设置数据库数据量配额，单位为B/K/KB/M/MB/G/GB/T/TB/P/PB
            ALTER DATABASE db_name SET DATA QUOTA quota;
            
        2) 重命名数据库
            ALTER DATABASE db_name RENAME new_db_name;
            
        3) 设置数据库的副本数量配额
            ALTER DATABASE db_name SET REPLICA QUOTA quota; 
            
    说明：
        重命名数据库后，如需要，请使用 REVOKE 和 GRANT 命令修改相应的用户权限。
        数据库的默认数据量配额为1024GB，默认副本数量配额为1073741824。

## example
    1. 设置指定数据库数据量配额
        ALTER DATABASE example_db SET DATA QUOTA 10995116277760;
        上述单位为字节,等价于
        ALTER DATABASE example_db SET DATA QUOTA 10T;

        ALTER DATABASE example_db SET DATA QUOTA 100G;

        ALTER DATABASE example_db SET DATA QUOTA 200M;

    2. 将数据库 example_db 重命名为 example_db2
        ALTER DATABASE example_db RENAME example_db2;
        
    3. 设定指定数据库副本数量配额
        ALTER DATABASE example_db SET REPLICA QUOTA 102400; 

## keyword
    ALTER,DATABASE,RENAME
    
