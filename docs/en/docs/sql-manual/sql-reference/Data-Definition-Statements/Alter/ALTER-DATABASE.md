---
{
    "title": "ALTER-DATABASE",
    "language": "en"
}
---

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

## ALTER-DATABASE

### Name

ALTER DATABASE

### Description

This statement is used to set properties of the specified database. (administrator only)

1) Set the database data quota, the unit is B/K/KB/M/MB/G/GB/T/TB/P/PB

```sql
ALTER DATABASE db_name SET DATA QUOTA quota;
```

2) Rename the database

```sql
ALTER DATABASE db_name RENAME new_db_name;
```

3) Set the quota for the number of copies of the database

```sql
ALTER DATABASE db_name SET REPLICA QUOTA quota;
```

illustrate:
    After renaming the database, use the REVOKE and GRANT commands to modify the appropriate user permissions, if necessary.
    The default data quota for the database is 1024GB, and the default replica quota is 1073741824.

4) Modify the properties of an existing database

```sql
ALTER DATABASE db_name SET PROPERTIES ("key"="value", ...); 
```

### Example

1. Set the specified database data volume quota

```sql
ALTER DATABASE example_db SET DATA QUOTA 10995116277760;
The above unit is bytes, which is equivalent to
ALTER DATABASE example_db SET DATA QUOTA 10T;

ALTER DATABASE example_db SET DATA QUOTA 100G;

ALTER DATABASE example_db SET DATA QUOTA 200M;
```

2. Rename the database example_db to example_db2

```sql
ALTER DATABASE example_db RENAME example_db2;
```

3. Set the quota for the number of copies of the specified database

```sql
ALTER DATABASE example_db SET REPLICA QUOTA 102400;
```

4. Modify the default replica distribution policy for tables in db (this operation only applies to newly created tables and will not modify existing tables in db)

```sql
ALTER DATABASE example_db SET PROPERTIES("replication_allocation" = "tag.location.default:2");
```

5. Cancel the default replica distribution policy for tables in db (this operation only applies to newly created tables and will not modify existing tables in db)

```sql
ALTER DATABASE example_db SET PROPERTIES("replication_allocation" = "");
```

### Keywords

```text
ALTER,DATABASE,RENAME
```

### Best Practice

