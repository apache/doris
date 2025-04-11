// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
suite("cse") {
    // cse should not extract expression use for lambda, such as ArrayItemSlot and ArrayItemReference
    sql """
        drop table if exists array_cse;
    """
    sql """
        create table array_cse(c1 int, c2 array<varchar(255)>) PROPERTIES ("replication_allocation" = "tag.location.default: 1");
    """
    sql """
        insert into array_cse values(1, [1,2,3]);
    """
    sql """
        sync
    """
    sql """
        SELECT array_map(x-> if(left(x, 5) = '12345', x, left(x, 5)), c2) FROM array_cse;
    """
    sql """
        SELECT c0, c0 FROM (SELECT ARRAY_MAP(x-> if(left(x, 5), x, left(x, 5)), `c2`) as `c0` FROM array_cse) t
    """
    
}

