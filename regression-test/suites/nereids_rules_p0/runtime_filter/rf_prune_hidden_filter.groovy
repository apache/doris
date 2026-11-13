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

suite("rf_prune_hidden_filter") {
    sql "set enable_runtime_filter_prune=true"
    sql "set runtime_filter_type=2"
    sql "set disable_join_reorder=true"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"

    sql "drop table if exists rf_prune_hidden_fact"
    sql """
        create table rf_prune_hidden_fact (id int, agent_code int, username varchar(32))
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1")
    """
    sql "drop table if exists rf_prune_hidden_agent"
    sql """
        create table rf_prune_hidden_agent (agent_code int, bloc_id int)
        unique key(agent_code)
        distributed by hash(agent_code) buckets 1
        properties("replication_num"="1", "enable_unique_key_merge_on_write"="false")
    """
    sql "drop table if exists rf_prune_hidden_bloc"
    sql """
        create table rf_prune_hidden_bloc (id int, bloc_code varchar(32))
        duplicate key(id)
        distributed by hash(id) buckets 1
        properties("replication_num"="1")
    """

    // The agent scan receives a bloc_id RF below its hidden delete-sign Filter.
    // That Filter must pass the effective source through to the agent_code join.
    explain {
        sql """
            shape plan select f.id from rf_prune_hidden_fact f
            join [broadcast] rf_prune_hidden_agent e on f.agent_code = e.agent_code
            join [broadcast] rf_prune_hidden_bloc b on e.bloc_id = b.id
            where b.bloc_code = 'keep' and f.username like '%user%'
        """
        contains "PhysicalOlapScan[rf_prune_hidden_fact] apply RFs:"
        contains "PhysicalOlapScan[rf_prune_hidden_agent] apply RFs:"
    }

    // A hidden delete-sign predicate alone must not make an ineffective RF useful.
    explain {
        sql """
            shape plan select f.id from rf_prune_hidden_fact f
            join [broadcast] rf_prune_hidden_agent e on f.agent_code = e.agent_code
        """
        notContains "build RFs"
    }

    explain {
        sql """
            shape plan select f.id from rf_prune_hidden_fact f
            join [broadcast] rf_prune_hidden_agent e on f.agent_code = e.agent_code
            where e.bloc_id = 1
        """
        contains "PhysicalOlapScan[rf_prune_hidden_fact] apply RFs:"
    }
}
