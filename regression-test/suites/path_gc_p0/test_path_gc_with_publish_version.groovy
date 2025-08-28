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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite('test_path_gc_with_publish_version', 'docker') {
    if (isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.beConfigs += [
        'path_gc_check=true',
        'path_gc_check_interval_second=1',
        'path_gc_check_step=0',
        'generate_tablet_meta_checkpoint_tasks_interval_secs=1',
        'tablet_meta_checkpoint_min_new_rowsets_num=1',
        'sys_log_verbose_modules=*',
    ]
    options.feNum = 1
    options.beNum = 1
    
    docker(options) {
        def be1 = cluster.getBeByIndex(1)
        be1.enableDebugPoint('_path_gc_thread_callback.interval.eq.1ms', null)
        be1.enableDebugPoint('_path_gc_thread_callback.always.do', null)

        sql "SET GLOBAL insert_visible_timeout_ms = 5000"
        // wait path gc interval time to 1ms 
        Thread.sleep(1000)

        sql """
            CREATE TABLE tbl (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1  PROPERTIES (
            "replication_allocation" = "tag.location.default: 1")
        """

        def result = sql_return_maparray """show tablets from tbl"""
        log.info("show tablet result {}", result)
        Long tabletId = result.TabletId[0] as Long

        be1.enableDebugPoint('EnginePublishVersionTask.handle.block_add_rowsets', null)
        be1.enableDebugPoint('EnginePublishVersionTask.handle.after_add_inc_rowset_rowsets_block', null)
        sql 'INSERT INTO tbl VALUES (1, 10)'
        // Rs not in pending

        be1.enableDebugPoint('DataDir::_perform_rowset_gc.simulation.slow', [tablet_id: tabletId])
        Thread.sleep(5000)

        be1.disableDebugPoint('EnginePublishVersionTask.handle.block_add_rowsets')
        Thread.sleep(5000)
        // publish continue
        // checkpoint clean Rs manager
        // path gc continue
        be1.disableDebugPoint('DataDir::_perform_rowset_gc.simulation.slow') 
        be1.disableDebugPoint('EnginePublishVersionTask.handle.after_add_inc_rowset_rowsets_block')
        Thread.sleep(3 * 1000)

        result = sql """select * from tbl"""
        log.info("result = {}", result)
    }
}
