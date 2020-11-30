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

#include "olap/tablet_sync_service.h"

using namespace std;

namespace doris {

TabletSyncService::TabletSyncService() {
    // TODO(ygl): add new config
    _fetch_rowset_pool = new BatchProcessThreadPool<FetchRowsetMetaTask>(
            3,     // thread num
            10000, // queue size
            10,    // batch size
            std::bind<void>(std::mem_fn(&TabletSyncService::_fetch_rowset_meta_thread), this,
                            std::placeholders::_1));

    _push_rowset_pool = new BatchProcessThreadPool<PushRowsetMetaTask>(
            3,     // thread num
            10000, // queue size
            10,    // batch size
            std::bind<void>(std::mem_fn(&TabletSyncService::_push_rowset_meta_thread), this,
                            std::placeholders::_1));
    _fetch_tablet_pool = new BatchProcessThreadPool<FetchTabletMetaTask>(
            3,     // thread num
            10000, // queue size
            10,    // batch size
            std::bind<void>(std::mem_fn(&TabletSyncService::_fetch_tablet_meta_thread), this,
                            std::placeholders::_1));
    _push_tablet_pool = new BatchProcessThreadPool<PushTabletMetaTask>(
            3,     // thread num
            10000, // queue size
            10,    // batch size
            std::bind<void>(std::mem_fn(&TabletSyncService::_push_tablet_meta_thread), this,
                            std::placeholders::_1));
}

TabletSyncService::~TabletSyncService() {
    if (_fetch_rowset_pool != nullptr) {
        delete _fetch_rowset_pool;
    }
    if (_push_rowset_pool != nullptr) {
        delete _push_rowset_pool;
    }
    if (_fetch_tablet_pool != nullptr) {
        delete _fetch_tablet_pool;
    }
    if (_push_tablet_pool != nullptr) {
        delete _push_tablet_pool;
    }
}

// fetch rowset meta and data to local metastore
// when add a task, should check if the task already exist
// if the rowset meta is not published, should commit it to local meta store
// if it is already visible, then just add it to tablet
// tablet_id + txn_id could find a unique rowset
// return a future object, caller could using it to wait the task to finished
// and check the status
std::future<OLAPStatus> TabletSyncService::fetch_rowset(TabletSharedPtr tablet, int64_t txn_id,
                                                        bool load_data) {
    auto pro = make_shared<promise<OLAPStatus>>();
    FetchRowsetMetaTask fetch_task;
    fetch_task.tablet = tablet;
    fetch_task.txn_id = txn_id;
    fetch_task.load_data = load_data;
    fetch_task.pro = pro;
    _fetch_rowset_pool->offer(fetch_task);
    return pro->get_future();
}

// fetch rowset meta and data using version
std::future<OLAPStatus> TabletSyncService::fetch_rowset(TabletSharedPtr tablet, Version& version,
                                                        bool load_data) {
    auto pro = make_shared<promise<OLAPStatus>>();
    FetchRowsetMetaTask fetch_task;
    fetch_task.tablet = tablet;
    fetch_task.txn_id = -1;
    fetch_task.load_data = load_data;
    fetch_task.version = version;
    fetch_task.pro = pro;
    _fetch_rowset_pool->offer(fetch_task);
    return pro->get_future();
}

std::future<OLAPStatus> TabletSyncService::push_rowset_meta(RowsetMetaPB& rowset_meta) {
    auto pro = make_shared<promise<OLAPStatus>>();
    PushRowsetMetaTask push_task;
    push_task.rowset_meta_pb = rowset_meta;
    push_task.op_type = MetaOpType::PUSH_META;
    push_task.pro = pro;
    _push_rowset_pool->offer(push_task);
    return pro->get_future();
}

std::future<OLAPStatus> TabletSyncService::delete_rowset_meta(RowsetMetaPB& rowset_meta) {
    auto pro = make_shared<promise<OLAPStatus>>();
    PushRowsetMetaTask push_task;
    push_task.rowset_meta_pb = rowset_meta;
    push_task.op_type = MetaOpType::DELETE_META;
    push_task.pro = pro;
    _push_rowset_pool->offer(push_task);
    return pro->get_future();
}

// fetch both tablet meta and all rowset meta
// when create a tablet, if it's eco_mode and term > 1 then should fetch
// all rowset and tablet meta from remote meta store
// Maybe, it's better to add a callback function here
std::future<OLAPStatus> TabletSyncService::fetch_tablet_meta(TabletSharedPtr tablet,
                                                             bool load_data) {
    auto pro = make_shared<promise<OLAPStatus>>();
    FetchTabletMetaTask fetch_task;
    fetch_task.tablet = tablet;
    fetch_task.load_data = load_data;
    fetch_task.pro = pro;
    _fetch_tablet_pool->offer(fetch_task);
    return pro->get_future();
}

std::future<OLAPStatus> TabletSyncService::push_tablet_meta(TabletMetaPB& tablet_meta) {
    auto pro = make_shared<promise<OLAPStatus>>();
    PushTabletMetaTask push_task;
    push_task.tablet_meta_pb = tablet_meta;
    push_task.pro = pro;
    _push_tablet_pool->offer(push_task);
    return pro->get_future();
}

void TabletSyncService::_fetch_rowset_meta_thread(std::vector<FetchRowsetMetaTask> tasks) {
    return;
}

void TabletSyncService::_push_rowset_meta_thread(std::vector<PushRowsetMetaTask> tasks) {
    return;
}

void TabletSyncService::_fetch_tablet_meta_thread(std::vector<FetchTabletMetaTask> tasks) {
    return;
}

void TabletSyncService::_push_tablet_meta_thread(std::vector<PushTabletMetaTask> tasks) {
    return;
}

} // namespace doris
