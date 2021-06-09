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

#ifndef DORIS_BE_SRC_OLAP_TABLET_SYNC_SERVICE_H
#define DORIS_BE_SRC_OLAP_TABLET_SYNC_SERVICE_H

#include <future>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "util/batch_process_thread_pool.hpp"

#define FETCH_DATA true
#define NOT_FETCH_DATA false

namespace doris {

enum MetaOpType { PUSH_META, DELETE_META };

struct FetchRowsetMetaTask {
public:
    int priority;
    TabletSharedPtr tablet;
    int64_t txn_id;
    Version version;
    bool load_data;
    std::shared_ptr<std::promise<OLAPStatus>> pro;
    bool operator<(const FetchRowsetMetaTask& o) const { return priority < o.priority; }

    FetchRowsetMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // FetchRowsetMetaTask

struct PushRowsetMetaTask {
public:
    int priority;
    MetaOpType op_type;
    RowsetMetaPB rowset_meta_pb;
    std::shared_ptr<std::promise<OLAPStatus>> pro;
    bool operator<(const PushRowsetMetaTask& o) const { return priority < o.priority; }

    PushRowsetMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // PushRowsetMetaTask

struct FetchTabletMetaTask {
public:
    int priority;
    TabletSharedPtr tablet;
    bool load_data;
    std::shared_ptr<std::promise<OLAPStatus>> pro;
    bool operator<(const FetchTabletMetaTask& o) const { return priority < o.priority; }

    FetchTabletMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // FetchTabletMetaTask

struct PushTabletMetaTask {
public:
    int priority;
    TabletMetaPB tablet_meta_pb;
    std::shared_ptr<std::promise<OLAPStatus>> pro;
    bool operator<(const PushTabletMetaTask& o) const { return priority < o.priority; }

    PushTabletMetaTask& operator++() {
        priority += 2;
        return *this;
    }
}; // PushTabletMetaTask

// sync meta and data from remote meta store to local meta store
// all method should consider dedup duplicate calls
// for example, thread1 call sync meta and thread2 call sync meta, if they are the same
// should not sync twice
class TabletSyncService {
public:
    TabletSyncService();
    ~TabletSyncService();
    // fetch rowset meta and data to local metastore
    // when add a task, should check if the task already exist
    // if the rowset meta is not published, should commit it to local meta store
    // if it is already visible, then just add it to tablet
    // tablet_id + txn_id could find a unique rowset
    // return a future object, caller could using it to wait the task to finished
    // and check the status
    std::future<OLAPStatus> fetch_rowset(TabletSharedPtr tablet, int64_t txn_id, bool load_data);

    // fetch rowset meta and data using version
    std::future<OLAPStatus> fetch_rowset(TabletSharedPtr tablet, Version& version, bool load_data);

    // save the rowset meta pb to remote meta store
    // !!!! the caller should not own tablet map lock or tablet lock because
    // this method will call tablet manager to get tablet info
    std::future<OLAPStatus> push_rowset_meta(RowsetMetaPB& rowset_meta);

    std::future<OLAPStatus> delete_rowset_meta(RowsetMetaPB& rowset_meta);

    // fetch both tablet meta and all rowset meta
    // when create a tablet, if it's eco_mode and term > 1 then should fetch
    // all rowset and tablet meta from remote meta store
    // Maybe, it's better to add a callback function here
    std::future<OLAPStatus> fetch_tablet_meta(TabletSharedPtr tablet, bool load_data);

    // save the tablet meta pb to remote meta store
    std::future<OLAPStatus> push_tablet_meta(TabletMetaPB& tablet_meta);

private:
    void _fetch_rowset_meta_thread(std::vector<FetchRowsetMetaTask> tasks);
    void _push_rowset_meta_thread(std::vector<PushRowsetMetaTask> tasks);
    void _fetch_tablet_meta_thread(std::vector<FetchTabletMetaTask> tasks);
    void _push_tablet_meta_thread(std::vector<PushTabletMetaTask> tasks);

private:
    BatchProcessThreadPool<FetchRowsetMetaTask>* _fetch_rowset_pool = nullptr;
    BatchProcessThreadPool<PushRowsetMetaTask>* _push_rowset_pool = nullptr;
    BatchProcessThreadPool<FetchTabletMetaTask>* _fetch_tablet_pool = nullptr;
    BatchProcessThreadPool<PushTabletMetaTask>* _push_tablet_pool = nullptr;
}; // TabletSyncService

} // namespace doris
#endif // DORIS_BE_SRC_OLAP_TABLET_SYNC_SERVICE_H