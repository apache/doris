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

#include "recycler/util.h"

#include <glog/logging.h>

#include <cstdint>

#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

namespace doris::cloud {
namespace config {
extern int32_t recycle_job_lease_expired_ms;
} // namespace config

int get_all_instances(TxnKv* txn_kv, std::vector<InstanceInfoPB>& res) {
    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(INFO) << "failed to init txn, err=" << err;
        return -1;
    }

    std::unique_ptr<RangeGetIterator> it;
    do {
        TxnErrorCode err = txn->get(key0, key1, &it);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get instance, err=" << err;
            return -1;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;

            InstanceInfoPB instance_info;
            if (!instance_info.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed instance info, key=" << hex(k);
                return -1;
            }
            res.push_back(std::move(instance_info));
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    return 0;
}

int prepare_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 int64_t interval_ms) {
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
        return -1;
    }
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    JobRecyclePB job_info;

    auto is_expired = [&]() {
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
            // if failed to parse, just recycle it.
            return true;
        }
        DCHECK(job_info.instance_id() == instance_id);
        if (job_info.status() == JobRecyclePB::BUSY) {
            if (now < job_info.expiration_time_ms()) {
                LOG(INFO) << "job is busy. host=" << job_info.ip_port()
                          << " expiration=" << job_info.expiration_time_ms()
                          << " instance=" << instance_id;
                return false;
            }
        }

        bool finish_expired = now - job_info.last_ctime_ms() > interval_ms;
        if (!finish_expired) {
            LOG(INFO) << "the time since last finished job is too short. host="
                      << job_info.ip_port() << " ctime=" << job_info.last_ctime_ms()
                      << " instance=" << instance_id;
        }

        return finish_expired;
    };

    if (err == TxnErrorCode::TXN_KEY_NOT_FOUND || is_expired()) {
        job_info.set_status(JobRecyclePB::BUSY);
        job_info.set_instance_id(instance_id);
        job_info.set_ip_port(ip_port);
        job_info.set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to commit, err=" << err << " key=" << hex(key);
            return -1;
        }
        return 0;
    }
    return 1;
}

void finish_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 bool success, int64_t ctime_ms) {
    std::string val;
    int retry_times = 0;
    do {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to create txn";
            return;
        }
        err = txn->get(key, &val);
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
            return;
        }

        using namespace std::chrono;
        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
            return;
        }
        DCHECK(job_info.instance_id() == instance_id);
        if (job_info.ip_port() != ip_port) {
            LOG(WARNING) << "job is doing at other machine: " << job_info.ip_port()
                         << " key=" << hex(key);
            return;
        }
        if (job_info.status() != JobRecyclePB::BUSY) {
            LOG(WARNING) << "job is not busy, key=" << hex(key);
            return;
        }
        job_info.set_status(JobRecyclePB::IDLE);
        job_info.set_instance_id(instance_id);
        job_info.set_last_finish_time_ms(now);
        job_info.set_last_ctime_ms(ctime_ms);
        if (success) {
            job_info.set_last_success_time_ms(now);
        }
        val = job_info.SerializeAsString();
        txn->put(key, val);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_OK) {
            LOG(INFO) << "succ to commit to finish recycle job, key=" << hex(key);
            return;
        }
        // maybe conflict with the commit of the leased thread
        LOG(WARNING) << "failed to commit to finish recycle job, err=" << err << " key=" << hex(key)
                     << " retry_times=" << retry_times;
    } while (retry_times++ < 3);
    LOG(WARNING) << "finally failed to commit to finish recycle job, key=" << hex(key);
}

int lease_instance_recycle_job(TxnKv* txn_kv, std::string_view key, const std::string& instance_id,
                               const std::string& ip_port) {
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to create txn";
        return -1;
    }
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to get kv, err=" << err << " key=" << hex(key);
        return -1;
    }

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    JobRecyclePB job_info;
    if (!job_info.ParseFromString(val)) {
        LOG(WARNING) << "failed to parse JobRecyclePB, key=" << hex(key);
        return 1;
    }
    DCHECK(job_info.instance_id() == instance_id);
    if (job_info.ip_port() != ip_port) {
        LOG(WARNING) << "job is doing at other machine: " << job_info.ip_port()
                     << " key=" << hex(key);
        return 1;
    }
    if (job_info.status() != JobRecyclePB::BUSY) {
        LOG(WARNING) << "job is not busy, key=" << hex(key);
        return 1;
    }
    job_info.set_expiration_time_ms(now + config::recycle_job_lease_expired_ms);
    val = job_info.SerializeAsString();
    txn->put(key, val);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to commit, failed to lease recycle job, err=" << err
                     << " key=" << hex(key);
        return -1;
    }
    return 0;
}

} // namespace doris::cloud
