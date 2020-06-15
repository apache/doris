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

#include "runtime/tmp_file_mgr.h"

#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread/locks.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/filesystem.hpp>
// #include <gutil/strings/substitute.h>
// #include <gutil/strings/join.h>

#include "olap/storage_engine.h"
#include "util/uid_util.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/filesystem_util.h"
#include "runtime/exec_env.h"

using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::split;
using boost::algorithm::token_compress_on;
using boost::filesystem::absolute;
using boost::filesystem::path;
using boost::uuids::random_generator;

using std::string;
using std::vector;

namespace doris {

const std::string _s_tmp_sub_dir_name = "doris-scratch";
const uint64_t _s_available_space_threshold_mb = 1024;

// Metric keys
const std::string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS = "tmp_file_mgr.active_scratch_dirs";
const std::string TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST = "tmp_file_mgr.active_scratch_dirs.list";

TmpFileMgr::TmpFileMgr(ExecEnv* exec_env) :
        _exec_env(exec_env), _initialized(false), _dir_status_lock(), _tmp_dirs() { }
        // _num_active_scratch_dirs_metric(NULL), _active_scratch_dirs_metric(NULL) {}

Status TmpFileMgr::init(MetricRegistry* metrics) {
    vector<string> all_tmp_dirs;
    for (auto& path : _exec_env->store_paths()) {
        all_tmp_dirs.emplace_back(path.path);
    }
    return init_custom(all_tmp_dirs, true, metrics);
}

Status TmpFileMgr::init_custom(
        const vector<string>& tmp_dirs, bool one_dir_per_device, MetricRegistry* metrics) {
    DCHECK(!_initialized);
    if (tmp_dirs.empty()) {
        LOG(WARNING) << "Running without spill to disk: no scratch directories provided.";
    }

    vector<bool> is_tmp_dir_on_disk(DiskInfo::num_disks(), false);
    // For each tmp directory, find the disk it is on,
    // so additional tmp directories on the same disk can be skipped.
    for (int i = 0; i < tmp_dirs.size(); ++i) {
        boost::filesystem::path tmp_path(
                boost::trim_right_copy_if(tmp_dirs[i], is_any_of("/")));
        tmp_path = boost::filesystem::absolute(tmp_path);
        path scratch_subdir_path(tmp_path / _s_tmp_sub_dir_name);
        // tmp_path must be a writable directory.
        Status status = FileSystemUtil::verify_is_directory(tmp_path.string());
        if (!status.ok()) {
            LOG(WARNING) << "Cannot use directory " << tmp_path.string() << " for scratch: "
                << status.get_error_msg();
            continue;
        }
        // Find the disk id of tmp_path. Add the scratch directory if there isn't another
        // directory on the same disk (or if we don't know which disk it is on).
        int disk_id = DiskInfo::disk_id(tmp_path.c_str());
        if (!one_dir_per_device || disk_id < 0 || !is_tmp_dir_on_disk[disk_id]) {
            uint64_t available_space;
            RETURN_IF_ERROR(
                    FileSystemUtil::get_space_available(tmp_path.string(), &available_space));
            if (available_space < _s_available_space_threshold_mb * 1024 * 1024) {
                LOG(WARNING) << "Filesystem containing scratch directory " << tmp_path
                        << " has less than " << _s_available_space_threshold_mb
                        << "MB available.";
            }
            // Create the directory, destroying if already present. If this succeeds, we will
            // have an empty writable scratch directory.
            status = FileSystemUtil::create_directory(scratch_subdir_path.string());
            if (status.ok()) {
                if (disk_id >= 0) {
                    is_tmp_dir_on_disk[disk_id] = true;
                }
                LOG(INFO) << "Using scratch directory " << scratch_subdir_path.string()
                        << " on disk " << disk_id;
                _tmp_dirs.push_back(Dir(scratch_subdir_path.string(), false));
            } else {
                LOG(WARNING) << "Could not remove and recreate directory "
                        << scratch_subdir_path.string() << ": cannot use it for scratch. "
                        << "Error was: " << status.get_error_msg();
            }
        }
    }

    DCHECK(metrics != NULL);
    _num_active_scratch_dirs_metric.reset(new IntGauge(MetricUnit::NOUNIT));
    metrics->register_metric("active_scratch_dirs", _num_active_scratch_dirs_metric.get());
    //_active_scratch_dirs_metric = metrics->register_metric(new SetMetric<std::string>(
    //        TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST,
    //        std::set<std::string>()));
    // TODO(zc):
    // _active_scratch_dirs_metric = SetMetric<string>::CreateAndRegister(
    // metrics, TMP_FILE_MGR_ACTIVE_SCRATCH_DIRS_LIST, std::set<std::string>());
    _num_active_scratch_dirs_metric->set_value(_tmp_dirs.size());
    // for (int i = 0; i < _tmp_dirs.size(); ++i) {
    //     _active_scratch_dirs_metric->add(_tmp_dirs[i].path());
    // }

    _initialized = true;

    if (_tmp_dirs.empty() && !tmp_dirs.empty()) {
        LOG(ERROR) << "Running without spill to disk: could not use any scratch "
            << "directories in list: " << join(tmp_dirs, ",")
            << ". See previous warnings for information on causes.";
    }
    return Status::OK();
}

Status TmpFileMgr::get_file(
        const DeviceId& device_id,
        const TUniqueId& query_id,
        File** new_file) {
    DCHECK(_initialized);
    DCHECK_GE(device_id, 0);
    DCHECK_LT(device_id, _tmp_dirs.size());
    if (is_blacklisted(device_id)) {
        std::stringstream error_msg;
        error_msg << "path is blacklist. path: " << _tmp_dirs[device_id].path();
        return Status::InternalError(error_msg.str());
    }

    // Generate the full file path.
    string unique_name = boost::lexical_cast<string>(boost::uuids::random_generator()());
    std::stringstream file_name;
    file_name << print_id(query_id) << "_" << unique_name;
    path new_file_path(_tmp_dirs[device_id].path());
    new_file_path /= file_name.str();

    *new_file = new File(this, device_id, new_file_path.string());
    return Status::OK();
}

string TmpFileMgr::get_tmp_dir_path(DeviceId device_id) const {
    DCHECK(_initialized);
    DCHECK_GE(device_id, 0);
    DCHECK_LT(device_id, _tmp_dirs.size());
    return _tmp_dirs[device_id].path();
}

void TmpFileMgr::blacklist_device(DeviceId device_id) {
    DCHECK(_initialized);
    DCHECK(device_id >= 0 && device_id < _tmp_dirs.size());
    bool added = true;
    {
        boost::lock_guard<SpinLock> l(_dir_status_lock);
        added = _tmp_dirs[device_id].blacklist();
    }
    if (added) {
        _num_active_scratch_dirs_metric->increment(-1);
        // _active_scratch_dirs_metric->remove(_tmp_dirs[device_id].path());
    }
}

bool TmpFileMgr::is_blacklisted(DeviceId device_id) {
    DCHECK(_initialized);
    DCHECK(device_id >= 0 && device_id < _tmp_dirs.size());
    boost::lock_guard<SpinLock> l(_dir_status_lock);
    return _tmp_dirs[device_id].is_blacklisted();
}

int TmpFileMgr::num_active_tmp_devices() {
    DCHECK(_initialized);
    boost::lock_guard<SpinLock> l(_dir_status_lock);
    int num_active = 0;
    for (int device_id = 0; device_id < _tmp_dirs.size(); ++device_id) {
        if (!_tmp_dirs[device_id].is_blacklisted()) {
            ++num_active;
        }
    }
    return num_active;
}

vector<TmpFileMgr::DeviceId> TmpFileMgr::active_tmp_devices() {
    vector<TmpFileMgr::DeviceId> devices;
    // Allocate vector before we grab lock
    devices.reserve(_tmp_dirs.size());
    {
        boost::lock_guard<SpinLock> l(_dir_status_lock);
        for (DeviceId device_id = 0; device_id < _tmp_dirs.size(); ++device_id) {
            if (!_tmp_dirs[device_id].is_blacklisted()) {
                devices.push_back(device_id);
            }
        }
    }
    return devices;
}

TmpFileMgr::File::File(TmpFileMgr* mgr, DeviceId device_id, const string& path) :
        _mgr(mgr),
        _path(path),
        _device_id(device_id),
        _current_size(0),
        _blacklisted(false) {
}

Status TmpFileMgr::File::allocate_space(int64_t write_size, int64_t* offset) {
    DCHECK_GT(write_size, 0);
    Status status;
    if (_mgr->is_blacklisted(_device_id)) {
        _blacklisted = true;
        std::stringstream error_msg;
        error_msg << "path is blacklist. path: " << _path;
        return Status::InternalError(error_msg.str());
    }
    if (_current_size == 0) {
        // First call to AllocateSpace. Create the file.
        status = FileSystemUtil::create_file(_path);
        if (!status.ok()) {
            report_io_error(status.get_error_msg());
            return status;
        }
        _disk_id = DiskInfo::disk_id(_path.c_str());
    }
    int64_t new_size = _current_size + write_size;
    status = FileSystemUtil::resize_file(_path, new_size);
    if (!status.ok()) {
        report_io_error(status.get_error_msg());
        return status;
    }
    *offset = _current_size;
    _current_size = new_size;
    return Status::OK();
}

void TmpFileMgr::File::report_io_error(const std::string& error_msg) {
    LOG(ERROR) << "Error for temporary file '" << _path << "': " << error_msg;
}

#if 0
void TmpFileMgr::File::report_io_error(const ErrorMsg& msg) {
    LOG(ERROR) << "Error for temporary file '" << _path << "': " << msg.msg();
    // IMPALA-2305: avoid blacklisting to prevent test failures.
    // blacklisted_ = true;
    // mgr_->BlacklistDevice(device_id_);
}
#endif

Status TmpFileMgr::File::remove() {
    if (_current_size > 0) {
        FileSystemUtil::remove_paths(vector<string>(1, _path));
    }
    return Status::OK();
}

} //namespace doris
