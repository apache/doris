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

#pragma once

#include <filesystem>
#include <utility>

#include "common/status.h"
#include "gen_cpp/BackendService_types.h"

namespace doris {

namespace fs = std::filesystem;

enum class PythonEnvType { CONDA, VENV };

struct PythonVersion {
    std::string full_version;    // e.g. "3.9.16"
    std::string base_path;       // e.g. "/root/anaconda3/envs/python3.9"
    std::string executable_path; // e.g. "{base_path}/bin/python3"

    PythonVersion() = default;

    explicit PythonVersion(std::string full_version, std::string base_path,
                           std::string executable_path)
            : full_version(std::move(full_version)),
              base_path(std::move(base_path)),
              executable_path(std::move(executable_path)) {}

    bool operator==(const PythonVersion& other) const {
        return full_version == other.full_version && base_path == other.base_path &&
               executable_path == other.executable_path;
    }

    const std::string& get_base_path() const { return base_path; }

    const std::string& get_executable_path() const { return executable_path; }

    bool is_valid() const {
        return !full_version.empty() && !base_path.empty() && !executable_path.empty() &&
               fs::exists(base_path) && fs::exists(executable_path);
    }

    std::string to_string() const {
        return fmt::format("[full_version: {}, base_path: {}, executable_path: {}]", full_version,
                           base_path, executable_path);
    }
};

struct PythonEnvironment {
    std::string env_name; // e.g. "base" or "myenv"
    PythonVersion python_version;

    PythonEnvironment(const std::string& name, const PythonVersion& python_version);

    std::string to_string() const;

    bool is_valid() const;

    static Status scan_from_conda_root_path(const fs::path& conda_root_path,
                                            std::vector<PythonEnvironment>* environments);

    static Status scan_from_venv_root_path(const fs::path& venv_root_path,
                                           const std::vector<std::string>& interpreter_paths,
                                           std::vector<PythonEnvironment>* environments);
};

class PythonEnvScanner {
public:
    PythonEnvScanner(const fs::path& env_root_path) : _env_root_path(env_root_path) {}

    virtual ~PythonEnvScanner() = default;

    virtual Status scan() = 0;

    Status get_versions(std::vector<PythonVersion>* versions) const;

    Status get_version(const std::string& runtime_version, PythonVersion* version) const;

    const std::vector<PythonEnvironment>& get_envs() const { return _envs; }

    std::string root_path() const { return _env_root_path.string(); }

    virtual PythonEnvType env_type() const = 0;

    virtual std::string to_string() const = 0;

protected:
    fs::path _env_root_path;
    std::vector<PythonEnvironment> _envs;
};

class CondaEnvScanner : public PythonEnvScanner {
public:
    CondaEnvScanner(const fs::path& python_root_path) : PythonEnvScanner(python_root_path) {}

    ~CondaEnvScanner() override = default;

    Status scan() override;

    std::string to_string() const override;

    PythonEnvType env_type() const override { return PythonEnvType::CONDA; }
};

class VenvEnvScanner : public PythonEnvScanner {
public:
    VenvEnvScanner(const fs::path& python_root_path,
                   const std::vector<std::string>& interpreter_paths)
            : PythonEnvScanner(python_root_path), _interpreter_paths(interpreter_paths) {}

    ~VenvEnvScanner() override = default;

    Status scan() override;

    std::string to_string() const override;

    PythonEnvType env_type() const override { return PythonEnvType::VENV; }

private:
    std::vector<std::string> _interpreter_paths;
};

class PythonVersionManager {
public:
    static PythonVersionManager& instance() {
        static PythonVersionManager instance;
        return instance;
    }

    Status init(PythonEnvType env_type, const fs::path& python_root_path,
                const std::string& python_venv_interpreter_paths);

    Status get_version(const std::string& runtime_version, PythonVersion* version) const {
        return _env_scanner->get_version(runtime_version, version);
    }

    const std::vector<PythonEnvironment>& get_envs() const { return _env_scanner->get_envs(); }

    PythonEnvType env_type() const { return _env_scanner->env_type(); }

    std::string to_string() const { return _env_scanner->to_string(); }

    std::vector<TPythonEnvInfo> env_infos_to_thrift() const;

    std::vector<TPythonPackageInfo> package_infos_to_thrift(
            const std::vector<std::pair<std::string, std::string>>& packages) const;

private:
    std::unique_ptr<PythonEnvScanner> _env_scanner;
};

// List installed pip packages for a given Python version.
// Returns pairs of (package_name, version).
Status list_installed_packages(const PythonVersion& version,
                               std::vector<std::pair<std::string, std::string>>* packages);

} // namespace doris

namespace std {
template <>
struct hash<doris::PythonVersion> {
    size_t operator()(const doris::PythonVersion& v) const noexcept {
        return hash<string> {}(v.full_version);
    }
};
} // namespace std
