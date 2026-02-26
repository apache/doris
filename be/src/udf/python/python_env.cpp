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

#include "python_env.h"

#include <fmt/core.h>
#include <rapidjson/document.h>

#include <filesystem>
#include <memory>
#include <regex>
#include <vector>

#include "common/status.h"
#include "gen_cpp/BackendService_types.h"
#include "udf/python/python_server.h"
#include "util/string_util.h"

namespace doris {

namespace fs = std::filesystem;

static std::string _python_env_type_to_string(PythonEnvType env_type) {
    switch (env_type) {
    case PythonEnvType::CONDA:
        return "conda";
    case PythonEnvType::VENV:
        return "venv";
    }
    return "unknown";
}

// extract python version by executing `python --version` and extract "3.9.16" from "Python 3.9.16"
// @param python_path: path to python executable, e.g. "/opt/miniconda3/envs/myenv/bin/python"
// @param version: extracted python version, e.g. "3.9.16"
static Status extract_python_version(const std::string& python_path, std::string* version) {
    static std::regex python_version_re(R"(^Python (\d+\.\d+\.\d+))");

    if (!fs::exists(python_path)) {
        return Status::NotFound("Python executable not found: {}", python_path);
    }

    std::string cmd = fmt::format("\"{}\" --version", python_path);
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        return Status::InternalError("Failed to run: {}", cmd);
    }

    std::string result;
    char buf[128];
    while (fgets(buf, sizeof(buf), pipe)) {
        result += buf;
    }
    pclose(pipe);

    std::smatch match;
    if (std::regex_search(result, match, python_version_re)) {
        *version = match[1].str();
        return Status::OK();
    }

    return Status::InternalError("Failed to extract Python version from path: {}, result: {}",
                                 python_path, result);
}

PythonEnvironment::PythonEnvironment(const std::string& name, const PythonVersion& python_version)
        : env_name(name), python_version(python_version) {}

std::string PythonEnvironment::to_string() const {
    return fmt::format(
            "[env_name: {}, env_base_path: {}, python_base_path: {}, python_full_version: {}]",
            env_name, python_version.base_path, python_version.executable_path,
            python_version.full_version);
}

bool PythonEnvironment::is_valid() const {
    if (!python_version.is_valid()) return false;

    auto perms = fs::status(python_version.executable_path).permissions();
    if ((perms & fs::perms::owner_exec) == fs::perms::none) {
        return false;
    }

    std::string version;
    if (!extract_python_version(python_version.executable_path, &version).ok()) {
        LOG(WARNING) << "Failed to extract python version from path: "
                     << python_version.executable_path;
        return false;
    }

    return python_version.full_version == version;
}

// Scan for environments under the /{conda_root_path}/envs directory from the conda root.
Status PythonEnvironment::scan_from_conda_root_path(const fs::path& conda_root_path,
                                                    std::vector<PythonEnvironment>* environments) {
    DCHECK(!conda_root_path.empty() && environments != nullptr);

    fs::path envs_dir = conda_root_path / "envs";
    if (!fs::exists(envs_dir) || !fs::is_directory(envs_dir)) {
        return Status::NotFound("Conda envs directory not found: {}", envs_dir.string());
    }

    for (const auto& entry : fs::directory_iterator(envs_dir)) {
        if (!entry.is_directory()) continue;

        std::string env_name = entry.path().filename(); // e.g. "myenv"
        std::string env_base_path = entry.path();       // e.g. "/opt/miniconda3/envs/myenv"
        std::string python_path =
                env_base_path + "/bin/python"; // e.g. "/{env_base_path}/bin/python"
        std::string python_full_version;       // e.g. "3.9.16"
        RETURN_IF_ERROR(extract_python_version(python_path, &python_full_version));
        size_t pos = python_full_version.find_last_of('.');

        if (UNLIKELY(pos == std::string::npos)) {
            return Status::InvalidArgument("Invalid python version: {}", python_full_version);
        }

        PythonVersion python_version(python_full_version, env_base_path, python_path);
        PythonEnvironment conda_env(env_name, python_version);

        if (UNLIKELY(!conda_env.is_valid())) {
            LOG(WARNING) << "Invalid conda environment: " << conda_env.to_string();
            continue;
        }

        environments->push_back(std::move(conda_env));
    }

    if (environments->empty()) {
        return Status::NotFound("No conda python environments found");
    }

    return Status::OK();
}

Status PythonEnvironment::scan_from_venv_root_path(
        const fs::path& venv_root_path, const std::vector<std::string>& interpreter_paths,
        std::vector<PythonEnvironment>* environments) {
    DCHECK(!venv_root_path.empty() && environments != nullptr);

    for (const auto& interpreter_path : interpreter_paths) {
        if (!fs::exists(interpreter_path) || !fs::is_regular_file(interpreter_path)) {
            return Status::NotFound("Interpreter path not found: {}", interpreter_path);
        }
        std::string python_full_version;
        RETURN_IF_ERROR(extract_python_version(interpreter_path, &python_full_version));
        size_t pos = python_full_version.find_last_of('.');
        if (UNLIKELY(pos == std::string::npos)) {
            return Status::InvalidArgument("Invalid python version: {}", python_full_version);
        }
        // Extract major.minor version (e.g., "3.12" from "3.12.0")
        std::string python_major_minor_version = python_full_version.substr(0, pos);

        std::string env_name = fmt::format("python{}", python_full_version); // e.g. "python3.9.16"
        std::string env_base_path = fmt::format("{}/{}", venv_root_path.string(),
                                                env_name); // e.g. "/opt/venv/python3.9.16"
        std::string python_path =
                fmt::format("{}/bin/python", env_base_path); // e.g. "/{venv_base_path}/bin/python"

        if (!fs::exists(env_base_path) || !fs::exists(python_path)) {
            fs::create_directories(env_base_path);
            // Use --system-site-packages to inherit packages from system Python
            // This ensures pandas and pyarrow are available if installed in system
            std::string create_venv_cmd = fmt::format("{} -m venv --system-site-packages {}",
                                                      interpreter_path, env_base_path);

            if (system(create_venv_cmd.c_str()) != 0 || !fs::exists(python_path)) {
                return Status::RuntimeError("Failed to create python virtual environment, cmd: {}",
                                            create_venv_cmd);
            }
        }

        // Use major.minor version for site-packages path (e.g., "python3.12")
        std::string python_dependency_path = fmt::format("{}/lib/python{}/site-packages",
                                                         env_base_path, python_major_minor_version);

        if (!fs::exists(python_dependency_path)) {
            return Status::NotFound("Python dependency path not found: {}", python_dependency_path);
        }

        PythonVersion python_version(python_full_version, env_base_path, python_path);
        PythonEnvironment venv_env(env_name, python_version);

        if (UNLIKELY(!venv_env.is_valid())) {
            LOG(WARNING) << "Invalid venv environment: " << venv_env.to_string();
            continue;
        }

        environments->push_back(std::move(venv_env));
    }

    if (environments->empty()) {
        return Status::NotFound("No venv python environments found");
    }

    return Status::OK();
}

Status PythonEnvScanner::get_versions(std::vector<PythonVersion>* versions) const {
    DCHECK(versions != nullptr);
    if (_envs.empty()) {
        return Status::InternalError("not found available version");
    }
    for (const auto& env : _envs) {
        versions->push_back(env.python_version);
    }
    return Status::OK();
}

Status PythonEnvScanner::get_version(const std::string& runtime_version,
                                     PythonVersion* version) const {
    if (_envs.empty()) {
        return Status::InternalError("not found available version");
    }
    std::string_view runtime_version_view(runtime_version);
    runtime_version_view = trim(runtime_version_view);
    for (const auto& env : _envs) {
        if (env.python_version.full_version == runtime_version_view) {
            *version = env.python_version;
            return Status::OK();
        }
    }
    return Status::NotFound("not found runtime version: {}", runtime_version);
}

Status CondaEnvScanner::scan() {
    RETURN_IF_ERROR(PythonEnvironment::scan_from_conda_root_path(_env_root_path, &_envs));
    return Status::OK();
}

std::string CondaEnvScanner::to_string() const {
    std::stringstream ss;
    ss << "Conda environments: ";
    for (const auto& conda_env : _envs) {
        ss << conda_env.to_string() << ", ";
    }
    return ss.str();
}

Status VenvEnvScanner::scan() {
    RETURN_IF_ERROR(PythonEnvironment::scan_from_venv_root_path(_env_root_path, _interpreter_paths,
                                                                &_envs));
    return Status::OK();
}

std::string VenvEnvScanner::to_string() const {
    std::stringstream ss;
    ss << "Venv environments: ";
    for (const auto& venv_env : _envs) {
        ss << venv_env.to_string() << ", ";
    }
    return ss.str();
}

Status PythonVersionManager::init(PythonEnvType env_type, const fs::path& python_root_path,
                                  const std::string& python_venv_interpreter_paths) {
    switch (env_type) {
    case PythonEnvType::CONDA: {
        if (!fs::exists(python_root_path) || !fs::is_directory(python_root_path)) {
            return Status::InvalidArgument("Invalid conda root path: {}",
                                           python_root_path.string());
        }
        _env_scanner = std::make_unique<CondaEnvScanner>(python_root_path);
        break;
    }
    case PythonEnvType::VENV: {
        if (!fs::exists(python_root_path) || !fs::is_directory(python_root_path)) {
            return Status::InvalidArgument("Invalid venv root path: {}", python_root_path.string());
        }
        std::vector<std::string> interpreter_paths = split(python_venv_interpreter_paths, ":");
        if (interpreter_paths.empty()) {
            return Status::InvalidArgument("Invalid python interpreter paths: {}",
                                           python_venv_interpreter_paths);
        }
        _env_scanner = std::make_unique<VenvEnvScanner>(python_root_path, interpreter_paths);
        break;
    }
    default:
        return Status::NotSupported("Unsupported python runtime type: {}",
                                    static_cast<int>(env_type));
    }
    std::vector<PythonVersion> versions;
    RETURN_IF_ERROR(_env_scanner->scan());
    RETURN_IF_ERROR(_env_scanner->get_versions(&versions));
    return Status::OK();
}

std::vector<TPythonEnvInfo> PythonVersionManager::env_infos_to_thrift() const {
    std::vector<TPythonEnvInfo> infos;
    const auto& envs = _env_scanner->get_envs();
    infos.reserve(envs.size());

    const auto env_type_str = _python_env_type_to_string(_env_scanner->env_type());
    for (const auto& env : envs) {
        TPythonEnvInfo info;
        info.__set_env_name(env.env_name);
        info.__set_full_version(env.python_version.full_version);
        info.__set_env_type(env_type_str);
        info.__set_base_path(env.python_version.base_path);
        info.__set_executable_path(env.python_version.executable_path);
        infos.emplace_back(std::move(info));
    }

    return infos;
}

std::vector<TPythonPackageInfo> PythonVersionManager::package_infos_to_thrift(
        const std::vector<std::pair<std::string, std::string>>& packages) const {
    std::vector<TPythonPackageInfo> infos;
    infos.reserve(packages.size());
    for (const auto& [name, ver] : packages) {
        TPythonPackageInfo info;
        info.__set_package_name(name);
        info.__set_version(ver);
        infos.emplace_back(std::move(info));
    }
    return infos;
}

Status list_installed_packages(const PythonVersion& version,
                               std::vector<std::pair<std::string, std::string>>* packages) {
    DCHECK(packages != nullptr);
    if (!version.is_valid()) {
        return Status::InvalidArgument("Invalid python version: {}", version.to_string());
    }

    // Run pip list --format=json to get installed packages
    std::string cmd =
            fmt::format("\"{}\" -m pip list --format=json 2>/dev/null", version.executable_path);
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        return Status::InternalError("Failed to run pip list for python version: {}",
                                     version.full_version);
    }

    std::string result;
    char buf[4096];
    while (fgets(buf, sizeof(buf), pipe)) {
        result += buf;
    }
    int ret = pclose(pipe);
    if (ret != 0) {
        return Status::InternalError(
                "pip list failed for python version: {}, exit code: {}, output: {}",
                version.full_version, ret, result);
    }

    // Parse JSON output: [{"name": "pkg", "version": "1.0"}, ...]
    // Simple JSON parsing without external library
    // Each entry looks like: {"name": "package_name", "version": "1.2.3"}
    rapidjson::Document doc;
    if (doc.Parse(result.data(), result.size()).HasParseError() || !doc.IsArray()) [[unlikely]] {
        return Status::InternalError("Failed to parse pip list json output for python version: {}",
                                     version.full_version);
    }

    packages->reserve(packages->size() + doc.Size());
    for (const auto& item : doc.GetArray()) {
        auto name_it = item.FindMember("name");
        auto version_it = item.FindMember("version");
        if (name_it == item.MemberEnd() || version_it == item.MemberEnd() ||
            !name_it->value.IsString() || !version_it->value.IsString()) [[unlikely]] {
            return Status::InternalError("Invalid pip list json format for python version: {}",
                                         version.full_version);
        }
        packages->emplace_back(name_it->value.GetString(), version_it->value.GetString());
    }

    return Status::OK();
}

} // namespace doris
