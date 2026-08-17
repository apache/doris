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

#include "udf/python/python_env.h"

#include <gtest/gtest.h>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace doris {

namespace fs = std::filesystem;

static PythonVersion create_fake_python_version_for_pip_list(const std::string& base_path,
                                                             const std::string& script_body,
                                                             const std::string& full_version) {
    const std::string bin_path = base_path + "/bin";
    const std::string exec_path = bin_path + "/python3";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(exec_path);
        ofs << "#!/bin/bash\n";
        ofs << script_body;
    }
    fs::permissions(exec_path, fs::perms::owner_all);

    return PythonVersion(full_version, base_path, exec_path);
}

class PythonEnvTest : public ::testing::Test {
protected:
    std::string test_dir_;
    // Some test frameworks set SIGCHLD to SIG_IGN,
    // which causes pclose() to get ECHILD because the kernel auto-reaps children.
    // We reset SIGCHLD to SIG_DFL for the duration of each test to mimic production
    // behaviour, and restore the original handler afterwards.
    sighandler_t old_sigchld_ = SIG_DFL;

    void SetUp() override {
        test_dir_ = fs::temp_directory_path().string() + "/python_env_test_" +
                    std::to_string(getpid()) + "_" + std::to_string(rand());
        fs::create_directories(test_dir_);
        old_sigchld_ = signal(SIGCHLD, SIG_DFL);
    }

    void TearDown() override {
        signal(SIGCHLD, old_sigchld_);
        if (!test_dir_.empty() && fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }
};

// ============================================================================
// PythonVersion tests
// ============================================================================

TEST_F(PythonEnvTest, PythonVersionDefaultConstruction) {
    PythonVersion pv;
    EXPECT_TRUE(pv.full_version.empty());
    EXPECT_TRUE(pv.base_path.empty());
    EXPECT_TRUE(pv.executable_path.empty());
}

TEST_F(PythonEnvTest, PythonVersionExplicitConstruction) {
    PythonVersion pv("3.9.16", "/opt/python", "/opt/python/bin/python3");
    EXPECT_EQ(pv.full_version, "3.9.16");
    EXPECT_EQ(pv.base_path, "/opt/python");
    EXPECT_EQ(pv.executable_path, "/opt/python/bin/python3");
}

TEST_F(PythonEnvTest, PythonVersionEquality) {
    PythonVersion pv1("3.9.16", "/opt/python", "/opt/python/bin/python3");
    PythonVersion pv2("3.9.16", "/opt/python", "/opt/python/bin/python3");
    PythonVersion pv3("3.10.0", "/opt/python", "/opt/python/bin/python3");
    PythonVersion pv4("3.9.16", "/different/path", "/opt/python/bin/python3");

    EXPECT_EQ(pv1, pv2);
    EXPECT_FALSE(pv1 == pv3);
    EXPECT_FALSE(pv1 == pv4);
}

TEST_F(PythonEnvTest, PythonVersionGetters) {
    PythonVersion pv("3.9.16", "/opt/python", "/opt/python/bin/python3");
    EXPECT_EQ(pv.get_base_path(), "/opt/python");
    EXPECT_EQ(pv.get_executable_path(), "/opt/python/bin/python3");
}

TEST_F(PythonEnvTest, PythonVersionIsValidEmpty) {
    PythonVersion pv;
    EXPECT_FALSE(pv.is_valid());
}

TEST_F(PythonEnvTest, PythonVersionIsValidPartiallyFilled) {
    PythonVersion pv1("3.9.16", "", "");
    EXPECT_FALSE(pv1.is_valid());

    PythonVersion pv2("", "/opt/python", "");
    EXPECT_FALSE(pv2.is_valid());
}

TEST_F(PythonEnvTest, PythonVersionIsValidNonExistentPaths) {
    PythonVersion pv("3.9.16", "/non/existent/path", "/non/existent/python");
    EXPECT_FALSE(pv.is_valid());
}

TEST_F(PythonEnvTest, PythonVersionIsValidWithExistingPaths) {
    // Create fake directories and executable
    std::string base_path = test_dir_ + "/python_base";
    std::string exec_path = test_dir_ + "/python_base/bin/python3";
    fs::create_directories(test_dir_ + "/python_base/bin");

    // Create a fake executable file
    std::ofstream ofs(exec_path);
    ofs << "#!/bin/bash\n";
    ofs.close();
    fs::permissions(exec_path, fs::perms::owner_all);

    PythonVersion pv("3.9.16", base_path, exec_path);
    // is_valid() also checks that extract_python_version works, which requires a real python
    // So this will fail on fake executable, but we verify paths exist
    EXPECT_TRUE(fs::exists(base_path));
    EXPECT_TRUE(fs::exists(exec_path));
}

TEST_F(PythonEnvTest, PythonVersionToString) {
    PythonVersion pv("3.9.16", "/opt/python", "/opt/python/bin/python3");
    std::string str = pv.to_string();
    EXPECT_TRUE(str.find("3.9.16") != std::string::npos);
    EXPECT_TRUE(str.find("/opt/python") != std::string::npos);
    EXPECT_TRUE(str.find("/opt/python/bin/python3") != std::string::npos);
}

TEST_F(PythonEnvTest, PythonVersionHash) {
    PythonVersion pv1("3.9.16", "/opt/python", "/opt/python/bin/python3");
    PythonVersion pv2("3.9.16", "/opt/python", "/opt/python/bin/python3");
    PythonVersion pv3("3.10.0", "/opt/python", "/opt/python/bin/python3");

    std::hash<PythonVersion> hasher;
    EXPECT_EQ(hasher(pv1), hasher(pv2));
    // Different versions should (very likely) have different hashes
    EXPECT_NE(hasher(pv1), hasher(pv3));
}

// ============================================================================
// PythonEnvironment tests
// ============================================================================

TEST_F(PythonEnvTest, PythonEnvironmentConstruction) {
    PythonVersion pv("3.9.16", "/opt/python", "/opt/python/bin/python3");
    PythonEnvironment env("test_env", pv);

    EXPECT_EQ(env.env_name, "test_env");
    EXPECT_EQ(env.python_version.full_version, "3.9.16");
}

TEST_F(PythonEnvTest, PythonEnvironmentToString) {
    PythonVersion pv("3.9.16", "/opt/python", "/opt/python/bin/python3");
    PythonEnvironment env("test_env", pv);

    std::string str = env.to_string();
    EXPECT_TRUE(str.find("test_env") != std::string::npos);
    EXPECT_TRUE(str.find("3.9.16") != std::string::npos);
}

TEST_F(PythonEnvTest, PythonEnvironmentIsValidWithInvalidVersion) {
    PythonVersion pv;
    PythonEnvironment env("test_env", pv);
    EXPECT_FALSE(env.is_valid());
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathNonExistentDir) {
    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path("/non/existent/path", &envs);
    EXPECT_FALSE(status.ok());
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathNoEnvsSubdir) {
    // Create root path without envs subdirectory
    std::string root_path = test_dir_ + "/conda_root";
    fs::create_directories(root_path);

    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path(root_path, &envs);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Conda envs directory not found") != std::string::npos);
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathEmptyEnvsDir) {
    // Create root path with empty envs subdirectory
    std::string root_path = test_dir_ + "/conda_root2";
    fs::create_directories(root_path + "/envs");

    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path(root_path, &envs);
    // Should fail because no valid environments found
    EXPECT_FALSE(status.ok());
}

TEST_F(PythonEnvTest, ScanFromVenvRootPathNonExistentInterpreter) {
    std::string root_path = test_dir_ + "/venv_root";
    fs::create_directories(root_path);

    std::vector<std::string> interpreter_paths = {"/non/existent/python"};
    std::vector<PythonEnvironment> envs;

    Status status =
            PythonEnvironment::scan_from_venv_root_path(root_path, interpreter_paths, &envs);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Interpreter path not found") != std::string::npos);
}

// ============================================================================
// PythonEnvScanner tests
// ============================================================================

TEST_F(PythonEnvTest, PythonEnvScannerGetVersionsEmpty) {
    CondaEnvScanner scanner(test_dir_);
    std::vector<PythonVersion> versions;
    Status status = scanner.get_versions(&versions);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("not found available version") != std::string::npos);
}

TEST_F(PythonEnvTest, PythonEnvScannerGetVersionNotFound) {
    CondaEnvScanner scanner(test_dir_);
    PythonVersion version;
    Status status = scanner.get_version("3.9.16", &version);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("not found available version") != std::string::npos);
}

TEST_F(PythonEnvTest, CondaEnvScannerProperties) {
    CondaEnvScanner scanner(test_dir_);
    EXPECT_EQ(scanner.env_type(), PythonEnvType::CONDA);
    EXPECT_EQ(scanner.root_path(), test_dir_);
}

TEST_F(PythonEnvTest, CondaEnvScannerToString) {
    CondaEnvScanner scanner(test_dir_);
    std::string str = scanner.to_string();
    EXPECT_TRUE(str.find("Conda environments") != std::string::npos);
}

TEST_F(PythonEnvTest, CondaEnvScannerScanNonExistent) {
    CondaEnvScanner scanner("/non/existent/path");
    Status status = scanner.scan();
    EXPECT_FALSE(status.ok());
}

TEST_F(PythonEnvTest, VenvEnvScannerProperties) {
    std::vector<std::string> paths = {"/usr/bin/python3"};
    VenvEnvScanner scanner(test_dir_, paths);
    EXPECT_EQ(scanner.env_type(), PythonEnvType::VENV);
    EXPECT_EQ(scanner.root_path(), test_dir_);
}

TEST_F(PythonEnvTest, VenvEnvScannerToString) {
    std::vector<std::string> paths = {"/usr/bin/python3"};
    VenvEnvScanner scanner(test_dir_, paths);
    std::string str = scanner.to_string();
    EXPECT_TRUE(str.find("Venv environments") != std::string::npos);
}

TEST_F(PythonEnvTest, VenvEnvScannerScanNonExistentInterpreter) {
    std::vector<std::string> paths = {"/non/existent/python3"};
    VenvEnvScanner scanner(test_dir_, paths);
    Status status = scanner.scan();
    EXPECT_FALSE(status.ok());
}

// ============================================================================
// PythonVersionManager tests
// ============================================================================

TEST_F(PythonEnvTest, PythonVersionManagerInitCondaInvalidPath) {
    PythonVersionManager mgr;
    Status status = mgr.init(PythonEnvType::CONDA, "/non/existent/conda/path", "");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Invalid conda root path") != std::string::npos);
}

TEST_F(PythonEnvTest, PythonVersionManagerInitVenvInvalidPath) {
    PythonVersionManager mgr;
    Status status = mgr.init(PythonEnvType::VENV, "/non/existent/venv/path", "/usr/bin/python3");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Invalid venv root path") != std::string::npos);
}

TEST_F(PythonEnvTest, PythonVersionManagerInitVenvEmptyInterpreters) {
    fs::create_directories(test_dir_ + "/venv_root");
    PythonVersionManager mgr;
    Status status = mgr.init(PythonEnvType::VENV, test_dir_ + "/venv_root", "");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Interpreter path not found:") != std::string::npos);
}

TEST_F(PythonEnvTest, PythonVersionManagerInitUnsupportedType) {
    PythonVersionManager mgr;
    Status status = mgr.init(static_cast<PythonEnvType>(99), test_dir_, "");
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Unsupported python runtime type") != std::string::npos);
}

// ============================================================================
// Tests covering additional paths using fake Python scripts
// ============================================================================

TEST_F(PythonEnvTest, PythonVersionIsValidWithFakePython) {
    // Create a fake Python script that prints "Python 3.9.16"
    std::string base_path = test_dir_ + "/fake_python";
    std::string bin_path = base_path + "/bin";
    std::string exec_path = bin_path + "/python3";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(exec_path);
        ofs << "#!/bin/bash\n";
        ofs << "echo 'Python 3.9.16'\n";
    }
    fs::permissions(exec_path, fs::perms::owner_all);

    PythonVersion pv("3.9.16", base_path, exec_path);
    // Now is_valid() should pass because the version matches
    EXPECT_TRUE(pv.is_valid());
}

TEST_F(PythonEnvTest, PythonVersionIsValidVersionMismatch) {
    // Create a fake Python script that prints a different version
    std::string base_path = test_dir_ + "/fake_python2";
    std::string bin_path = base_path + "/bin";
    std::string exec_path = bin_path + "/python3";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(exec_path);
        ofs << "#!/bin/bash\n";
        ofs << "echo 'Python 3.10.0'\n"; // prints 3.10.0
    }
    fs::permissions(exec_path, fs::perms::owner_all);

    PythonVersion pv("3.9.16", base_path, exec_path); // but declared version is 3.9.16
    // PythonVersion::is_valid() only checks that the path exists, not the version match
    EXPECT_TRUE(pv.is_valid());

    // PythonEnvironment::is_valid() checks the version match
    PythonEnvironment env("test_env", pv);
    EXPECT_FALSE(env.is_valid()); // should return false for version mismatch
}

TEST_F(PythonEnvTest, PythonVersionIsValidNoExecutePermission) {
    // Create the file without execute permission
    std::string base_path = test_dir_ + "/no_exec";
    std::string bin_path = base_path + "/bin";
    std::string exec_path = bin_path + "/python3";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(exec_path);
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    // Give only read/write permission, no execute permission
    fs::permissions(exec_path, fs::perms::owner_read | fs::perms::owner_write);

    PythonVersion pv("3.9.16", base_path, exec_path);
    // PythonVersion::is_valid() only checks that the path exists
    EXPECT_TRUE(pv.is_valid());

    // PythonEnvironment::is_valid() checks execute permission
    PythonEnvironment env("test_env", pv);
    EXPECT_FALSE(env.is_valid()); // should return false when execute permission is missing
}

TEST_F(PythonEnvTest, PythonVersionIsValidInvalidVersionOutput) {
    // Python outputs an invalid format
    std::string base_path = test_dir_ + "/invalid_output";
    std::string bin_path = base_path + "/bin";
    std::string exec_path = bin_path + "/python3";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(exec_path);
        ofs << "#!/bin/bash\n";
        ofs << "echo 'Invalid Output'\n"; // does not match the "Python X.Y.Z" format
    }
    fs::permissions(exec_path, fs::perms::owner_all);

    PythonVersion pv("3.9.16", base_path, exec_path);
    // PythonVersion::is_valid() only checks that the path exists
    EXPECT_TRUE(pv.is_valid());

    // PythonEnvironment::is_valid() checks the version output format
    PythonEnvironment env("test_env", pv);
    EXPECT_FALSE(env.is_valid()); // should return false for invalid output
}

TEST_F(PythonEnvTest, PythonEnvironmentIsValidWithFakePython) {
    // Create a valid fake Python environment
    std::string base_path = test_dir_ + "/fake_env";
    std::string bin_path = base_path + "/bin";
    std::string exec_path = bin_path + "/python3";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(exec_path);
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(exec_path, fs::perms::owner_all);

    PythonVersion pv("3.9.16", base_path, exec_path);
    PythonEnvironment env("test_env", pv);
    EXPECT_TRUE(env.is_valid());
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathWithFakePython) {
    // Create a fake Conda environment structure
    std::string conda_root = test_dir_ + "/conda";
    std::string env_path = conda_root + "/envs/myenv";
    std::string bin_path = env_path + "/bin";
    std::string python_path = bin_path + "/python";
    fs::create_directories(bin_path);

    {
        std::ofstream ofs(python_path);
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(python_path, fs::perms::owner_all);

    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path(conda_root, &envs);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(envs.size(), 1);
    EXPECT_EQ(envs[0].env_name, "myenv");
    EXPECT_EQ(envs[0].python_version.full_version, "3.9.16");
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathMultipleEnvs) {
    // Create multiple fake Conda environments
    std::string conda_root = test_dir_ + "/conda_multi";

    // Environment 1: Python 3.9.16
    std::string env1_path = conda_root + "/envs/py39";
    fs::create_directories(env1_path + "/bin");
    {
        std::ofstream ofs(env1_path + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(env1_path + "/bin/python", fs::perms::owner_all);

    // Environment 2: Python 3.10.0
    std::string env2_path = conda_root + "/envs/py310";
    fs::create_directories(env2_path + "/bin");
    {
        std::ofstream ofs(env2_path + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.10.0'\n";
    }
    fs::permissions(env2_path + "/bin/python", fs::perms::owner_all);

    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path(conda_root, &envs);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(envs.size(), 2);
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathSkipsInvalidEnvs) {
    // Create a valid and an invalid environment
    std::string conda_root = test_dir_ + "/conda_mixed";

    // Valid environment
    std::string valid_env = conda_root + "/envs/valid";
    fs::create_directories(valid_env + "/bin");
    {
        std::ofstream ofs(valid_env + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(valid_env + "/bin/python", fs::perms::owner_all);

    // Invalid environment (no python file)
    std::string invalid_env = conda_root + "/envs/invalid";
    fs::create_directories(invalid_env + "/bin");
    // Do not create the python file

    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path(conda_root, &envs);
    // 应该失败因为遇到无效环境
    EXPECT_FALSE(status.ok());
}

TEST_F(PythonEnvTest, ScanFromCondaRootPathSkipsFiles) {
    // If envs contains files instead of directories, they should be skipped
    std::string conda_root = test_dir_ + "/conda_files";
    fs::create_directories(conda_root + "/envs");

    // Create a file (not a directory)
    {
        std::ofstream ofs(conda_root + "/envs/somefile.txt");
        ofs << "not a directory";
    }

    // Create a valid environment directory
    std::string valid_env = conda_root + "/envs/valid";
    fs::create_directories(valid_env + "/bin");
    {
        std::ofstream ofs(valid_env + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(valid_env + "/bin/python", fs::perms::owner_all);

    std::vector<PythonEnvironment> envs;
    Status status = PythonEnvironment::scan_from_conda_root_path(conda_root, &envs);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(envs.size(), 1);
}

TEST_F(PythonEnvTest, PythonEnvScannerGetVersionWithEnvs) {
    // First create a valid Conda environment
    std::string conda_root = test_dir_ + "/conda_get_version";
    std::string env_path = conda_root + "/envs/myenv";
    fs::create_directories(env_path + "/bin");
    {
        std::ofstream ofs(env_path + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(env_path + "/bin/python", fs::perms::owner_all);

    CondaEnvScanner scanner(conda_root);
    Status scan_status = scanner.scan();
    EXPECT_TRUE(scan_status.ok()) << scan_status.to_string();

    // Test get_version
    PythonVersion version;
    Status status = scanner.get_version("3.9.16", &version);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(version.full_version, "3.9.16");

    // Test finding a non-existent version
    PythonVersion not_found;
    Status not_found_status = scanner.get_version("3.99.99", &not_found);
    EXPECT_FALSE(not_found_status.ok());
    EXPECT_TRUE(not_found_status.to_string().find("not found runtime version") !=
                std::string::npos);
}

TEST_F(PythonEnvTest, PythonEnvScannerGetVersionsWithEnvs) {
    // Create multiple environments
    std::string conda_root = test_dir_ + "/conda_versions";

    std::string env1 = conda_root + "/envs/py39";
    fs::create_directories(env1 + "/bin");
    {
        std::ofstream ofs(env1 + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(env1 + "/bin/python", fs::perms::owner_all);

    std::string env2 = conda_root + "/envs/py310";
    fs::create_directories(env2 + "/bin");
    {
        std::ofstream ofs(env2 + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.10.0'\n";
    }
    fs::permissions(env2 + "/bin/python", fs::perms::owner_all);

    CondaEnvScanner scanner(conda_root);
    EXPECT_TRUE(scanner.scan().ok());

    std::vector<PythonVersion> versions;
    Status status = scanner.get_versions(&versions);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(versions.size(), 2);
}

TEST_F(PythonEnvTest, PythonEnvScannerGetVersionWithWhitespace) {
    // Test version string with surrounding whitespace
    std::string conda_root = test_dir_ + "/conda_whitespace";
    std::string env_path = conda_root + "/envs/myenv";
    fs::create_directories(env_path + "/bin");
    {
        std::ofstream ofs(env_path + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(env_path + "/bin/python", fs::perms::owner_all);

    CondaEnvScanner scanner(conda_root);
    EXPECT_TRUE(scanner.scan().ok());

    PythonVersion version;
    // 版本字符串带前后空格
    Status status = scanner.get_version("  3.9.16  ", &version);
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(version.full_version, "3.9.16");
}

TEST_F(PythonEnvTest, PythonVersionManagerInitCondaSuccess) {
    // Create a valid Conda environment
    std::string conda_root = test_dir_ + "/conda_mgr";
    std::string env_path = conda_root + "/envs/myenv";
    fs::create_directories(env_path + "/bin");
    {
        std::ofstream ofs(env_path + "/bin/python");
        ofs << "#!/bin/bash\necho 'Python 3.9.16'\n";
    }
    fs::permissions(env_path + "/bin/python", fs::perms::owner_all);

    PythonVersionManager mgr;
    Status status = mgr.init(PythonEnvType::CONDA, conda_root, "");
    EXPECT_TRUE(status.ok()) << status.to_string();
}

// ============================================================================
// list_installed_packages tests
// ============================================================================

TEST_F(PythonEnvTest, ListInstalledPackagesInvalidPythonVersion) {
    PythonVersion invalid_version;
    std::vector<std::pair<std::string, std::string>> packages;

    Status status = list_installed_packages(invalid_version, &packages);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Invalid python version") != std::string::npos);
}

TEST_F(PythonEnvTest, ListInstalledPackagesPipListExitNonZero) {
    PythonVersion version = create_fake_python_version_for_pip_list(
            test_dir_ + "/pip_nonzero",
            "echo '[{\"name\":\"numpy\",\"version\":\"1.26.0\"}]'\n"
            "exit 1\n",
            "3.9.16");
    std::vector<std::pair<std::string, std::string>> packages;

    Status status = list_installed_packages(version, &packages);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("pip list failed") != std::string::npos);
}

TEST_F(PythonEnvTest, ListInstalledPackagesParseError) {
    PythonVersion version = create_fake_python_version_for_pip_list(
            test_dir_ + "/pip_parse_error", "echo 'not-json'\nexit 0\n", "3.9.16");
    std::vector<std::pair<std::string, std::string>> packages;

    Status status = list_installed_packages(version, &packages);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to parse pip list json output") !=
                std::string::npos);
}

TEST_F(PythonEnvTest, ListInstalledPackagesJsonIsNotArray) {
    PythonVersion version = create_fake_python_version_for_pip_list(
            test_dir_ + "/pip_not_array", "echo '{\"name\":\"numpy\"}'\nexit 0\n", "3.9.16");
    std::vector<std::pair<std::string, std::string>> packages;

    Status status = list_installed_packages(version, &packages);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to parse pip list json output") !=
                std::string::npos);
}

TEST_F(PythonEnvTest, ListInstalledPackagesInvalidJsonItemFormat) {
    PythonVersion version = create_fake_python_version_for_pip_list(
            test_dir_ + "/pip_invalid_item", "echo '[{\"name\":\"numpy\"}]'\nexit 0\n", "3.9.16");
    std::vector<std::pair<std::string, std::string>> packages;

    Status status = list_installed_packages(version, &packages);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Invalid pip list json format") != std::string::npos);
}

TEST_F(PythonEnvTest, ListInstalledPackagesSuccess) {
    PythonVersion version = create_fake_python_version_for_pip_list(
            test_dir_ + "/pip_success",
            "echo "
            "'[{\"name\":\"numpy\",\"version\":\"1.26.0\"},{\"name\":\"pandas\",\"version\":\"2.2."
            "0\"}]'\n"
            "exit 0\n",
            "3.9.16");
    std::vector<std::pair<std::string, std::string>> packages;

    Status status = list_installed_packages(version, &packages);
    EXPECT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(packages.size(), 2);
    EXPECT_EQ(packages[0].first, "numpy");
    EXPECT_EQ(packages[0].second, "1.26.0");
    EXPECT_EQ(packages[1].first, "pandas");
    EXPECT_EQ(packages[1].second, "2.2.0");
}

} // namespace doris
