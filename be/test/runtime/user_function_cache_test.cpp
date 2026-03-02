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

#include "runtime/user_function_cache.h"

#include <gtest/gtest.h>
#include <minizip/zip.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "common/status.h"

namespace doris {

class UserFunctionCacheTest : public ::testing::Test {
protected:
    UserFunctionCache ufc;
    std::string test_dir_;

    void SetUp() override {
        // Save original DORIS_HOME
        original_doris_home_ = getenv("DORIS_HOME");

        // Create a unique test directory
        test_dir_ = std::filesystem::temp_directory_path().string() + "/ufc_test_" +
                    std::to_string(getpid());
        std::filesystem::create_directories(test_dir_);
    }

    void TearDown() override {
        // Restore original DORIS_HOME
        if (original_doris_home_) {
            setenv("DORIS_HOME", original_doris_home_, 1);
        } else {
            unsetenv("DORIS_HOME");
        }

        // Clean up test directory
        if (!test_dir_.empty() && std::filesystem::exists(test_dir_)) {
            std::filesystem::remove_all(test_dir_);
        }
    }

    // Helper function to create a zip file with specified files
    bool create_zip_file(const std::string& zip_path,
                         const std::vector<std::pair<std::string, std::string>>& files) {
        zipFile zf = zipOpen(zip_path.c_str(), APPEND_STATUS_CREATE);
        if (zf == nullptr) {
            return false;
        }

        for (const auto& [filename, content] : files) {
            zip_fileinfo zi = {};

            if (zipOpenNewFileInZip(zf, filename.c_str(), &zi, nullptr, 0, nullptr, 0, nullptr,
                                    Z_DEFLATED, Z_DEFAULT_COMPRESSION) != ZIP_OK) {
                zipClose(zf, nullptr);
                return false;
            }

            if (!content.empty()) {
                if (zipWriteInFileInZip(zf, content.c_str(),
                                        static_cast<unsigned int>(content.size())) != ZIP_OK) {
                    zipCloseFileInZip(zf);
                    zipClose(zf, nullptr);
                    return false;
                }
            }

            zipCloseFileInZip(zf);
        }

        zipClose(zf, nullptr);
        return true;
    }

    // Helper function to create a simple text file
    void create_text_file(const std::string& path, const std::string& content) {
        std::ofstream ofs(path);
        ofs << content;
        ofs.close();
    }

private:
    const char* original_doris_home_ = nullptr;
};

TEST_F(UserFunctionCacheTest, SplitStringByChecksumTest) {
    // Test valid string format
    std::string valid_str =
            "7119053928154065546.20c8228267b6c9ce620fddb39467d3eb.postgresql-42.5.0.jar";
    auto result = ufc._split_string_by_checksum(valid_str);
    ASSERT_EQ(result.size(), 4);
    EXPECT_EQ(result[0], "7119053928154065546");
    EXPECT_EQ(result[1], "20c8228267b6c9ce620fddb39467d3eb");
    EXPECT_EQ(result[2], "postgresql-42.5.0");
    EXPECT_EQ(result[3], "jar");
}

// Test _get_real_url method
TEST_F(UserFunctionCacheTest, TestGetRealUrlWithAbsoluteUrl) {
    std::string result_url;

    // Test with absolute URL (contains ":/")
    Status status = ufc._get_real_url("http://example.com/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "http://example.com/udf.jar");

    // Test with S3 URL
    status = ufc._get_real_url("s3://bucket/path/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "s3://bucket/path/udf.jar");

    // Test with file URL
    status = ufc._get_real_url("file:///path/to/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///path/to/udf.jar");

    // Test with https URL
    status = ufc._get_real_url("https://secure.example.com/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "https://secure.example.com/udf.jar");
}

TEST_F(UserFunctionCacheTest, TestGetRealUrlWithRelativeUrl) {
    std::string result_url;

    // Set DORIS_HOME for testing
    setenv("DORIS_HOME", "/tmp/test_doris", 1);

    // Test with relative URL (no ":/" found) - should call _check_and_return_default_java_udf_url
    Status status = ufc._get_real_url("my-udf.jar", &result_url);

    // This should process successfully in non-cloud mode
    EXPECT_TRUE(status.ok() || !result_url.empty());
}

// Test _check_and_return_default_java_udf_url method
TEST_F(UserFunctionCacheTest, TestCheckAndReturnDefaultJavaUdfUrlNonCloudMode) {
    // Set DORIS_HOME environment variable for testing
    setenv("DORIS_HOME", "/tmp/doris_test", 1);

    std::string result_url;
    Status status = ufc._check_and_return_default_java_udf_url("test-udf.jar", &result_url);

    // In non-cloud mode, should return file:// URL
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(result_url.find("file://") == 0);
    EXPECT_TRUE(result_url.find("test-udf.jar") != std::string::npos);
    EXPECT_TRUE(result_url.find("/plugins/java_udf/") != std::string::npos);
}

TEST_F(UserFunctionCacheTest, TestPathConstruction) {
    // Set DORIS_HOME environment variable
    setenv("DORIS_HOME", "/test/doris", 1);

    std::string result_url;
    Status status = ufc._check_and_return_default_java_udf_url("my-udf.jar", &result_url);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///test/doris/plugins/java_udf/my-udf.jar");

    // Test with nested path
    status = ufc._check_and_return_default_java_udf_url("nested/path/udf.jar", &result_url);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(result_url, "file:///test/doris/plugins/java_udf/nested/path/udf.jar");
}

TEST_F(UserFunctionCacheTest, TestEdgeCases) {
    setenv("DORIS_HOME", "/tmp/test", 1);

    std::string result_url;

    // Test empty URL
    Status status = ufc._get_real_url("", &result_url);
    // Empty string should be treated as relative URL
    EXPECT_TRUE(status.ok());

    // Test URL with just colon (no slash after)
    status = ufc._get_real_url("invalid:url", &result_url);
    // Should be treated as relative URL since it doesn't contain ":/"
    EXPECT_TRUE(status.ok());

    // Test URL with spaces
    status = ufc._get_real_url("my udf.jar", &result_url);
    // Should be treated as relative URL
    EXPECT_TRUE(status.ok());
}

TEST_F(UserFunctionCacheTest, TestUrlDetectionLogic) {
    std::string result_url;

    // Test various URL patterns that should be detected as absolute
    std::vector<std::string> absolute_urls = {
            "http://example.com/file.jar", "https://example.com/file.jar", "s3://bucket/file.jar",
            "file:///local/file.jar", "ftp://server/file.jar"};

    for (const auto& url : absolute_urls) {
        Status status = ufc._get_real_url(url, &result_url);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(result_url, url);
    }

    // Test patterns that should be treated as relative
    std::vector<std::string> relative_urls = {"file.jar", "path/file.jar", "invalid:no-slash", ""};

    setenv("DORIS_HOME", "/tmp", 1);

    for (const auto& url : relative_urls) {
        Status status = ufc._get_real_url(url, &result_url);
        // Should process through _check_and_return_default_java_udf_url
        EXPECT_TRUE(status.ok());
        if (!url.empty()) {
            EXPECT_TRUE(result_url.find(url) != std::string::npos);
        }
    }
}

TEST_F(UserFunctionCacheTest, TestConsistentBehavior) {
    setenv("DORIS_HOME", "/consistent/test", 1);

    std::string result_url1, result_url2;

    // Multiple calls should return consistent results
    Status status1 = ufc._check_and_return_default_java_udf_url("same-udf.jar", &result_url1);
    Status status2 = ufc._check_and_return_default_java_udf_url("same-udf.jar", &result_url2);

    EXPECT_TRUE(status1.ok());
    EXPECT_TRUE(status2.ok());
    EXPECT_EQ(result_url1, result_url2);
}

// _unzip_lib
TEST_F(UserFunctionCacheTest, UnzipLibSuccessWithFiles) {
    // Create a zip file with some Python files
    std::string zip_path = test_dir_ + "/test_udf.zip";
    std::vector<std::pair<std::string, std::string>> files = {{"main.py", "print('hello world')"},
                                                              {"utils.py", "def helper(): pass"},
                                                              {"config.txt", "config=value"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));
    ASSERT_TRUE(std::filesystem::exists(zip_path));

    // Unzip the file
    Status status = ufc._unzip_lib(zip_path);
    EXPECT_TRUE(status.ok()) << status.to_string();

    // Verify extracted files
    std::string unzip_dir = test_dir_ + "/test_udf";
    EXPECT_TRUE(std::filesystem::exists(unzip_dir));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/main.py"));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/utils.py"));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/config.txt"));

    // Verify file content
    std::ifstream ifs(unzip_dir + "/main.py");
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "print('hello world')");
}

TEST_F(UserFunctionCacheTest, UnzipLibSuccessWithDirectories) {
    // Create a zip file with directories
    std::string zip_path = test_dir_ + "/test_dir.zip";
    std::vector<std::pair<std::string, std::string>> files = {
            {"subdir/", ""}, // Directory entry (ends with /)
            {"subdir/script.py", "import os"},
            {"subdir/nested/", ""},
            {"subdir/nested/deep.py", "x = 1"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._unzip_lib(zip_path);
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::string unzip_dir = test_dir_ + "/test_dir";
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/subdir"));
    EXPECT_TRUE(std::filesystem::is_directory(unzip_dir + "/subdir"));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/subdir/script.py"));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/subdir/nested"));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/subdir/nested/deep.py"));
}

TEST_F(UserFunctionCacheTest, UnzipLibSkipsMacOSXFiles) {
    // Create a zip file with __MACOSX entries (common in Mac-created zips)
    std::string zip_path = test_dir_ + "/mac_zip.zip";
    std::vector<std::pair<std::string, std::string>> files = {
            {"script.py", "print('test')"},
            {"__MACOSX/", ""},
            {"__MACOSX/._script.py", "mac metadata"},
            {"__MACOSX/nested/._file", "more metadata"},
            {"real_file.py", "real content"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._unzip_lib(zip_path);
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::string unzip_dir = test_dir_ + "/mac_zip";
    // Real files should exist
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/script.py"));
    EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/real_file.py"));
    // __MACOSX directory should NOT exist
    EXPECT_FALSE(std::filesystem::exists(unzip_dir + "/__MACOSX"));
}

TEST_F(UserFunctionCacheTest, UnzipLibFailsForNonExistentFile) {
    std::string non_existent_path = test_dir_ + "/non_existent.zip";

    Status status = ufc._unzip_lib(non_existent_path);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to open zip file") != std::string::npos);
}

TEST_F(UserFunctionCacheTest, UnzipLibFailsForInvalidZipFile) {
    // Create a file that is not a valid zip
    std::string invalid_zip_path = test_dir_ + "/invalid.zip";
    create_text_file(invalid_zip_path, "This is not a zip file content");

    Status status = ufc._unzip_lib(invalid_zip_path);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("Failed to open zip file") != std::string::npos);
}

TEST_F(UserFunctionCacheTest, UnzipLibFailsForEmptyFile) {
    // Create an empty file
    std::string empty_zip_path = test_dir_ + "/empty.zip";
    create_text_file(empty_zip_path, "");

    Status status = ufc._unzip_lib(empty_zip_path);
    EXPECT_FALSE(status.ok());
}

TEST_F(UserFunctionCacheTest, UnzipLibHandlesEmptyZip) {
    // Create an empty but valid zip file
    std::string zip_path = test_dir_ + "/empty_valid.zip";
    std::vector<std::pair<std::string, std::string>> files = {};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._unzip_lib(zip_path);
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::string unzip_dir = test_dir_ + "/empty_valid";
    EXPECT_TRUE(std::filesystem::exists(unzip_dir));
}

TEST_F(UserFunctionCacheTest, UnzipLibHandlesLargeFileContent) {
    // Create a zip with a larger file
    std::string zip_path = test_dir_ + "/large.zip";
    std::string large_content(100000, 'x'); // 100KB of 'x'
    std::vector<std::pair<std::string, std::string>> files = {{"large.py", large_content}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._unzip_lib(zip_path);
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::string unzip_dir = test_dir_ + "/large";
    std::ifstream ifs(unzip_dir + "/large.py");
    std::string content((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content.size(), 100000);
}

TEST_F(UserFunctionCacheTest, UnzipLibMultipleFilesInSameDirectory) {
    std::string zip_path = test_dir_ + "/multi.zip";
    std::vector<std::pair<std::string, std::string>> files = {{"file1.py", "content1"},
                                                              {"file2.py", "content2"},
                                                              {"file3.py", "content3"},
                                                              {"file4.py", "content4"},
                                                              {"file5.py", "content5"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._unzip_lib(zip_path);
    EXPECT_TRUE(status.ok()) << status.to_string();

    std::string unzip_dir = test_dir_ + "/multi";
    for (int i = 1; i <= 5; i++) {
        EXPECT_TRUE(std::filesystem::exists(unzip_dir + "/file" + std::to_string(i) + ".py"));
    }
}

// _check_cache_is_python_udf
TEST_F(UserFunctionCacheTest, CheckCacheIsPythonUdfSuccess) {
    // Create a zip file with Python files
    std::string zip_name = "python_udf.zip";
    std::string zip_path = test_dir_ + "/" + zip_name;
    std::vector<std::pair<std::string, std::string>> files = {{"main.py", "def udf(): pass"},
                                                              {"helper.py", "def helper(): pass"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._check_cache_is_python_udf(test_dir_, zip_name);
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST_F(UserFunctionCacheTest, CheckCacheIsPythonUdfFailsNoPythonFile) {
    // Create a zip file without Python files
    std::string zip_name = "no_python.zip";
    std::string zip_path = test_dir_ + "/" + zip_name;
    std::vector<std::pair<std::string, std::string>> files = {
            {"config.txt", "config=value"}, {"data.json", "{\"key\": \"value\"}"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._check_cache_is_python_udf(test_dir_, zip_name);
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.to_string().find("No Python file found") != std::string::npos);
}

TEST_F(UserFunctionCacheTest, CheckCacheIsPythonUdfWithNestedPythonFile) {
    // Create a zip file with Python file in nested directory
    std::string zip_name = "nested_py.zip";
    std::string zip_path = test_dir_ + "/" + zip_name;
    std::vector<std::pair<std::string, std::string>> files = {
            {"subdir/", ""}, {"subdir/module.py", "x = 1"}, {"readme.txt", "readme"}};

    ASSERT_TRUE(create_zip_file(zip_path, files));

    // Note: The current implementation only checks top-level directory
    // This test documents the current behavior
    Status status = ufc._check_cache_is_python_udf(test_dir_, zip_name);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.to_string(),
              "[INTERNAL_ERROR]No Python file found in the unzipped directory.");
}

TEST_F(UserFunctionCacheTest, CheckCacheIsPythonUdfInvalidZip) {
    // Create an invalid zip file
    std::string zip_name = "invalid_py.zip";
    std::string zip_path = test_dir_ + "/" + zip_name;
    create_text_file(zip_path, "not a zip file");

    Status status = ufc._check_cache_is_python_udf(test_dir_, zip_name);
    EXPECT_FALSE(status.ok());
}

// _make_lib_file
TEST_F(UserFunctionCacheTest, MakeLibFilePyZip) {
    // Initialize the cache with a lib dir
    ufc._lib_dir = test_dir_;

    std::string lib_file =
            ufc._make_lib_file(12345, "abc123checksum", LibType::PY_ZIP, "my_python_udf.zip");

    // Expected format: lib_dir/shard/function_id.checksum.file_name
    int expected_shard = 12345 % 128;
    std::string expected = test_dir_ + "/" + std::to_string(expected_shard) +
                           "/12345.abc123checksum.my_python_udf.zip";
    EXPECT_EQ(lib_file, expected);
}

TEST_F(UserFunctionCacheTest, MakeLibFileJar) {
    ufc._lib_dir = test_dir_;

    std::string lib_file =
            ufc._make_lib_file(99999, "md5checksum", LibType::JAR, "my_java_udf.jar");

    int expected_shard = 99999 % 128;
    std::string expected =
            test_dir_ + "/" + std::to_string(expected_shard) + "/99999.md5checksum.my_java_udf.jar";
    EXPECT_EQ(lib_file, expected);
}

TEST_F(UserFunctionCacheTest, MakeLibFileSo) {
    ufc._lib_dir = test_dir_;

    std::string lib_file = ufc._make_lib_file(100, "somechecksum", LibType::SO, "ignored.so");

    int expected_shard = 100 % 128;
    // SO files don't include the file_name, just .so extension
    std::string expected =
            test_dir_ + "/" + std::to_string(expected_shard) + "/100.somechecksum.so";
    EXPECT_EQ(lib_file, expected);
}

// _load_entry_from_lib with PY_ZIP
TEST_F(UserFunctionCacheTest, LoadEntryFromLibPyZip) {
    // First create a valid Python zip file with correct naming
    std::string sub_dir = test_dir_ + "/0";
    std::filesystem::create_directories(sub_dir);

    // File name format: function_id.checksum.filename.zip
    std::string zip_name = "12345.abc123def456abc123def456abc1.my_udf.zip";
    std::string zip_path = sub_dir + "/" + zip_name;

    std::vector<std::pair<std::string, std::string>> files = {{"main.py", "def udf(): pass"}};
    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._load_entry_from_lib(sub_dir, zip_name);
    EXPECT_TRUE(status.ok()) << status.to_string();

    // Verify entry was added to the map
    EXPECT_EQ(ufc._entry_map.count(12345), 1);
}

TEST_F(UserFunctionCacheTest, LoadEntryFromLibZipWithoutPython) {
    // Create a zip file without Python files - should fail
    std::string sub_dir = test_dir_ + "/1";
    std::filesystem::create_directories(sub_dir);

    std::string zip_name = "67890.checksum1234checksum1234che.no_python.zip";
    std::string zip_path = sub_dir + "/" + zip_name;

    std::vector<std::pair<std::string, std::string>> files = {{"data.txt", "some data"}};
    ASSERT_TRUE(create_zip_file(zip_path, files));

    Status status = ufc._load_entry_from_lib(sub_dir, zip_name);
    // Should fail because no Python file found
    EXPECT_FALSE(status.ok());
}

TEST_F(UserFunctionCacheTest, LoadEntryFromLibInvalidFileName) {
    std::string sub_dir = test_dir_ + "/2";
    std::filesystem::create_directories(sub_dir);

    // Invalid file name format (missing checksum)
    std::string invalid_name = "invalid_name.zip";
    std::string zip_path = sub_dir + "/" + invalid_name;
    create_text_file(zip_path, "dummy");

    Status status = ufc._load_entry_from_lib(sub_dir, invalid_name);
    EXPECT_FALSE(status.ok());
}

} // namespace doris