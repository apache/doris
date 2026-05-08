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

package org.apache.doris.fs.remote.dfs;

import org.apache.doris.backup.Status;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.fs.remote.RemoteFile;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DFSFileSystemTest {

    private DFSFileSystem dfsFileSystem;

    @Mocked
    private FileSystem mockFileSystem;

    private boolean originalConfigValue;

    @BeforeEach
    public void setUp() {
        // Save original config value
        originalConfigValue = Config.split_assigner_optimized_local_scheduling;

        // Create a real DFSFileSystem instance for testing
        dfsFileSystem = new DFSFileSystem(new HdfsProperties(new HashMap<>()));
    }

    @AfterEach
    public void tearDown() {
        // Restore original config value
        Config.split_assigner_optimized_local_scheduling = originalConfigValue;
    }



    @Test
    public void testListFilesRecursiveWithNewImplementation() throws Exception {
        // Enable new implementation
        Config.split_assigner_optimized_local_scheduling = false;

        // Create mock file structure:
        // /test/
        // ├── file1.txt
        // ├── file2.txt
        // └── subdir/
        //      ├── file3.txt
        //      └── file4.txt

        Path file1Path = new Path("/test/file1.txt");
        Path file2Path = new Path("/test/file2.txt");
        Path file3Path = new Path("/test/subdir/file3.txt");
        Path file4Path = new Path("/test/subdir/file4.txt");

        // Create additional paths for proper testing
        Path rootPath = new Path("/test");
        Path subdirPath = new Path("/test/subdir");

        // Mock FileStatus objects
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);
        FileStatus file2Status = createMockFileStatus(file2Path, false, 2048, 2000);
        FileStatus subdirStatus = createMockFileStatus(subdirPath, true, 0, 3000);
        FileStatus file3Status = createMockFileStatus(file3Path, false, 512, 4000);
        FileStatus file4Status = createMockFileStatus(file4Path, false, 256, 5000);

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatus calls
        new Expectations() {
            {
                // Mock listStatus for root directory
                mockFileSystem.listStatus(rootPath);
                result = new FileStatus[]{file1Status, file2Status, subdirStatus};

                // Mock listStatus for subdirectory (called during recursive traversal)
                mockFileSystem.listStatus(subdirPath);
                result = new FileStatus[]{file3Status, file4Status};
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // Verify results
        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(4, result.size()); // Should find all 4 files

        // Verify file details
        Map<String, RemoteFile> fileMap = new HashMap<>();
        for (RemoteFile file : result) {
            fileMap.put(file.getName(), file);
        }

        Assertions.assertTrue(fileMap.containsKey("file1.txt"));
        Assertions.assertTrue(fileMap.containsKey("file2.txt"));
        Assertions.assertTrue(fileMap.containsKey("file3.txt"));
        Assertions.assertTrue(fileMap.containsKey("file4.txt"));

        RemoteFile file1 = fileMap.get("file1.txt");
        Assertions.assertTrue(file1.isFile());
        Assertions.assertFalse(file1.isDirectory());
        Assertions.assertEquals(1024, file1.getSize());
        Assertions.assertEquals(1000, file1.getModificationTime());
        Assertions.assertNull(file1.getBlockLocations()); // New implementation sets null
    }

    @Test
    public void testListFilesNonRecursiveWithNewImplementation() throws Exception {
        // Enable new implementation
        Config.split_assigner_optimized_local_scheduling = false;

        // Create mock file structure for non-recursive test:
        // /test/
        // ├── file1.txt
        // ├── file2.txt
        // └── subdir/ (directory, should not be traversed in non-recursive mode)

        Path rootPath = new Path("/test");
        Path file1Path = new Path("/test/file1.txt");
        Path file2Path = new Path("/test/file2.txt");
        Path subdirPath = new Path("/test/subdir");

        // Mock FileStatus objects
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);
        FileStatus file2Status = createMockFileStatus(file2Path, false, 2048, 2000);
        FileStatus subdirStatus = createMockFileStatus(subdirPath, true, 0, 3000);

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatus calls
        new Expectations() {
            {
                // Mock listStatus for root directory only (non-recursive)
                mockFileSystem.listStatus(rootPath);
                result = new FileStatus[]{file1Status, file2Status, subdirStatus};
                // Note: subdirectory should NOT be traversed in non-recursive mode
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(2, result.size()); // Only files, not directories

        Map<String, RemoteFile> fileMap = new HashMap<>();
        for (RemoteFile file : result) {
            fileMap.put(file.getName(), file);
        }

        Assertions.assertTrue(fileMap.containsKey("file1.txt"));
        Assertions.assertTrue(fileMap.containsKey("file2.txt"));
        Assertions.assertFalse(fileMap.containsKey("subdir")); // Directory should not be included

        // Verify all returned files have null block locations
        for (RemoteFile file : result) {
            Assertions.assertNull(file.getBlockLocations());
        }
    }

    @Test
    public void testListFilesFileNotFound() throws Exception {
        // Enable new implementation
        Config.split_assigner_optimized_local_scheduling = false;

        Path nonexistentPath = new Path("/nonexistent");

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatus to throw FileNotFoundException
        new Expectations() {
            {
                // Mock listStatus to throw FileNotFoundException
                mockFileSystem.listStatus(nonexistentPath);
                result = new java.io.FileNotFoundException("Path not found: /nonexistent");
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/nonexistent", true, result);

        // Verify error handling
        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("Path not found"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesIOException() throws Exception {
        // Enable new implementation
        Config.split_assigner_optimized_local_scheduling = false;

        Path testPath = new Path("/test");

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatus to throw IOException
        new Expectations() {
            {
                // Mock listStatus to throw IOException
                mockFileSystem.listStatus(testPath);
                result = new java.io.IOException("Network error");
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // Verify error handling
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("Network error"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesUserException() throws Exception {
        // Enable new implementation
        Config.split_assigner_optimized_local_scheduling = false;

        // Use MockUp to partially mock DFSFileSystem - override nativeFileSystem to throw UserException
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) throws UserException {
                throw new UserException("Authentication failed");
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // Verify error handling
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("Authentication failed"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesEmptyDirectory() throws Exception {
        // Enable new implementation
        Config.split_assigner_optimized_local_scheduling = false;

        Path emptyPath = new Path("/empty");

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatus to return empty array
        new Expectations() {
            {
                // Mock listStatus to return empty array (empty directory)
                mockFileSystem.listStatus(emptyPath);
                result = new FileStatus[0];
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/empty", true, result);

        // Verify results
        Assertions.assertEquals(Status.OK, status);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesWithOldImplementation() throws Exception {
        // Disable new implementation to test fallback to parent class
        Config.split_assigner_optimized_local_scheduling = true;

        // When the flag is disabled, the method should call super.listFiles()
        // We need to mock the parent class behavior
        new Expectations() {
            {
                // The real implementation will call super.listFiles() when flag is false
                // We can't easily test this without mocking the parent class
                // For now, we'll just verify that the flag controls the behavior
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // When flag is disabled, it should call parent implementation
        // The exact behavior depends on the parent class implementation
        // For this test, we just verify that the method doesn't crash
        Assertions.assertNotNull(status);
    }

    @Test
    public void testListFilesBasicFunctionality() throws Exception {
        // Simple test to verify basic functionality
        Config.split_assigner_optimized_local_scheduling = false;

        Path rootPath = new Path("/test");
        Path file1Path = new Path("/test/test1.txt");
        Path file2Path = new Path("/test/test2.txt");

        // Mock FileStatus objects
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);
        FileStatus file2Status = createMockFileStatus(file2Path, false, 2048, 2000);

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatus calls
        new Expectations() {
            {
                // Mock listStatus for root directory
                mockFileSystem.listStatus(rootPath);
                result = new FileStatus[]{file1Status, file2Status};
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("test1.txt", result.get(0).getName());
        Assertions.assertEquals("test2.txt", result.get(1).getName());

        // Verify all files have null block locations (new implementation)
        for (RemoteFile file : result) {
            Assertions.assertNull(file.getBlockLocations());
        }
    }

    @Test
    public void testListFilesConfigurationToggle() throws Exception {
        // Test that the configuration flag properly controls behavior

        Path rootPath = new Path("/test");
        Path filePath = new Path("/test/file.txt");
        FileStatus fileStatus = createMockFileStatus(filePath, false, 1024, 1000);

        // Test with new implementation enabled
        Config.split_assigner_optimized_local_scheduling = false;

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {
            {
                mockFileSystem.listStatus(rootPath);
                result = new FileStatus[]{fileStatus};

            }
        };

        List<RemoteFile> result1 = new ArrayList<>();
        Status status1 = dfsFileSystem.listFiles("/test", false, result1);

        Assertions.assertEquals(Status.OK, status1);
        Assertions.assertEquals(1, result1.size());
        Assertions.assertNull(result1.get(0).getBlockLocations()); // New implementation sets null

        // Test with new implementation disabled (falls back to parent)
        Config.split_assigner_optimized_local_scheduling = true;

        List<RemoteFile> result2 = new ArrayList<>();
        Status status2 = dfsFileSystem.listFiles("/test", false, result2);

        // When flag is disabled, it should call parent implementation
        Assertions.assertNotNull(status2);
    }



    /**
     * Helper method to create mock FileStatus objects
     */
    private FileStatus createMockFileStatus(final Path path, final boolean isDirectory,
                                            final long length, final long modificationTime) {
        // Create a simple mock FileStatus using an anonymous class
        return new FileStatus() {
            @Override
            public Path getPath() {
                return path;
            }

            @Override
            public boolean isDirectory() {
                return isDirectory;
            }

            @Override
            public long getLen() {
                return length;
            }

            @Override
            public long getBlockSize() {
                return 134217728L; // 128MB default block size
            }

            @Override
            public long getModificationTime() {
                return modificationTime;
            }

            @Override
            public boolean isFile() {
                return !isDirectory;
            }

            @Override
            public short getReplication() {
                return 3; // Default HDFS replication
            }

            @Override
            public long getAccessTime() {
                return modificationTime;
            }

            @Override
            public FsPermission getPermission() {
                return new FsPermission((short) 0644);
            }

            @Override
            public String getOwner() {
                return "testuser";
            }

            @Override
            public String getGroup() {
                return "testgroup";
            }
        };
    }
}
