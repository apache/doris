//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#pragma once

#include <string>
#include <memory>

#include "common/status.h"
#include "util/slice.h"

namespace doris {

class RandomAccessFile;
class RandomRWFile;
class WritableFile;
class SequentialFile;
class WritableFileOptions;
class RandomAccessFileOptions;
class RandomRWFileOptions;

class Env {
public:
    // Governs if/how the file is created.
    //
    // enum value                   | file exists       | file does not exist
    // -----------------------------+-------------------+--------------------
    // CREATE_OR_OPEN_WITH_TRUNCATE | opens + truncates | creates
    // CREATE_OR_OPEN               | opens             | creates
    // MUST_CREATE                  | fails             | creates
    // MUST_EXIST                   | opens             | fails
    enum OpenMode {
        CREATE_OR_OPEN_WITH_TRUNCATE,
        CREATE_OR_OPEN,
        MUST_CREATE,
        MUST_EXIST
    };

    Env() { }
    virtual ~Env() { }

    // Return a default environment suitable for the current operating
    // system.  Sophisticated users may wish to provide their own Env
    // implementation instead of relying on this default environment.
    static Env* Default();

    // Create a brand new sequentially-readable file with the specified name.
    // On success, stores a pointer to the new file in *result and returns OK.
    // On failure stores NULL in *result and returns non-OK.  If the file does
    // not exist, returns a non-OK status.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual Status new_sequential_file(const std::string& fname,
                                       std::unique_ptr<SequentialFile>* result) = 0;

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores nullptr in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    virtual Status new_random_access_file(const std::string& fname,
                                          std::unique_ptr<RandomAccessFile>* result) = 0;

    virtual Status new_random_access_file(const RandomAccessFileOptions& opts,
                                          const std::string& fname,
                                          std::unique_ptr<RandomAccessFile>* result) = 0;

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual Status new_writable_file(const std::string& fname,
                                     std::unique_ptr<WritableFile>* result) = 0;

    // Like the previous new_writable_file, but allows options to be
    // specified.
    virtual Status new_writable_file(const WritableFileOptions& opts,
                                     const std::string& fname,
                                     std::unique_ptr<WritableFile>* result) = 0;

    // Creates a new readable and writable file. If a file with the same name
    // already exists on disk, it is deleted.
    //
    // Some of the methods of the new file may be accessed concurrently,
    // while others are only safe for access by one thread at a time.
    virtual Status new_random_rw_file(const std::string& fname,
                                      std::unique_ptr<RandomRWFile>* result) = 0;

    // Like the previous new_random_rw_file, but allows options to be specified.
    virtual Status new_random_rw_file(const RandomRWFileOptions& opts,
                                      const std::string& fname,
                                      std::unique_ptr<RandomRWFile>* result) = 0;

    // Returns OK if the path exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    virtual Status path_exists(const std::string& fname) = 0;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    virtual Status get_children(const std::string& dir,
                                std::vector<std::string>* result) = 0;

    // Iterate the specified directory and call given callback function with child's
    // name. This function continues execution until all children have been iterated
    // or callback function return false.
    // The names are relative to "dir".
    //
    // The function call extra cost is acceptable. Compared with returning all children
    // into a given vector, the performance of this method is 5% worse. However this
    // approach is more flexiable and efficient in fulfilling other requirements.
    //
    // Returns OK if "dir" exists.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    virtual Status iterate_dir(const std::string& dir,
                               const std::function<bool(const char*)>& cb) = 0;

    // Delete the named file.
    virtual Status delete_file(const std::string& fname) = 0;

    // Create the specified directory.
    // NOTE: It will return error if the path already exist(not necessarily as a directory)
    virtual Status create_dir(const std::string& dirname) = 0;

    // Creates directory if missing.
    // Return OK if it exists, or successful in Creating.
    virtual Status create_dir_if_missing(const std::string& dirname, bool* created = nullptr) = 0;

    // Delete the specified directory.
    // NOTE: The dir must be empty.
    virtual Status delete_dir(const std::string& dirname) = 0;

    // Synchronize the entry for a specific directory.
    virtual Status sync_dir(const std::string& dirname) = 0;

    // Checks if the file is a directory. Returns an error if it doesn't
    // exist, otherwise writes true or false into 'is_dir' appropriately.
    virtual Status is_directory(const std::string& path, bool* is_dir) = 0;

    // Canonicalize 'path' by applying the following conversions:
    // - Converts a relative path into an absolute one using the cwd.
    // - Converts '.' and '..' references.
    // - Resolves all symbolic links.
    //
    // All directory entries in 'path' must exist on the filesystem.
    virtual Status canonicalize(const std::string& path, std::string* result) = 0;

    virtual Status get_file_size(const std::string& fname, uint64_t* size) = 0;

    // Store the last modification time of fname in *file_mtime.
    virtual Status get_file_modified_time(const std::string& fname,
                                              uint64_t* file_mtime) = 0;
    // Rename file src to target.
    virtual Status rename_file(const std::string& src,
                               const std::string& target) = 0;

    // create a hard-link
    virtual Status link_file(const std::string& /*old_path*/,
                             const std::string& /*new_path*/) = 0;
};

struct RandomAccessFileOptions {
    RandomAccessFileOptions() { }
};

// Creation-time options for WritableFile
struct WritableFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = false;
    // See OpenMode for details.
    Env::OpenMode mode = Env::CREATE_OR_OPEN_WITH_TRUNCATE;
};

// Creation-time options for RWFile
struct RandomRWFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = false;
    // See OpenMode for details.
    Env::OpenMode mode = Env::CREATE_OR_OPEN_WITH_TRUNCATE;
};

// A file abstraction for reading sequentially through a file
class SequentialFile {
public:
    SequentialFile() { }
    virtual ~SequentialFile() { }

    // Read up to "result.size" bytes from the file.
    // Sets "result.data" to the data that was read.
    //
    // If an error was encountered, returns a non-OK status
    // and the contents of "result" are invalid.
    //
    // REQUIRES: External synchronization
    virtual Status read(Slice* result) = 0;

    // Skip "n" bytes from the file. This is guaranteed to be no
    // slower that reading the same data, but may be faster.
    //
    // If end of file is reached, skipping will stop at the end of the
    // file, and Skip will return OK.
    //
    // REQUIRES: External synchronization
    virtual Status skip(uint64_t n) = 0;

    // Returns the filename provided when the SequentialFile was constructed.
    virtual const std::string& filename() const = 0;
};

class RandomAccessFile {
public:
    RandomAccessFile() { }
    virtual ~RandomAccessFile() { }

    // Read "result.size" bytes from the file starting at "offset".
    // Copies the resulting data into "result.data".
    //
    // If an error was encountered, returns a non-OK status.
    //
    // This method will internally retry on EINTR and "short reads" in order to
    // fully read the requested number of bytes. In the event that it is not
    // possible to read exactly 'length' bytes, an IOError is returned.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status read_at(uint64_t offset, const Slice& result) const = 0;

    // Reads up to the "results" aggregate size, based on each Slice's "size",
    // from the file starting at 'offset'. The Slices must point to already-allocated
    // buffers for the data to be written to.
    //
    // If an error was encountered, returns a non-OK status.
    //
    // This method will internally retry on EINTR and "short reads" in order to
    // fully read the requested number of bytes. In the event that it is not
    // possible to read exactly 'length' bytes, an IOError is returned.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const = 0;

    // Return the size of this file
    virtual Status size(uint64_t* size) const = 0;

    // Return name of this file
    virtual const std::string& file_name() const = 0;
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
// Note: To avoid user misuse, WritableFile's API should support only
// one of Append or PositionedAppend. We support only Append here.
class WritableFile {
public:
    enum FlushMode {
        FLUSH_SYNC,
        FLUSH_ASYNC
    };

    WritableFile() { }
    virtual ~WritableFile() { }

    // Append data to the end of the file
    virtual Status append(const Slice& data) = 0;

    // If possible, uses scatter-gather I/O to efficiently append
    // multiple buffers to a file. Otherwise, falls back to regular I/O.
    //
    // For implementation specific quirks and details, see comments in
    // implementation source code (e.g., env_posix.cc)
    virtual Status appendv(const Slice* data, size_t cnt) = 0;

    // Pre-allocates 'size' bytes for the file in the underlying filesystem.
    // size bytes are added to the current pre-allocated size or to the current
    // offset, whichever is bigger. In no case is the file truncated by this
    // operation.
    //
    // On some implementations, preallocation is done without initializing the
    // contents of the data blocks (as opposed to writing zeroes), requiring no
    // IO to the data blocks.
    //
    // In no case is the file truncated by this operation.
    virtual Status pre_allocate(uint64_t size) = 0;

    virtual Status close() = 0;

    // Flush all dirty data (not metadata) to disk.
    //
    // If the flush mode is synchronous, will wait for flush to finish and
    // return a meaningful status.
    virtual Status flush(FlushMode mode) = 0;

    virtual Status sync() = 0;

    virtual uint64_t size() const = 0;

    // Returns the filename provided when the WritableFile was constructed.
    virtual const std::string& filename() const = 0;

private:
    // No copying allowed
    WritableFile(const WritableFile&);
    void operator=(const WritableFile&);
};

// A file abstraction for random reading and writing.
class RandomRWFile {
public:
    enum FlushMode {
        FLUSH_SYNC,
        FLUSH_ASYNC
    };
    RandomRWFile() {}
    virtual ~RandomRWFile() { }

    virtual Status read_at(uint64_t offset, const Slice& result) const = 0;

    virtual Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const = 0;

    virtual Status write_at(uint64_t offset, const Slice& data) = 0;

    virtual Status writev_at(uint64_t offset, const Slice* data, size_t data_cnt) = 0;

    virtual Status flush(FlushMode mode, uint64_t offset, size_t length) = 0;

    virtual Status sync() = 0;

    virtual Status close() = 0;

    virtual Status size(uint64_t* size) const = 0;
    virtual const std::string& filename() const = 0;
};

}
