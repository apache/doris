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
    // enum value                      | file exists       | file does not exist
    // --------------------------------+-------------------+--------------------
    // CREATE_IF_NON_EXISTING_TRUNCATE | opens + truncates | creates
    // CREATE_NON_EXISTING             | fails             | creates
    // OPEN_EXISTING                   | opens             | fails
    enum CreateMode {
        CREATE_IF_NON_EXISTING_TRUNCATE,
        CREATE_NON_EXISTING,
        OPEN_EXISTING
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

    // Returns OK if the named file exists.
    //         NotFound if the named file does not exist,
    //                  the calling process does not have permission to determine
    //                  whether this file exists, or if the path is invalid.
    //         IOError if an IO Error was encountered
    virtual Status file_exists(const std::string& fname) = 0;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    // Returns OK if "dir" exists and "*result" contains its children.
    //         NotFound if "dir" does not exist, the calling process does not have
    //                  permission to access "dir", or if "dir" is invalid.
    //         IOError if an IO Error was encountered
    virtual Status get_children(const std::string& dir,
                                std::vector<std::string>* result) = 0;

    // Delete the named file.
    virtual Status delete_file(const std::string& fname) = 0;

    // Create the specified directory. Returns error if directory exists.
    virtual Status create_dir(const std::string& dirname) = 0;

    // Creates directory if missing. Return Ok if it exists, or successful in
    // Creating.
    virtual Status create_dir_if_missing(const std::string& dirname) = 0;

    // Delete the specified directory.
    virtual Status delete_dir(const std::string& dirname) = 0;

    virtual Status get_file_size(const std::string& fname, uint64_t* size) = 0;

    // Store the last modification time of fname in *file_mtime.
    virtual Status get_file_modification_time(const std::string& fname,
                                              uint64_t* file_mtime) = 0;
    // Rename file src to target.
    virtual Status rename_file(const std::string& src,
                               const std::string& target) = 0;

    // Hard Link file src to target.
    virtual Status link_file(const std::string& /*src*/,
                             const std::string& /*target*/) {
        return Status::NotSupported("link file is not supported for this Env");
    }
};

struct RandomAccessFileOptions {
    RandomAccessFileOptions() { }
};

// Creation-time options for WritableFile
struct WritableFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = false;
    // See CreateMode for details.
    Env::CreateMode mode = Env::CREATE_IF_NON_EXISTING_TRUNCATE;
};

// Creation-time options for RWFile
struct RandomRWFileOptions {
    // Call Sync() during Close().
    bool sync_on_close = false;
    // See CreateMode for details.
    Env::CreateMode mode = Env::CREATE_IF_NON_EXISTING_TRUNCATE;
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
class WritableFile {
public:
    enum FlushMode {
        FLUSH_SYNC,
        FLUSH_ASYNC
    };

    WritableFile() { }
    virtual ~WritableFile() { }

    // Append data to the end of the file
    // Note: A WritableFile object must support either Append or
    // PositionedAppend, so the users cannot mix the two.
    virtual Status append(const Slice& data) = 0;

    virtual Status appendv(const Slice* data, size_t cnt) = 0;

    virtual Status pre_allocate(uint64_t size) = 0;

    virtual Status close() = 0;

    virtual Status flush(FlushMode mode) = 0;

    virtual Status sync() = 0; // sync data

    virtual uint64_t size() const = 0;

    // Returns the filename provided when the WritableFile was constructed.
    virtual const std::string& filename() const = 0;
};

// A file abstraction for random reading and writing.
class RandomRWFile {
public:
    enum FlushMode {
        FLUSH_SYNC,
        FLUSH_ASYNC
    };
    RandomRWFile() {}
    virtual ~RandomRWFile() {}

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
