// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#ifndef BDG_PALO_BE_SRC_RPC_FILE_UTILS_H
#define BDG_PALO_BE_SRC_RPC_FILE_UTILS_H

#include "util.h"

#include <mutex>
#include <vector>

extern "C" {
#include <dirent.h>
#include <sys/socket.h>
#include <sys/types.h>
}

namespace palo {

/**
 * The FileUtils class provides static functions to easily access
 * and handle files and the file system.
 */
class FileUtils {
public:

    /** Reads a whole file into a std::string
     *
     * @param fname The file name
     * @param contents A reference to a std::string which will receive the data
     * @return <i>true</i> on success, <i>false</i> on error
     */
    static bool read(const std::string &fname, std::string &contents);

    /** Reads data from a file descriptor into a buffer
     *
     * @param fd The open file descriptor
     * @param vptr Pointer to the memory buffer
     * @param n Maximum size to read, in bytes
     * @return The number of bytes read, or -1 on error
     */
    static ssize_t read(int fd, void *vptr, size_t n);

    /** Reads positional data from a file descriptor into a buffer
     *
     * @param fd The open file descriptor
     * @param vptr Pointer to the memory buffer
     * @param n Maximum size to read, in bytes
     * @param offset The start offset in the file
     * @return The number of bytes read, or -1 on error
     */
    static ssize_t pread(int fd, void *vptr, size_t n, off_t offset);

    /** Writes a std::string buffer to a file; the file is overwritten if it
     * already exists
     *
     * @param fname Path of the file that is (over)written
     * @param contents The string contents that are written to the file
     * @return Number of bytes written, or -1 on error
     */
    static ssize_t write(const std::string &fname, const std::string &contents);

    /** Writes a memory buffer to a file descriptor
     *
     * @param fd Open file handle
     * @param vptr Pointer to the memory buffer
     * @param n Size of the memory buffer, in bytes
     * @return Number of bytes written, or -1 on error
     */
    static ssize_t write(int fd, const void *vptr, size_t n);

    /** Writes a string to a file descriptor
     *
     * @param fd Open file handle
     * @param str std::string to write to file
     * @return Number of bytes written, or -1 on error
     */
    static ssize_t write(int fd, const std::string &str) {
        return write(fd, str.c_str(), str.length());
    }

    /** Atomically writes data from multiple buffers to a file descriptor
     *
     *    struct iovec {
     *      void  *iov_base;    // Starting address
     *      size_t iov_len;     // Number of bytes to transfer
     *    };
     *
     * @param fd Open file handle
     * @param vector An iovec array holding pointers to the data
     * @param count Number of iovec structures in @a vector
     * @return Total number of bytes written, or -1 on error
     */
    static ssize_t writev(int fd, const struct iovec *vector, int count);

    /** Sends data through a network connection; if the socket is TCP then
     * the address is ignored. For UDP sockets the address structure
     * specifies the recipient.
     *
     * @param fd Open file handle/socket descriptor
     * @param vptr Pointer to the memory buffer which is sent
     * @param n Size of the memory buffer, in bytes
     * @param to The recipient's address (only UDP; ignored for TCP sockets)
     * @param tolen Length of the sockaddr structure
     * @return Total number of bytes sent, or -1 on error
     */
    static ssize_t sendto(int fd, const void *vptr, size_t n,
            const sockaddr *to, socklen_t tolen);

    /** Sends data through a network connection
     *
     * @param fd Open file handle/socket descriptor
     * @param vptr Pointer to the memory buffer which is sent
     * @param n Size of the memory buffer, in bytes
     * @return Total number of bytes sent, or -1 on error
     */
    static ssize_t send(int fd, const void *vptr, size_t n);

    /** Receives data from a network connection and returns the sender's
     * address
     *
     * @param fd Open file handle/socket descriptor
     * @param vptr Pointer to the memory buffer which receives the data
     * @param n Capacity of the memory buffer, in bytes
     * @param from The sender's address
     * @param fromlen Length of the sockaddr structure
     * @return Total number of bytes received, or -1 on error, 0 on eof
     */
    static ssize_t recvfrom(int fd, void *vptr, size_t n,
            struct sockaddr *from, socklen_t *fromlen);

    /** Receives data from a network connection
     *
     * @param fd Open file handle/socket descriptor
     * @param vptr Pointer to the memory buffer which receives the data
     * @param n Capacity of the memory buffer, in bytes
     * @return Total number of bytes received, or -1 on error, 0 on eof
     */
    static ssize_t recv(int fd, void *vptr, size_t n);

    /** Sets fcntl flags of a socket
     *
     * @param fd Open file handle/socket descriptor
     * @param flags The fcntl flags; will be ORed with the existing flags
     * @return true on success, otherwise false (sets errno)
     */
    static bool set_flags(int fd, int flags);

    /** Reads a full file into a new buffer; the buffer is allocated with
     * operator new[], and the caller has to delete[] it.
     *
     * @param fname The file name
     * @param lenp Receives the length of the buffer, in bytes
     * @return A pointer allocated with new[]; needs to be delete[]d by
     *          the caller. Returns 0 on error (sets errno)
     */
    static char *file_to_buffer(const std::string &fname, off_t *lenp);

    /** Reads a full file into a std::string
     *
     * @param fname The file name
     * @return A string with the data, or an empty string on error (sets errno)
     */
    static std::string file_to_string(const std::string &fname);

    /** Maps a full file into memory using mmap; the mapping will be released
     * when the application terminates (there's currently no munmap)
     *
     * @param fname The file name
     * @param lenp Receives the length of the buffer, in bytes
     * @return A pointer to the mapped data
     */
    static void *mmap(const std::string &fname, off_t *lenp);

    /** Creates a directory (with all parent directories, if required)
     *
     * @param dirname The directory name to create
     * @return true on success, otherwise falls (sets errno)
     */
    static bool mkdirs(const std::string &dirname);

    /** Checks if a file or directory exists
     *
     * @return true if the file or directory exists, otherwise false
     */
    static bool exists(const std::string &fname);

    /** Unlinks (deletes) a file or directory
     *
     * @return true on success, otherwise false (sets errno)
     */
    static bool unlink(const std::string &fname);

    /** Renames a file or directory
     *
     * @param oldpath The path of the file (or directory) to rename
     * @param newpath The new filename
     * @return true on success, otherwise false (sets errno)
     */
    static bool rename(const std::string &oldpath, const std::string &newpath);

    /** Returns the size of a file (0 on error)
     *
     * @param fname The path of the file
     * @return The file size (in bytes) or 0 on error (sets errno)
     */
    static uint64_t size(const std::string &fname);

    /** Returns the size of a file (-1 on error)
     *
     * @param fname The path of the file
     * @return The file size (in bytes) or -1 on error (sets errno)
     */
    static off_t length(const std::string &fname);

    /** Adds a trailing slash to a path */
    static void add_trailing_slash(std::string &path);

    /** Expands a leading tilde character in a filename
     *
     *    Examples:
     *
     *      ~chris/foo -> /home/chris/foo
     *      ~/foo      -> /home/$USER/foo
     *
     *  @return true on success, false on error
     */
    static bool expand_tilde(std::string &fname);

    /** Reads all directory entries, applies a regular expression and returns
     * those which match.
     *
     * This function will call HT_FATAL on error!
     *
     * @param dirname The directory name
     * @param fname_regex The regular expression; can be empty
     * @param listing Vector with the results
     */
    static void readdir(const std::string &dirname, const std::string &fname_regex,
            std::vector<struct dirent> &listing);

    /// Mutex for protecting thread-unsafe glibc library function calls
    static std::mutex ms_mutex;
};

} //namespace palo
#endif //BDG_PALO_BE_SRC_RPC_FILE_UTILS_H
