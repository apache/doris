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

namespace java org.apache.doris.thrift
namespace cpp doris

enum TBrokerOperationStatusCode {
    OK = 0;
    END_OF_FILE = 301;
    
    // user input error
    NOT_AUTHORIZED = 401;
    DUPLICATE_REQUEST = 402;
    INVALID_INPUT_OFFSET = 403; // user input offset is invalid, is large than file length
    INVALID_INPUT_FILE_PATH = 404;
    INVALID_ARGUMENT = 405;
    
    // internal server error
    FILE_NOT_FOUND = 501;
    TARGET_STORAGE_SERVICE_ERROR = 502; // the target storage service error
    OPERATION_NOT_SUPPORTED = 503; // the api is not implemented
}

struct TBrokerOperationStatus {
    1: required TBrokerOperationStatusCode statusCode;
    2: optional string message;
}

enum TBrokerVersion {
    VERSION_ONE = 1;
}

enum TBrokerOpenMode {
    APPEND = 1;
}

struct TBrokerFileStatus {
    1: required string path; //file path
    2: required bool isDir;  //determine whether it is a directory or a file
    3: required i64 size;    //file size
    4: required bool isSplitable; //false mean indicates that the file is indivisible,
                                  //and the entire file must be imported as a complete map task.
                                  //the return value of the compressed file is false
    5: optional i64 blockSize; //Block size in FS. e.g. HDFS and S3
    6: optional i64 modificationTime = 0; // Last modification time
}

struct TBrokerFD {
    1: required i64 high;
    2: required i64 low;
}

struct TBrokerListResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional list<TBrokerFileStatus> files;
    
}

struct TBrokerOpenReaderResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional TBrokerFD fd;
    3: optional i64 size; // file size(Deprecated)
}

struct TBrokerReadResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional binary data; 
}

struct TBrokerOpenWriterResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional TBrokerFD fd;
}

struct TBrokerCheckPathExistResponse {
    1: required TBrokerOperationStatus opStatus;
    2: required bool isPathExist;
}

struct TBrokerIsSplittableResponse {
    1: optional TBrokerOperationStatus opStatus;
    2: optional bool splittable;
}

struct TBrokerListPathRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required bool isRecursive;
    4: required map<string,string> properties;
    5: optional bool fileNameOnly;
    6: optional bool onlyFiles;
}

struct TBrokerIsSplittableRequest {
    1: optional TBrokerVersion version;
    2: optional string path;
    3: optional string inputFormat;
    4: optional map<string,string> properties;
}

struct TBrokerDeletePathRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required map<string,string> properties;
}

struct TBrokerRenamePathRequest {
    1: required TBrokerVersion version;
    2: required string srcPath;
    3: required string destPath;
    4: required map<string,string> properties;
}

struct TBrokerCheckPathExistRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required map<string,string> properties;
}

struct TBrokerOpenReaderRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required i64 startOffset;
    4: required string clientId;
    5: required map<string,string> properties;
}

struct TBrokerPReadRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
    3: required i64 offset;
    4: required i64 length;
}

struct TBrokerSeekRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
    3: required i64 offset;
}

struct TBrokerCloseReaderRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
}

struct TBrokerOpenWriterRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required TBrokerOpenMode openMode;
    4: required string clientId;
    5: required map<string,string> properties;
}

struct TBrokerPWriteRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
    3: required i64 offset;
    4: required binary data;
}

struct TBrokerCloseWriterRequest {
    1: required TBrokerVersion version;
    2: required TBrokerFD fd;
}

struct TBrokerPingBrokerRequest {
    1: required TBrokerVersion version;
    2: required string clientId;
}

struct TBrokerFileSizeRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: optional map<string,string> properties;
}

struct TBrokerFileSizeResponse {
    1: required TBrokerOperationStatus opStatus;
    2: optional i64 fileSize;
}

service TPaloBrokerService {
    
    // return a list of files under a path
    TBrokerListResponse listPath(1: TBrokerListPathRequest request);

    // return located files of a given path. A broker implementation refers to
    // 'org.apache.doris.fs.remote.RemoteFileSystem#listLocatedFiles' in fe-core.
    TBrokerListResponse listLocatedFiles(1: TBrokerListPathRequest request);

    // return whether the path with specified input format is splittable.
    TBrokerIsSplittableResponse isSplittable(1: TBrokerIsSplittableRequest request);
    
    // delete a file, if the deletion of the file fails, the status code will return an error message
    // input:
    //     path: path to delete file
    TBrokerOperationStatus deletePath(1: TBrokerDeletePathRequest request);

    // rename the file
    TBrokerOperationStatus renamePath(1: TBrokerRenamePathRequest request);

    // check if a file exits
    TBrokerCheckPathExistResponse checkPathExist(1: TBrokerCheckPathExistRequest request);
    
    // open a file for reading
    // input:
    //     path: file path
    //     startOffset: read start position
    // return:
    //     fd: The unique identifier of the file read stream in the broker. The user needs to
    //carry this fd every time they read it. The reason is that there may be multiple clients
    //reading the same file on a broker, so fd is needed to read the file separately identified.
    TBrokerOpenReaderResponse openReader(1: TBrokerOpenReaderRequest request);
    
    // read file data
    // input:
    //     fd: returned when open reader
    //     length: read file length
    //     offset: the user must carry the offset every time when reading, but this does not
    //mean that the user can specify the offset to read. This offset is mainly used by the backend
    //to verify whether to read repeatedly.During network communication, it is very likely that
    //the user initiates a read, but does not get the result,and then reads it again. It is very
    //likely that the first read has already occurred, but the user does not receive the result,
    //so the user initiates a second read. The data returned is wrong,but the user cannot perceive it.
    // return:
    //     under normal circumstances, binary data is returned. In abnormal cases, such as the reader is closed,
    //the file is read to the end of the file, etc. it needs to be returned through the status code.
    TBrokerReadResponse pread(1: TBrokerPReadRequest request);
    
    // position the reader's offset to a specific position
    TBrokerOperationStatus seek(1: TBrokerSeekRequest request);
    
    // close reader
    TBrokerOperationStatus closeReader(1: TBrokerCloseReaderRequest request);
    
    // open a file to write a stream according to the path. This API is mainly designed for backup
    //and restore in the future. At present, this API is not needed for import.
    //    1. if the file does not exist then create it and return fd;
    //    2. if the file exists, but is a directory, return failure
    //the parameters for recursively creating folders are not provided here.
    //the default is that if the folder does not exist, it will be created.
    // this API is currently considered for backup use only, and has nothing to do with importing.
    // input:
    //     openMode: options to open the write stream：overwrite、create new、append
    TBrokerOpenWriterResponse openWriter(1: TBrokerOpenWriterRequest request);
    
    // write data to the file corresponding to fd
    TBrokerOperationStatus pwrite(1: TBrokerPWriteRequest request);
    
    // close file write stream
    TBrokerOperationStatus closeWriter(1: TBrokerCloseWriterRequest request);
    
    // ping broker service
    TBrokerOperationStatus ping(1: TBrokerPingBrokerRequest request);

    // get size of specified file
    TBrokerFileSizeResponse fileSize(1: TBrokerFileSizeRequest request);
}
