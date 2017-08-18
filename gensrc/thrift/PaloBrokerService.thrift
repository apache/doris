// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace java com.baidu.palo.thrift
namespace cpp palo

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
    1: required string path; // 文件的路径
    2: required bool isDir; // 表示文件是个目录还是文件？
    3: required i64 size; // 文件的大小
    4: required bool isSplitable; // 如果这个值是false，那么表示这个文件不可以切分，整个文件必须作为
                                    // 一个完整的map task来进行导入,如果是一个压缩文件返回值也是false
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

struct TBrokerListPathRequest {
    1: required TBrokerVersion version;
    2: required string path;
    3: required bool isRecursive;
    4: required map<string,string> properties;
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

service TPaloBrokerService {
    
    // 返回一个路径下的文件列表
    TBrokerListResponse listPath(1: TBrokerListPathRequest request);
    
    // 删除一个文件，如果删除文件失败，status code会返回error信息
    // input:
    //     path: 删除文件的路径    
    TBrokerOperationStatus deletePath(1: TBrokerDeletePathRequest request);

    // 将文件重命名
    TBrokerOperationStatus renamePath(1: TBrokerRenamePathRequest request);

    // 检查一个文件是否存在
    TBrokerCheckPathExistResponse checkPathExist(1: TBrokerCheckPathExistRequest request);
    
    // 打开一个文件用来读取
    // input:
    //     path: 文件的路径
    //     startOffset: 读取的起始位置
    // return:
    //     fd: 在broker内对这个文件读取流的唯一标识，用户每次读取的时候都需要带着这个fd，原因是一个broker
    //         上可能有多个client端在读取同一个文件，所以需要用fd来分别标识。
    TBrokerOpenReaderResponse openReader(1: TBrokerOpenReaderRequest request);
    
    // 读取文件数据
    // input:
    //     fd: open reader时返回的fd
    //     length: 读取的长度
    //     offset: 用户读取时必须每次带着offset，但是这并不表示用户可以指定offset来读取，这个offset主要是后端
    //             用来验证是否重复读取时使用的。 在网络通信时很有可能用户发起一次读取，但是并没有得到result，
    //             然后又读取了一次，很有可能第一次读取已经发生了，只是用户没有收到结果，这样用户发起第二次读取，
    //             返回的数据是错的，但是用户无法感知。 
    // return:
    //     正常情况下返回binary数据，异常情况，比如reader关闭了，文件读取到文件末尾了等，需要通过status code来返回
    //     这里的binary以后会封装到一个response对象里。
    TBrokerReadResponse pread(1: TBrokerPReadRequest request);
    
    // 将reader的offset定位到特定的位置
    TBrokerOperationStatus seek(1: TBrokerSeekRequest request);
    
    // 将reader关闭
    TBrokerOperationStatus closeReader(1: TBrokerCloseReaderRequest request);
    
    // 根据path打开一个文件写入流，这个API主要是为以后的backup restore设计的，目前导入来看不需要这个API
    //    1. 如果文件不存在那么就创建，并返回fd；
    //    2. 如果文件存在，但是是一个directory，那么返回失败
    // 这里不提供递归创建文件夹的参数，默认就是文件夹如果不存在，那么就创建
    // 这个API 目前考虑只是为了备份使用的，跟导入无关。
    // input:
    //     openMode: 打开写入流的方式，可选项：overwrite， create new， append等
    TBrokerOpenWriterResponse openWriter(1: TBrokerOpenWriterRequest request);
    
    // 向fd对应的文件中写入数据
    TBrokerOperationStatus pwrite(1: TBrokerPWriteRequest request);
    
    // 将文件写入流关闭
    TBrokerOperationStatus closeWriter(1: TBrokerCloseWriterRequest request);
    
    // 
    TBrokerOperationStatus ping(1: TBrokerPingBrokerRequest request);
}
