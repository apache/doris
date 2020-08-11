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

#include "udf_sample.h"
#include <string>
#include <algorithm>

using namespace std;
namespace doris_udf {

StringVal ParseUrlUdf(FunctionContext* context, const StringVal& urlStr, const StringVal& partToExtract) {
    if (urlStr.is_null || partToExtract.is_null) {
        return StringVal::null();
    }
    std::string raw_url = std::string(reinterpret_cast<const char*>(urlStr.ptr), urlStr.len);
    std::string part = std::string(reinterpret_cast<const char*>(partToExtract.ptr), partToExtract.len);
    std::string path,domain,x,protocol,port,query;
    int offset = 0;
    size_t pos1,pos2,pos3,pos4;
    x = raw_url;
    offset = offset==0 && x.compare(0, 8, "https://")==0 ? 8 : offset;
    offset = offset==0 && x.compare(0, 7, "http://" )==0 ? 7 : offset;
    pos1 = x.find_first_of('/', offset+1 );
    path = pos1==std::string::npos ? "" : x.substr(pos1);
    domain = std::string( x.begin()+offset, pos1 != std::string::npos ? x.begin()+pos1 : x.end() );
    path = (pos2 = path.find("#"))!=std::string::npos ? path.substr(0,pos2) : path;
    port = (pos3 = domain.find(":"))!=std::string::npos ? domain.substr(pos3+1) : "";
    domain = domain.substr(0, pos3!=std::string::npos ? pos3 : domain.length());
    protocol = offset > 0 ? x.substr(0,offset-3) : "";
    query = (pos4 = path.find("?"))!=std::string::npos ? path.substr(pos4+1) : "";
    path = pos4 != std::string::npos ? path.substr(0, pos4) : path;
    std::string res = "";
    if (part == "HOST") {
        res = domain;
    } else if (part == "PORT") {
        res = port;
    } else if (part == "PATH") {
        res = path;
    } else if (part == "QUERY") {
        res = query;
    } else if (part == "PROTOCOL") {
        res = protocol;
    }
	return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>(res.c_str())), res.size());
}


/// --- Prepare / Close Functions ---
/// ---------------------------------
void ParUrlUdfPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope){}
void ParseUrlUdfClose(FunctionContext* context, FunctionContext::FunctionStateScope scope) {}

}
