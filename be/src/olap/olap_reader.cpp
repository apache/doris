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

#include "olap/olap_reader.h"

#include <sstream>

#include "runtime/datetime_value.h"
#include "util/palo_metrics.h"

using std::exception;
using std::string;
using std::stringstream;
using std::vector;
using std::map;
using std::nothrow;

namespace palo {

Status OLAPShowHints::show_hints(
        TShowHintsRequest& fetch_request,
        std::vector<std::vector<std::vector<std::string>>>* ranges, 
        RuntimeProfile* profile) {
    OLAP_LOG_DEBUG("Show hints:%s", apache::thrift::ThriftDebugString(fetch_request).c_str());
    {
        RuntimeProfile::Counter* show_hints_timer = profile->get_counter("ShowHintsTime");
        SCOPED_TIMER(show_hints_timer);
    
        OLAPStatus res = OLAP_SUCCESS;
        ranges->clear();
    
        SmartOLAPTable table = OLAPEngine::get_instance()->get_table(
                fetch_request.tablet_id, fetch_request.schema_hash);
        if (table.get() == NULL) {
            OLAP_LOG_WARNING("table does not exists. [tablet_id=%ld schema_hash=%d]",
                             fetch_request.tablet_id, fetch_request.schema_hash);
            return Status("table does not exists");
        }
    
        vector<string> start_key_strings;
        vector<string> end_key_strings;
        vector<vector<string>> range;
        if (fetch_request.start_key.size() == 0) {
            res = table->split_range(start_key_strings,
                                     end_key_strings,
                                     fetch_request.block_row_count,
                                     &range);
    
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to show hints by split range. [res=%d]", res);
                return Status("fail to show hints");
            }
    
            ranges->push_back(range);
        } else {
            for (int key_pair_index = 0;
                    key_pair_index < fetch_request.start_key.size(); ++key_pair_index) {
                start_key_strings.clear();
                end_key_strings.clear();
                range.clear();
    
                TFetchStartKey& start_key_field = fetch_request.start_key[key_pair_index];
                for (vector<string>::const_iterator it = start_key_field.key.begin();
                        it != start_key_field.key.end(); ++it) {
                    start_key_strings.push_back(*it);
                }
    
                TFetchEndKey& end_key_field = fetch_request.end_key[key_pair_index];
                for (vector<string>::const_iterator it = end_key_field.key.begin();
                        it != end_key_field.key.end(); ++it) {
                    end_key_strings.push_back(*it);
                }
    
                res = table->split_range(start_key_strings,
                                         end_key_strings,
                                         fetch_request.block_row_count,
                                         &range);
                if (res != OLAP_SUCCESS) {
                    OLAP_LOG_WARNING("fail to show hints by split range. [res=%d]", res);
                    return Status("fail to show hints");
                }
    
                ranges->push_back(range);
            }
        }
    }

    return Status::OK;
}

}  // namespace palo
