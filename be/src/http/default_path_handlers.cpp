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

#include "http/default_path_handlers.h"

#include <gen_cpp/Metrics_types.h>

#include <boost/algorithm/string/replace.hpp>
#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif
#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
#include <gperftools/malloc_extension.h>
#endif

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "http/action/tablets_info_action.h"
#include "http/web_page_handler.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "util/easy_json.h"
#include "util/mem_info.h"
#include "util/perf_counters.h"
#include "util/pretty_printer.h"
#include "util/thread.h"

using std::vector;
using std::shared_ptr;
using std::string;

namespace doris {

// Writes the last config::web_log_bytes of the INFO logfile to a webpage
// Note to get best performance, set GLOG_logbuflevel=-1 to prevent log buffering
void logs_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    /*std::string logfile;
    get_full_log_filename(google::INFO, &logfile);
    (*output) << "<h2>INFO logs</h2>" << std::endl;
    (*output) << "Log path is: " << logfile << std::endl;

    struct stat file_stat;

    if (stat(logfile.c_str(), &file_stat) == 0) {
        long size = file_stat.st_size;
        long seekpos = size < config::web_log_bytes ? 0L : size - config::web_log_bytes;
        std::ifstream log(logfile.c_str(), std::ios::in);
        // Note if the file rolls between stat and seek, this could fail
        // (and we could wind up reading the whole file). But because the
        // file is likely to be small, this is unlikely to be an issue in
        // practice.
        log.seekg(seekpos);
        (*output) << "<br/>Showing last " << config::web_log_bytes << " bytes of log" << std::endl;
        (*output) << "<br/><pre>" << log.rdbuf() << "</pre>";

    } else {
        (*output) << "<br/>Couldn't open INFO log file: " << logfile;
    }*/

    (*output) << "<br/>Couldn't open INFO log file: ";
}

// Registered to handle "/varz", and prints out all command-line flags and their values
void config_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Configurations</h2>";
    (*output) << "<pre>";
    std::lock_guard<std::mutex> lock(*config::get_mutable_string_config_lock());
    for (const auto& it : *(config::full_conf_map)) {
        (*output) << it.first << "=" << it.second << std::endl;
    }
    (*output) << "</pre>";
}

// Registered to handle "/memz", and prints out memory allocation statistics.
void mem_usage_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<pre>"
              << "Mem Limit: " << PrettyPrinter::print(MemInfo::mem_limit(), TUnit::BYTES)
              << std::endl
              << "Physical Mem From Perf: "
              << PrettyPrinter::print(PerfCounters::get_vm_rss(), TUnit::BYTES) << std::endl
              << "</pre>";

    (*output) << "<pre>";
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    (*output) << "Memory tracking is not available with address sanitizer builds.";
#elif defined(USE_JEMALLOC)
    std::string tmp;
    auto write_cb = [](void* opaque, const char* buf) {
        auto* _opaque = static_cast<std::string*>(opaque);
        _opaque->append(buf);
    };
    jemalloc_stats_print(write_cb, &tmp, "a");
    boost::replace_all(tmp, "\n", "<br>");
    (*output) << tmp << "</pre>";
#else
    char buf[2048];
    MallocExtension::instance()->GetStats(buf, 2048);
    // Replace new lines with <br> for html
    std::string tmp(buf);
    boost::replace_all(tmp, "\n", "<br>");
    (*output) << tmp << "</pre>";
#endif
}

void display_tablets_callback(const WebPageHandler::ArgumentMap& args, EasyJson* ej) {
    std::string tablet_num_to_return;
    auto it = args.find("limit");
    if (it != args.end()) {
        tablet_num_to_return = it->second;
    } else {
        tablet_num_to_return = "1000"; // default
    }
    (*ej) = TabletsInfoAction::get_tablets_info(tablet_num_to_return);
}

// Registered to handle "/mem_tracker", and prints out memory tracker information.
void mem_tracker_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h1>Memory usage by subsystem</h1>\n";
    std::vector<MemTracker::Snapshot> snapshots;
    auto iter = args.find("type");
    if (iter != args.end()) {
        if (iter->second == "global") {
            MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::GLOBAL);
        } else if (iter->second == "query") {
            MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::QUERY);
        } else if (iter->second == "load") {
            MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::LOAD);
        } else if (iter->second == "compaction") {
            MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::COMPACTION);
        } else if (iter->second == "schema_change") {
            MemTrackerLimiter::make_type_snapshots(&snapshots,
                                                   MemTrackerLimiter::Type::SCHEMA_CHANGE);
        } else if (iter->second == "other") {
            MemTrackerLimiter::make_type_snapshots(&snapshots, MemTrackerLimiter::Type::OTHER);
        } else if (iter->second == "reserved_memory") {
            GlobalMemoryArbitrator::make_reserved_memory_snapshots(&snapshots);
        } else if (iter->second == "all") {
            MemTrackerLimiter::make_all_memory_state_snapshots(&snapshots);
        }
    } else {
        (*output) << "<h4>*Notice:</h4>\n";
        (*output) << "<h4>    1. MemTracker only counts the memory on part of the main execution "
                     "path, "
                     "which is usually less than the real process memory.</h4>\n";
        (*output) << "<h4>    2. each `type` is the sum of a set of tracker values, "
                     "`sum of all trackers` is the sum of all trackers of all types, .</h4>\n";
        (*output) << "<h4>    3. `process resident memory` is the physical memory of the process, "
                     "from /proc VmRSS VmHWM.</h4>\n";
        (*output) << "<h4>    4. `process virtual memory` is the virtual memory of the process, "
                     "from /proc VmSize VmPeak.</h4>\n";
        (*output) << "<h4>    5.`/mem_tracker?type=<type name>` to view the memory details of each "
                     "type, for example, `/mem_tracker?type=query` will list the memory of all "
                     "queries; "
                     "`/mem_tracker?type=global` will list the memory of all Cache, metadata and "
                     "other "
                     "global life cycles.</h4>\n";
        (*output) << "<h4>see documentation for details.";
        MemTrackerLimiter::make_process_snapshots(&snapshots);
    }

    (*output) << "<table data-toggle='table' "
                 "       data-pagination='true' "
                 "       data-search='true' "
                 "       class='table table-striped'>\n";
    (*output) << "<thead><tr>"
                 "<th data-sortable='true'>Type</th>"
                 "<th data-sortable='true'>Label</th>"
                 "<th data-sortable='true'>Parent Label</th>"
                 "<th>Limit</th>"
                 "<th data-sortable='true' "
                 ">Current Consumption(Bytes)</th>"
                 "<th>Current Consumption(Normalize)</th>"
                 "<th data-sortable='true' "
                 ">Peak Consumption(Bytes)</th>"
                 "<th>Peak Consumption(Normalize)</th>"
                 "</tr></thead>";
    (*output) << "<tbody>\n";
    for (const auto& item : snapshots) {
        string limit_str = item.limit == -1 ? "none" : AccurateItoaKMGT(item.limit);
        string current_consumption_normalize = AccurateItoaKMGT(item.cur_consumption);
        string peak_consumption_normalize = AccurateItoaKMGT(item.peak_consumption);
        (*output) << strings::Substitute(
                "<tr><td>$0</td><td>$1</td><td>$2</td><td>$3</td><td>$4</td><td>$5</td><td>$6</"
                "td><td>$7</td></tr>\n",
                item.type, item.label, item.parent_label, limit_str, item.cur_consumption,
                current_consumption_normalize, item.peak_consumption, peak_consumption_normalize);
    }
    (*output) << "</tbody></table>\n";
}

void heap_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Heap Profile</h2>" << std::endl;

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    (*output) << "<pre>" << std::endl;
    (*output) << "Heap profiling is not available with address sanitizer builds." << std::endl;
    (*output) << "</pre>" << std::endl;
    return;

#else
    (*output) << "<pre>" << std::endl;
    (*output) << "Heap profiling will use pprof tool to sample and get heap profile. It will take "
                 "30 seconds"
              << std::endl;
    (*output) << "(Only one thread can obtain profile at the same time)" << std::endl;
    (*output) << std::endl;
    (*output) << "If you want to get the Heap profile, you need to install gperftools-2.0 on the "
                 "host machine,"
              << std::endl;
    (*output) << "and make sure there is a 'pprof' executable file in the system PATH or "
                 "'be/tools/bin/' directory."
              << std::endl;
    (*output) << "Doris will obtain Profile in the following ways:" << std::endl;
    (*output) << std::endl;
    (*output) << "    curl http://localhost:" << config::webserver_port
              << "/pprof/heap?seconds=30 > perf.data" << std::endl;
    (*output) << "    pprof --text be/lib/doris_be perf.data" << std::endl;
    (*output) << std::endl;
    (*output) << "</pre>" << std::endl;
    (*output) << "<div id=\"heap\">" << std::endl;
    (*output) << "    <div><button type=\"button\" id=\"getHeap\">Profile It!</button></div>"
              << std::endl;
    (*output) << "    <br/>" << std::endl;
    (*output) << "    <div id=\"heapResult\"><pre id=\"heapContent\"></pre></div>" << std::endl;
    (*output) << "</div>" << std::endl;

    (*output) << "<script>" << std::endl;
    (*output) << "$('#getHeap').click(function () {" << std::endl;
    (*output) << "    document.getElementById(\"heapContent\").innerText = \"Sampling... (30 "
                 "seconds)\";"
              << std::endl;
    (*output) << "    $.ajax({" << std::endl;
    (*output) << "        type: \"GET\"," << std::endl;
    (*output) << "        dataType: \"text\"," << std::endl;
    (*output) << "        url: \"pprof/heap?readable=true\"," << std::endl;
    (*output) << "        timeout: 60000," << std::endl;
    (*output) << "        success: function (result) {" << std::endl;
    (*output) << "            $('#heapResult').removeClass('hidden');" << std::endl;
    (*output) << "            document.getElementById(\"heapContent\").innerText = result;"
              << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "        error: function (result) {" << std::endl;
    (*output) << "            alert(result);" << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "    });" << std::endl;
    (*output) << "});" << std::endl;

    (*output) << "</script>" << std::endl;

    return;
#endif
}

void cpu_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>CPU Profile</h2>" << std::endl;

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER) || \
        defined(USE_JEMALLOC)
    (*output) << "<pre>" << std::endl;
    (*output) << "CPU profiling is not available with address sanitizer builds." << std::endl;
    (*output) << "</pre>" << std::endl;
    return;

#else

    (*output) << "<pre>" << std::endl;
    (*output) << "CPU profiling will use perf tool to sample and get CPU profile. It will take 30 "
                 "seconds"
              << std::endl;
    (*output) << "(Only one thread can obtain profile at the same time)" << std::endl;
    (*output) << std::endl;
    (*output) << "If you want to get the CPU profile in text form, you need to install "
                 "gperftools-2.0 on the host machine,"
              << std::endl;
    (*output) << "and make sure there is a 'pprof' executable file in the system PATH or "
                 "'be/tools/bin/' directory."
              << std::endl;
    (*output) << "Doris will obtain Profile in the following ways:" << std::endl;
    (*output) << std::endl;
    (*output) << "    curl http://localhost:" << config::webserver_port
              << "/pprof/profile?seconds=30 > perf.data" << std::endl;
    (*output) << "    pprof --text be/lib/doris_be perf.data" << std::endl;
    (*output) << std::endl;
    (*output) << "If you want to get the flame graph, you must first make sure that there is a "
                 "'perf' command on the host machine."
              << std::endl;
    (*output) << "And you need to download the FlameGraph and place it under 'be/tools/FlameGraph'."
              << std::endl;
    (*output) << "Finally, check if the following files exist" << std::endl;
    (*output) << std::endl;
    (*output) << "    be/tools/FlameGraph/stackcollapse-perf.pl" << std::endl;
    (*output) << "    be/tools/FlameGraph/flamegraph.pl" << std::endl;
    (*output) << std::endl;
    (*output) << "Doris will obtain the flame graph in the following ways:" << std::endl;
    (*output) << std::endl;
    (*output) << "    perf record -m 2 -g -p be_pid -o perf.data - sleep 30" << std::endl;
    (*output) << "    perf script -i perf.data | stackcollapse-perf.pl | flamegraph.pl > "
                 "flamegraph.svg"
              << std::endl;
    (*output) << std::endl;
    (*output) << "</pre>" << std::endl;
    (*output) << "<div id=\"cpu\">" << std::endl;
    (*output)
            << "    <div><button type=\"button\" id=\"getCpu\">Profile It! (Text)</button><button "
               "type=\"button\" id=\"getCpuGraph\">Profile It! (FlameGraph)</button></div>"
            << std::endl;
    (*output) << "    <br/>" << std::endl;
    (*output) << "    <div id=\"cpuResult\"><pre id=\"cpuContent\"></pre></div>" << std::endl;
    (*output) << "    <br/>" << std::endl;
    (*output) << "    <div id=\"cpuResultGraph\"><pre id=\"cpuContentGraph\"></pre></div>"
              << std::endl;
    (*output) << "</div>" << std::endl;

    // for text profile
    (*output) << "<script>" << std::endl;
    (*output) << "$('#getCpu').click(function () {" << std::endl;
    (*output) << "    document.getElementById(\"cpuContent\").innerText = \"Sampling... (30 "
                 "seconds)\";"
              << std::endl;
    (*output) << "    $.ajax({" << std::endl;
    (*output) << "        type: \"GET\"," << std::endl;
    (*output) << "        dataType: \"text\"," << std::endl;
    (*output) << "        url: \"pprof/profile?type=text\"," << std::endl;
    (*output) << "        timeout: 60000," << std::endl;
    (*output) << "        success: function (result) {" << std::endl;
    (*output) << "            document.getElementById(\"cpuContent\").innerText = result;"
              << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "        error: function (result) {" << std::endl;
    (*output) << "            alert(result);" << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "    });" << std::endl;
    (*output) << "});" << std::endl;

    // for graph profile
    (*output) << "$('#getCpuGraph').click(function () {" << std::endl;
    (*output) << "    document.getElementById(\"cpuContentGraph\").innerText = \"Sampling... (30 "
                 "seconds)\";"
              << std::endl;
    (*output) << "    $.ajax({" << std::endl;
    (*output) << "        type: \"GET\"," << std::endl;
    (*output) << "        dataType: \"text\"," << std::endl;
    (*output) << "        url: \"pprof/profile?type=flamegraph\"," << std::endl;
    (*output) << "        timeout: 60000," << std::endl;
    (*output) << "        success: function (result) {" << std::endl;
    (*output) << "            document.getElementById(\"cpuResultGraph\").innerHTML = result;"
              << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "        error: function (result) {" << std::endl;
    (*output) << "            alert(result);" << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "    });" << std::endl;
    (*output) << "});" << std::endl;

    (*output) << "</script>" << std::endl;

    return;
#endif
}

void add_default_path_handlers(WebPageHandler* web_page_handler) {
    // TODO(yingchun): logs_handler is not implemented yet, so not show it on navigate bar
    web_page_handler->register_page("/logs", "Logs", logs_handler, false /* is_on_nav_bar */);
    if (!config::hide_webserver_config_page) {
        web_page_handler->register_page("/varz", "Configs", config_handler,
                                        true /* is_on_nav_bar */);
    }
    web_page_handler->register_page("/memz", "Memory", mem_usage_handler, true /* is_on_nav_bar */);
    web_page_handler->register_page(
            "/mem_tracker", "MemTracker",
            [](auto&& PH1, auto&& PH2) {
                return mem_tracker_handler(std::forward<decltype(PH1)>(PH1),
                                           std::forward<decltype(PH2)>(PH2));
            },
            true /* is_on_nav_bar */);
    web_page_handler->register_page("/heap", "Heap Profile", heap_handler,
                                    true /* is_on_nav_bar */);
    web_page_handler->register_page("/cpu", "CPU Profile", cpu_handler, true /* is_on_nav_bar */);
    register_thread_display_page(web_page_handler);
    web_page_handler->register_template_page(
            "/tablets_page", "Tablets",
            [](auto&& PH1, auto&& PH2) {
                return display_tablets_callback(std::forward<decltype(PH1)>(PH1),
                                                std::forward<decltype(PH2)>(PH2));
            },
            true /* is_on_nav_bar */);
}

} // namespace doris
