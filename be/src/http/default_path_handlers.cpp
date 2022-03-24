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

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>

#include <boost/algorithm/string.hpp>
#include <sstream>

#include "agent/utils.h"
#include "common/configbase.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "http/action/tablets_info_action.h"
#include "http/web_page_handler.h"
#include "runtime/mem_tracker.h"
#include "util/debug_util.h"
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
void mem_usage_handler(const std::shared_ptr<MemTracker>& mem_tracker,
                       const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    if (mem_tracker != nullptr) {
        (*output) << "<pre>"
                  << "Mem Limit: " << PrettyPrinter::print(mem_tracker->limit(), TUnit::BYTES)
                  << std::endl
                  << "Mem Consumption: "
                  << PrettyPrinter::print(mem_tracker->consumption(), TUnit::BYTES) << std::endl
                  << "</pre>";
    } else {
        (*output) << "<pre>"
                  << "No process memory limit set."
                  << "</pre>";
    }

    (*output) << "<pre>";
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    (*output) << "Memory tracking is not available with address sanitizer builds.";
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
    TabletsInfoAction tablet_info_action;
    std::string tablet_num_to_return;
    WebPageHandler::ArgumentMap::const_iterator it = args.find("limit");
    if (it != args.end()) {
        tablet_num_to_return = it->second;
    } else {
        tablet_num_to_return = "1000"; // default
    }
    (*ej) = tablet_info_action.get_tablets_info(tablet_num_to_return);
}

// Registered to handle "/mem_tracker", and prints out memory tracker information.
void mem_tracker_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h1>Memory usage by subsystem</h1>\n";
    (*output) << "<table data-toggle='table' "
                 "       data-pagination='true' "
                 "       data-search='true' "
                 "       class='table table-striped'>\n";
    (*output) << "<thead><tr>"
                 "<th data-sortable='true' "
                 ">Id</th>"
                 "<th>Parent</th>"
                 "<th>Limit</th>"
                 "<th data-sorter='bytesSorter' "
                 "    data-sortable='true' "
                 ">Current Consumption</th>"
                 "<th data-sorter='bytesSorter' "
                 "    data-sortable='true' "
                 ">Peak Consumption</th>"
                 "<th data-sortable='true' "
                 ">Use Count</th></tr></thead>";
    (*output) << "<tbody>\n";

    std::vector<shared_ptr<MemTracker>> trackers;
    MemTracker::list_process_trackers(&trackers);
    for (const shared_ptr<MemTracker>& tracker : trackers) {
        string parent = tracker->parent() == nullptr ? "none" : tracker->parent()->label();
        string limit_str;
        string current_consumption_str;
        string peak_consumption_str;
        limit_str = tracker->limit() == -1 ? "none" : AccurateItoaKMGT(tracker->limit());
        current_consumption_str = AccurateItoaKMGT(tracker->consumption());
        peak_consumption_str = AccurateItoaKMGT(tracker->peak_consumption());
        
        int64_t use_count = tracker.use_count();
        (*output) << strings::Substitute(
                "<tr><td>$0</td><td>$1</td><td>$2</td>" // id, parent, limit
                "<td>$3</td><td>$4</td><td>$5</td></tr>\n",        // current, peak
                tracker->label(), parent, limit_str, current_consumption_str, peak_consumption_str, use_count);
    }
    (*output) << "</tbody></table>\n";
}

void heap_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h2>Heap Profile</h2>" << std::endl;

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
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
    (*output) << "    pprof --text be/lib/palo_be perf.data" << std::endl;
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

#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
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
    (*output) << "    pprof --text be/lib/palo_be perf.data" << std::endl;
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

void add_default_path_handlers(WebPageHandler* web_page_handler,
                               const std::shared_ptr<MemTracker>& process_mem_tracker) {
    // TODO(yingchun): logs_handler is not implemented yet, so not show it on navigate bar
    web_page_handler->register_page("/logs", "Logs", logs_handler, false /* is_on_nav_bar */);
    web_page_handler->register_page("/varz", "Configs", config_handler, true /* is_on_nav_bar */);
    web_page_handler->register_page("/memz", "Memory",
                                    std::bind<void>(&mem_usage_handler, process_mem_tracker,
                                                    std::placeholders::_1, std::placeholders::_2),
                                    true /* is_on_nav_bar */);
    web_page_handler->register_page("/mem_tracker", "MemTracker", mem_tracker_handler,
                                    true /* is_on_nav_bar */);
    web_page_handler->register_page("/heap", "Heap Profile", heap_handler,
                                    true /* is_on_nav_bar */);
    web_page_handler->register_page("/cpu", "CPU Profile", cpu_handler, true /* is_on_nav_bar */);
    register_thread_display_page(web_page_handler);
    web_page_handler->register_template_page(
            "/tablets_page", "Tablets",
            std::bind<void>(&display_tablets_callback, std::placeholders::_1,
                            std::placeholders::_2),
            true /* is_on_nav_bar */);
}

} // namespace doris
