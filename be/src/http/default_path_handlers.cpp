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
#include "http/action/tablets_info_action.h"
#include "http/web_page_handler.h"
#include "runtime/process_profile.h"
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

void memory_info_handler(std::stringstream* output) {
    (*output) << "<h2>Memory Info</h2>\n";
    (*output) << "<pre>";
    (*output) << "<h4 id=\"memoryDocumentsTitle\">Memory Documents</h4>\n"
              << "<a "
                 "href=https://doris.apache.org/zh-CN/docs/dev/admin-manual/memory-management/"
                 "overview>Memory Management Overview</a>\n"
              << "<a "
                 "href=https://doris.apache.org/zh-CN/docs/dev/admin-manual/memory-management/"
                 "memory-issue-faq>Memory Issue FAQ</a>\n"
              << "\n---\n\n";

    (*output) << "<h4 id=\"memoryPropertiesTitle\">Memory Properties</h4>\n"
              << "System Physical Mem: "
              << PrettyPrinter::print(MemInfo::physical_mem(), TUnit::BYTES) << std::endl
              << "System Page Size: " << MemInfo::get_page_size() << std::endl
              << "Mem Limit: " << MemInfo::mem_limit_str() << std::endl
              << "Soft Mem Limit: " << MemInfo::soft_mem_limit_str() << std::endl
              << "System Mem Available Low Water Mark: "
              << PrettyPrinter::print(MemInfo::sys_mem_available_low_water_mark(), TUnit::BYTES)
              << std::endl
              << "System Mem Available Warning Water Mark: "
              << PrettyPrinter::print(MemInfo::sys_mem_available_warning_water_mark(), TUnit::BYTES)
              << std::endl
              << "Cgroup Mem Limit: "
              << PrettyPrinter::print(MemInfo::cgroup_mem_limit(), TUnit::BYTES) << std::endl
              << "Cgroup Mem Usage: "
              << PrettyPrinter::print(MemInfo::cgroup_mem_usage(), TUnit::BYTES) << std::endl
              << "Cgroup Mem Refresh State: " << MemInfo::cgroup_mem_refresh_state() << std::endl
              << "\n---\n\n";

    (*output) << "<h4 id=\"memoryOptionSettingsTitle\">Memory Option Settings</h4>\n";
    {
        std::lock_guard<std::mutex> lock(*config::get_mutable_string_config_lock());
        for (const auto& it : *(config::full_conf_map)) {
            if (it.first.find("memory") != std::string::npos ||
                it.first.find("cache") != std::string::npos ||
                it.first.find("mem") != std::string::npos) {
                (*output) << it.first << "=" << it.second << std::endl;
            }
        }
    }
    (*output) << "\n---\n\n";

    (*output) << "<h4 id=\"jemallocProfilesTitle\">Jemalloc Profiles</h4>\n";
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
    (*output) << tmp;
#else
    char buf[2048];
    MallocExtension::instance()->GetStats(buf, 2048);
    // Replace new lines with <br> for html
    std::string tmp(buf);
    boost::replace_all(tmp, "\n", "<br>");
    (*output) << tmp;
#endif
    (*output) << "</pre>";
}

// Registered to handle "/profile".
void process_profile_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output) {
    (*output) << "<h4>Copy Process Profile To Clipboard (拷贝 Process Profile 到剪切板) </h4>";
    (*output) << "<button id=\"copyToClipboard\">Copy Page Text</button>" << std::endl;
    (*output) << "<script>" << std::endl;
    (*output) << "$('#copyToClipboard').click(function () {" << std::endl;
    // create a hidden textarea element
    (*output) << "     var textarea = document.createElement('textarea');" << std::endl;
    (*output) << "     textarea.style.position = 'absolute';" << std::endl;
    (*output) << "     textarea.style.left = '-9999px';" << std::endl;
    // get the content to copy
    (*output) << "     var contentToCopy = document.getElementById('allPageText').innerHTML;"
              << std::endl;
    (*output) << "     textarea.value = contentToCopy;"
              << std::endl; // set the content to the textarea
    (*output) << "     document.body.appendChild(textarea);" << std::endl;
    (*output) << "     textarea.select();" << std::endl;
    (*output) << "     textarea.setSelectionRange(0, 99999);"
              << std::endl; // compatible with mobile devices
    (*output) << "try {" << std::endl;
    (*output) << "          document.execCommand('copy');"
              << std::endl; //copy the selected text to the clipboard
    (*output) << "          alert('Process profile copied to clipboard!');" << std::endl;
    (*output) << "      } catch (err) {" << std::endl;
    (*output) << "          alert('Failed to copy process profile! ' + err);" << std::endl;
    (*output) << "      }" << std::endl;
    (*output) << "});" << std::endl;
    (*output) << "</script>" << std::endl;

    doris::ProcessProfile::instance()->refresh_profile();

    (*output) << "<div id=\"allPageText\">" << std::endl;
    (*output) << "<h2 id=\"processProfileTitle\">Process Profile</h2>" << std::endl;
    (*output) << "<pre id=\"processProfile\">"
              << doris::ProcessProfile::instance()->print_process_profile_no_root() << "</pre>"
              << "\n\n---\n\n";
    memory_info_handler(output);

    // TODO, expect more information about process status, CPU, IO, etc.

    (*output) << "</div>" << std::endl;
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
    (*output) << "<h2>mem_tracker webpage has been offline, please click <a "
                 "href=../profile>Process Profile</a>, see MemoryProfile and Memory Info</h2>\n";
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
    (*output) << "    pprof --text be/lib/doris_be perf.data" << std::endl;
    (*output) << std::endl;
    (*output) << "If you want to get the flame graph, you must first make sure that there is a "
                 "'perf' command on the host machine."
              << std::endl;
    (*output) << "And you need to download the FlameGraph and place it under 'be/tools/FlameGraph'."
              << std::endl;
    (*output) << "Finally, check if the following files exist. And should be executable."
              << std::endl;
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
    (*output) << "        timeout: 120000," << std::endl;
    (*output) << "        success: function (result) {" << std::endl;
    (*output) << "            document.getElementById(\"cpuContent\").innerText = result;"
              << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "        error: function (result) {" << std::endl;
    (*output) << "            alert(JSON.stringify(result));" << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "    });" << std::endl;
    (*output) << "});" << std::endl;

    // for graph profile
    (*output) << "$('#getCpuGraph').click(function () {" << std::endl;
    (*output) << "    document.getElementById(\"cpuContent\").innerText = \"Sampling... (30 "
                 "seconds)\";"
              << std::endl;
    (*output) << "    $.ajax({" << std::endl;
    (*output) << "        type: \"GET\"," << std::endl;
    (*output) << "        dataType: \"text\"," << std::endl;
    (*output) << "        url: \"pprof/profile?type=flamegraph\"," << std::endl;
    (*output) << "        timeout: 120000," << std::endl;
    (*output) << "        success: function (result) {" << std::endl;
    (*output) << "            document.getElementById(\"cpuContent\").innerHTML = result;"
              << std::endl;
    (*output) << "        }" << std::endl;
    (*output) << "        ," << std::endl;
    (*output) << "        error: function (result) {" << std::endl;
    (*output) << "            alert(JSON.stringify(result));" << std::endl;
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
    web_page_handler->register_page("/profile", "Process Profile", process_profile_handler,
                                    true /* is_on_nav_bar */);
    web_page_handler->register_page("/mem_tracker", "MemTracker", mem_tracker_handler,
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
