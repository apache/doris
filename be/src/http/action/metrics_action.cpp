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

#include "http/action/metrics_action.h"

#include <string>

#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/webserver.h"
#include "runtime/exec_env.h"
#include "util/metrics.h"

namespace palo {

class PrometheusMetricsVisitor : public MetricsVisitor {
public:
    virtual ~PrometheusMetricsVisitor() {}
    void visit(const std::string& prefix, const std::string& name,
               MetricCollector* collector) override;
    std::string to_string() const { return _ss.str(); }
private:
    void _visit_simple_metric(
        const std::string& name, const MetricLabels& labels, SimpleMetric* metric);
private:
    std::stringstream _ss;
};

void PrometheusMetricsVisitor::visit(const std::string& prefix,
                                     const std::string& name,
                                     MetricCollector* collector) {
    if (collector->empty() || name.empty()) {
        return;
    }
    std::string metric_name;
    if (prefix.empty()) {
        metric_name = name;
    } else {
        metric_name = prefix + "_" + name;
    }
    // Output metric type
    _ss << "# TYPE " << metric_name << " " << collector->type() << "\n";
    switch (collector->type()) {
    case MetricType::COUNTER:
    case MetricType::GAUGE:
        for (auto& it : collector->metrics()) {
            _visit_simple_metric(metric_name, it.first, (SimpleMetric*)it.second);
        }
        break;
    default:
        break;
    }
}

void PrometheusMetricsVisitor::_visit_simple_metric(
        const std::string& name, const MetricLabels& labels, SimpleMetric* metric) {
    _ss << name;
    // labels
    if (!labels.empty()) {
        _ss << "{";
        int i = 0;
        for (auto& label : labels.labels) {
            if (i++ > 0) {
                _ss << ",";
            }
            _ss << label.name << "=\"" << label.value << "\"";
        }
        _ss << "}";
    }
    _ss << " " << metric->to_string() << "\n";
}

void MetricsAction::handle(HttpRequest* req, HttpChannel* channel) {
    PrometheusMetricsVisitor visitor;
    _metrics->collect(&visitor);
    std::string str = visitor.to_string();
    HttpResponse response(HttpStatus::OK, "text/plain; version=0.0.4", &str);
    channel->send_response(response);
}

}
