#pragma once

#include <vector.h>

#include <memory>
#include <mutex>

#include "http/http_client.h"

namespace doris::io {
class HttpClientCache {
public:
    std::unique_ptr<HttpClient> GetClient() {
        std::lock_guard<std::mutex> lck(lock);
        if (clients.size() == 0) {
            return nullptr;
        }

        auto client = std::move(clients.back());
        clients.pop_back();
        return client;
    }
    void StoreClient(std::unique_ptr<HttpClient> client) { clients.push_back(client); }

protected:
    std::vector<std::unique_ptr<HttpClient>> clients;
    std::mutex lock;
};
} // namespace doris::io
