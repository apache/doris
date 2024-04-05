#include "http/http_handler.h"

namespace doris {

class InjectionPointAction : public HttpHandler {
public:
    InjectionPointAction();

    ~InjectionPointAction() override = default;

    void handle(HttpRequest* req) override;
};

} // namespace doris
