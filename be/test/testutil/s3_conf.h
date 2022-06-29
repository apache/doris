#include <cstdlib>
#include <sstream>
#include <string>

namespace doris {

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;

    void init_from_env();

    std::string to_string() const;
};

inline void S3Conf::init_from_env() {
    char* var = std::getenv("S3_AK");
    ak = var ? var : "";
    var = std::getenv("S3_SK");
    sk = var ? var : "";
    var = std::getenv("S3_ENDPOINT");
    endpoint = var ? var : "";
    var = std::getenv("S3_REGION");
    region = var ? var : "";
    var = std::getenv("S3_BUCKET");
    bucket = var ? var : "";
    var = std::getenv("S3_PREFIX");
    prefix = var ? var : "";
}

inline std::string S3Conf::to_string() const {
    std::stringstream ss;
    ss << "ak: " << ak << ", sk: " << sk << ", endpoint: " << endpoint << ", region: " << region
       << ", bucket: " << bucket << ", prefix: " << prefix;
    return ss.str();
}

} // namespace doris
