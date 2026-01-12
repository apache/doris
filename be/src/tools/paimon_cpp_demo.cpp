// Minimal Paimon C++ link/ABI check for Doris BE.
#include <iostream>

#include "paimon/status.h"

int main() {
    auto status = paimon::Status::OK();
    std::cout << "Paimon C++ OK: " << status.ToString() << std::endl;
    return 0;
}
