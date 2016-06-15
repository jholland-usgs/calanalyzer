#include "calibration.h"

Calibration::Calibration() {
    std::cout << "YYY" << std::endl;
}

Calibration::Calibration(std::vector<unsigned char> & data, int offset) {
    std::cout << "XXX" << std::endl;
    // for (int i = 0; i < 60; i++) {
    //     std::cout << "[" << i << "]\t";
    //     std::cout << std::hex << std::showbase;
    //     std::cout << static_cast<int>(data[offset + i]) << "\t" << data[offset + i] << std::endl;
    //     std::cout << std::dec << std::noshowbase;
    // }
}