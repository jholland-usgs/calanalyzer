#ifndef _Calibration_H_
#define _Calibration_H_

#include <iostream>
#include <string>
#include <vector>

class Calibration {
protected:
    int type;
    int cal_nbbn;
    
public:
    Calibration(); 
    Calibration(std::vector<unsigned char> & data, int offset);
};

#endif  //_Calibration_H_