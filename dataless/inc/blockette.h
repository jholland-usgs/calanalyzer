#ifndef _Blockette_H_
#define _Blockette_H_

#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

class Blockette {
public:
    int type;
    int length;
    
    Blockette();
    Blockette(std::vector<unsigned char> &dataless, int index);
    void Parse();
    std::string get_value(std::vector<unsigned char> &dataless, int *index, int length);
};

#endif  //_Blockette_H_