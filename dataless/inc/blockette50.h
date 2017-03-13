#ifndef _Blockette50_H_
#define _Blockette50_H_

class Blockette50 : Blockette {
public:
    std::string station_call_letters;
    double latitude;
    double longitude;
    double elevation;
    int number_of_channels;
    int number_of_station_comments;
    int network_identifier_code;
    int 32_bit_word_order;
    int 16_bit_word_order;
    
};

#endif  //_Blockette50_H_