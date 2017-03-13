#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "calibration.h"

//finds and returns the filesize of the file in question
std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}

struct Blockette {
    int type;
    int index;
};

struct DataRecord {
    int index;
    int sequence_number;
    std::vector<Blockette> blockettes;
};

struct Calibration300 {
    int type;
    int cal_nbbn;
    
};

//overloaded operator to print out pertinent information for each Blockette struct
std::ostream & operator<<(std::ostream & os, Blockette const & blkt) {
    os << blkt.type;
    
    return os;
}

//overloaded operator to print out pertinent information for each DataRecord struct
std::ostream & operator<<(std::ostream & os, DataRecord const & dr) {
    os << "Seq# " << dr.sequence_number << " [byte " << dr.index << "]";

    for (auto b : dr.blockettes) {
        os << " [blkt "<< b << "]";
    }

    return os;
}

int main() {
    std::string filename = "/Users/ambaker/Documents/seed/IU_HRV/2015/180/00_LHZ.512.seed";
    std::ifstream seedfile(filename);
    int seedfile_length = filesize(filename.c_str());
    std::ofstream output("output.txt", std::ios::trunc);
    
    //put the data into a vector for indexing
    std::vector<unsigned char> data;
    data.reserve(seedfile_length);
    for (int i = 0; i < seedfile_length; ++i) {
        data.push_back(seedfile.get());
    }
    
    //a vector to hold each record
    std::vector<DataRecord> station_day;
    //an integer to hold the next blockette byte number, used for indexing
    int nbbn;
    
    //TODO: change from 512 to a variable derived from the filename, or arguments
    for (int i = 0; i < seedfile_length; i += 512) {

        // std::cout << "Trigger" << std::endl;
        DataRecord data_record;
        data_record.index = i;
        
        //parse the sequence number of the record
        std::string str_sequence_number(std::begin(data) + i, std::begin(data) + i + 6);
        data_record.sequence_number = std::stoi(str_sequence_number);
        
        //initialize the next blockette byte number to the end of the Fixed Section of Data Header
        nbbn = 48;
        
        //while there is a blockette present after the current
        while (nbbn != 0) {
            Blockette blockette;
            blockette.index = nbbn;
            blockette.type = (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]);
            data_record.blockettes.push_back(blockette);
            
            //detect the desired cals
            if (blockette.type == 300) {
                // Calibration cal(data, (i + nbbn));
                Calibration cal = Calibration();
                std::cout << std::endl << "CALIBRATION 300 BLOCKETTE" << std::endl;
                // Calibration calibration(data, i + nbbn);
                // for (int j = 0; j < 60; j++) {
                //     std::cout << "[" << j << "]\t";
                //     std::cout << std::hex << std::showbase;
                //     std::cout << static_cast<int>(data[i + nbbn + j]) << "\t" << data[i + nbbn + j] << std::endl;
                //     std::cout << std::dec << std::noshowbase;
                // }
                //B300F01 - blockette type
                std::cout << "  Blockette Type: ";
                std::cout << (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]) << std::endl;
                
                //B300F02 - next blockette's byte number
                std::cout << " Next blkt index: ";
                std::cout << (static_cast<int>(data[i + nbbn + 2]) << 8) + static_cast<int>(data[i + nbbn + 3]) << std::endl;
                
                //B300F03 - beginning of calibration time
                std::cout << "Calibration time: " << std::setfill('0')
                    << (static_cast<int>(data[i + nbbn + 4]) << 8) + static_cast<int>(data[i + nbbn + 5]) << "," << std::setw(3)    //year
                    << (static_cast<int>(data[i + nbbn + 6]) << 8) + static_cast<int>(data[i + nbbn + 7]) << "T" << std::setw(2)    //jday (001 - 366)
                    << static_cast<int>(data[i + nbbn + 8]) << ":" << std::setw(2)   //hour (0 - 23)
                    << static_cast<int>(data[i + nbbn + 9]) << ":" << std::setw(2)   //minute (0 - 60)
                    << (static_cast<int>(data[i + nbbn + 10]) << 8) + static_cast<int>(data[i + nbbn + 11]) << "." << std::setw(4)   //second (0 - 60)
                    << (static_cast<int>(data[i + nbbn + 12]) << 8) + static_cast<int>(data[i + nbbn + 13]) << std::endl;   //second (0 - 60)
                
                //B300F04 - number of step calibrations
                std::cout << "No Step Calibras: ";
                std::cout << static_cast<int>(data[i + nbbn + 14]) << std::endl;
                
                //B300F05 - calibration flags
                std::cout << "Calibration flag: ";
                std::cout << std::bitset<8>(data[i + nbbn + 15]) << std::endl;  //big endian
                
                //B300F06 - step duration
                std::cout << "   Step duration: ";
                std::cout << (static_cast<int>(data[i + nbbn + 16]) << 24) +
                    (static_cast<int>(data[i + nbbn + 17]) << 16) +
                    (static_cast<int>(data[i + nbbn + 18]) << 8) +
                    static_cast<int>(data[i + nbbn + 19])
                        << std::endl;
                
                //B300F07 - interval duration
                std::cout << "Intervl duration: ";
                std::cout << (static_cast<int>(data[i + nbbn + 20]) << 24) +
                    (static_cast<int>(data[i + nbbn + 21]) << 16) +
                    (static_cast<int>(data[i + nbbn + 22]) << 8) +
                    static_cast<int>(data[i + nbbn + 23])
                        << std::endl;
                
                //B300F08 - calibration signal amplitude
                std::cout << "Cal signal ampli: ";
                // std::cout << std::hex << std::showbase;
                // std::cout << (static_cast<int>(data[i + nbbn + 24]) << 24) +
                //     (static_cast<int>(data[i + nbbn + 25]) << 16) +
                //     (static_cast<int>(data[i + nbbn + 26]) << 8) +
                //     static_cast<int>(data[i + nbbn + 27]) << std::endl;
                // std::cout << std::dec << std::noshowbase;
                std::cout << std::hex << std::showbase;
                std::cout << static_cast<int>(data[i + nbbn + 24]) << " "
                    << static_cast<int>(data[i + nbbn + 25]) << " "
                    << static_cast<int>(data[i + nbbn + 26]) << " "
                    << static_cast<int>(data[i + nbbn + 27])
                        << std::endl;
                std::cout << std::dec << std::noshowbase;
                
                //B300F09 - channel with calibration output
                std::cout << "  Output channel: ";
                std::cout << data[i + nbbn + 28];
                std::cout << data[i + nbbn + 29];
                std::cout << data[i + nbbn + 30];
                std::cout << std::endl;
                
                //B300F10 - reserved byte
                std::cout << "   Reserved byte: ";
                std::cout << static_cast<int>(data[i + nbbn + 31]) << std::endl;
                
                //B300F11 - reference amplitude
                std::cout << "Reference amplit: ";
                std::cout << (static_cast<int>(data[i + nbbn + 32]) << 24) +
                    (static_cast<int>(data[i + nbbn + 33]) << 16) +
                    (static_cast<int>(data[i + nbbn + 34]) << 8) +
                    static_cast<int>(data[i + nbbn + 35]) << std::endl;
                
                //B300F12 - coupling
                std::cout << "        Coupling: ";
                std::string str_coupling(std::begin(data) + i + nbbn + 36, std::begin(data) + i + nbbn + 47);
                std::cout << str_coupling << std::endl;
                // std::cout << std::string(std::begin(data) + i + nbbn + 436, std::begin(data) + i + nbbn + 47) << std::endl;
                
                //B300F13 - rolloff
                std::cout << "         Rolloff: ";
                std::string str_rolloff(std::begin(data) + i + nbbn + 48, std::begin(data) + i + nbbn + 59);
                std::cout << str_rolloff << std::endl;
                // std::cout << std::string(std::begin(data) + i + nbbn + 48, std::begin(data) + i + nbbn + 59) << std::endl;

            } else 
            if (blockette.type == 310) {
                // Calibration cal = Calibration();
                std::cout << std::endl << "CALIBRATION 310 BLOCKETTE" << std::endl;

                //B310F01 - blockette type
                std::cout << "  Blockette Type: ";
                std::cout << (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]) << std::endl;
                
                //B310F02 - next blockette's byte number
                std::cout << " Next blkt index: ";
                std::cout << (static_cast<int>(data[i + nbbn + 2]) << 8) + static_cast<int>(data[i + nbbn + 3]) << std::endl;
                
                //B310F03 - beginning of calibration time
                std::cout << "Calibration time: " << std::setfill('0')
                    << (static_cast<int>(data[i + nbbn + 4]) << 8) + static_cast<int>(data[i + nbbn + 5]) << "," << std::setw(3)    //year
                    << (static_cast<int>(data[i + nbbn + 6]) << 8) + static_cast<int>(data[i + nbbn + 7]) << "T" << std::setw(2)    //jday (001 - 366)
                    << static_cast<int>(data[i + nbbn + 8]) << ":" << std::setw(2)   //hour (0 - 23)
                    << static_cast<int>(data[i + nbbn + 9]) << ":" << std::setw(2)   //minute (0 - 60)
                    << (static_cast<int>(data[i + nbbn + 10]) << 8) + static_cast<int>(data[i + nbbn + 11]) << "." << std::setw(4)   //second (0 - 60)
                    << (static_cast<int>(data[i + nbbn + 12]) << 8) + static_cast<int>(data[i + nbbn + 13]) << std::endl;   //second (0 - 60)
                
                //B310F04 - Reserved byte
                std::cout << "   Reserved byte: ";
                std::cout << static_cast<int>(data[i + nbbn + 14]) << std::endl;
                
                //B310F05 - calibration flags
                std::cout << "Calibration flag: ";
                std::cout << std::bitset<8>(data[i + nbbn + 15]) << std::endl;  //big endian
                
                //B310F06 - calibration duration
                std::cout << "  Calib duration: ";
                std::cout << (static_cast<int>(data[i + nbbn + 16]) << 24) +
                    (static_cast<int>(data[i + nbbn + 17]) << 16) +
                    (static_cast<int>(data[i + nbbn + 18]) << 8) +
                    static_cast<int>(data[i + nbbn + 19])
                        << std::endl;
                
                //B310F07 - period of signal (seconds)
                std::cout << "Period of signal: ";
                std::cout << std::hex << std::showbase;
                std::cout << static_cast<int>(data[i + nbbn + 20]) << " " <<
                    static_cast<int>(data[i + nbbn + 21]) << " " <<
                    static_cast<int>(data[i + nbbn + 22]) << " " <<
                    static_cast<int>(data[i + nbbn + 23]) << std::endl;
                std::cout << std::dec << std::noshowbase;
                
                //B310F08 - amplitude of signal
                std::cout << "Signal amplitude: ";
                std::cout << std::hex << std::showbase;
                std::cout << static_cast<int>(data[i + nbbn + 24]) << " " <<
                    static_cast<int>(data[i + nbbn + 25]) << " " <<
                    static_cast<int>(data[i + nbbn + 26]) << " " <<
                    static_cast<int>(data[i + nbbn + 27]) << std::endl;
                std::cout << std::dec << std::noshowbase;

                //B300F09 - channel with calibration output
                std::cout << "  Output channel: ";
                std::cout << data[i + nbbn + 28];
                std::cout << data[i + nbbn + 29];
                std::cout << data[i + nbbn + 30];
                std::cout << std::endl;
                
                //B310F10 - reserved byte
                std::cout << "   Reserved byte: ";
                std::cout << static_cast<int>(data[i + nbbn + 31]) << std::endl;
                
                //B310F11 - reference amplitude
                std::cout << "Reference amplit: ";
                std::cout << (static_cast<int>(data[i + nbbn + 32]) << 24) +
                    (static_cast<int>(data[i + nbbn + 33]) << 16) +
                    (static_cast<int>(data[i + nbbn + 34]) << 8) +
                    static_cast<int>(data[i + nbbn + 35])
                        << std::endl;
                
                //B310F12 - coupling
                std::cout << "        Coupling: ";
                std::string str_coupling(std::begin(data) + i + nbbn + 36, std::begin(data) + i + nbbn + 47);
                std::cout << str_coupling << std::endl;
                
                //B310F13 - rolloff
                std::cout << "         Rolloff: ";
                std::string str_rolloff(std::begin(data) + i + nbbn + 48, std::begin(data) + i + nbbn + 59);
                std::cout << str_rolloff << std::endl;

            } else
            if (blockette.type == 320) {
                // Calibration cal = Calibration();
                std::cout << std::endl << "CALIBRATION 320 BLOCKETTE" << std::endl;

                //B320F01 - blockette type
                std::cout << "  Blockette Type: ";
                std::cout << (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]) << std::endl;
                
                //B320F02 - next blockette's byte number
                std::cout << " Next blkt index: ";
                std::cout << (static_cast<int>(data[i + nbbn + 2]) << 8) + static_cast<int>(data[i + nbbn + 3]) << std::endl;
                
                //B320F03 - beginning of calibration time
                std::cout << "Calibration time: " << std::setfill('0')
                    << (static_cast<int>(data[i + nbbn + 4]) << 8) + static_cast<int>(data[i + nbbn + 5]) << "," << std::setw(3)    //year
                    << (static_cast<int>(data[i + nbbn + 6]) << 8) + static_cast<int>(data[i + nbbn + 7]) << "T" << std::setw(2)    //jday (001 - 366)
                    << static_cast<int>(data[i + nbbn + 8]) << ":" << std::setw(2)   //hour (0 - 23)
                    << static_cast<int>(data[i + nbbn + 9]) << ":" << std::setw(2)   //minute (0 - 60)
                    << (static_cast<int>(data[i + nbbn + 10]) << 8) + static_cast<int>(data[i + nbbn + 11]) << "." << std::setw(4)   //second (0 - 60)
                    << (static_cast<int>(data[i + nbbn + 12]) << 8) + static_cast<int>(data[i + nbbn + 13]) << std::endl;   //second (0 - 60)
                
                //B320F04 - Reserved byte
                std::cout << "   Reserved byte: ";
                std::cout << static_cast<int>(data[i + nbbn + 14]) << std::endl;
                
                //B320F05 - calibration flags
                std::cout << "Calibration flag: ";
                std::cout << std::bitset<8>(data[i + nbbn + 15]) << std::endl;  //big endian
                
                //B320F06 - calibration duration
                std::cout << "  Calib duration: ";
                std::cout << (static_cast<int>(data[i + nbbn + 16]) << 24) +
                    (static_cast<int>(data[i + nbbn + 17]) << 16) +
                    (static_cast<int>(data[i + nbbn + 18]) << 8) +
                    static_cast<int>(data[i + nbbn + 19])
                        << std::endl;
                
                //B320F07 - peak to peak amplitude of signal
                std::cout << "   P2P amplitude: ";
                std::cout << std::hex << std::showbase;
                std::cout << static_cast<int>(data[i + nbbn + 20]) << " " <<
                    static_cast<int>(data[i + nbbn + 21]) << " " <<
                    static_cast<int>(data[i + nbbn + 22]) << " " <<
                    static_cast<int>(data[i + nbbn + 23]) << std::endl;
                std::cout << std::dec << std::noshowbase;
                
                //B320F08 - channel with calibration output
                std::cout << "  Output channel: ";
                std::cout << data[i + nbbn + 24];
                std::cout << data[i + nbbn + 25];
                std::cout << data[i + nbbn + 26];
                std::cout << std::endl;
                
                //B320F09 - reserved byte
                std::cout << "   Reserved byte: ";
                std::cout << static_cast<int>(data[i + nbbn + 27]) << std::endl;
                
                //B320F10 - reference amplitude
                std::cout << "Reference amplit: ";
                std::cout << (static_cast<int>(data[i + nbbn + 28]) << 24) +
                    (static_cast<int>(data[i + nbbn + 29]) << 16) +
                    (static_cast<int>(data[i + nbbn + 30]) << 8) +
                    static_cast<int>(data[i + nbbn + 31])
                        << std::endl;
                
                //B320F11 - coupling
                std::cout << "        Coupling: ";
                std::string str_coupling(std::begin(data) + i + nbbn + 32, std::begin(data) + i + nbbn + 43);
                std::cout << str_coupling << std::endl;
                
                //B320F12 - rolloff
                std::cout << "         Rolloff: ";
                std::string str_rolloff(std::begin(data) + i + nbbn + 44, std::begin(data) + i + nbbn + 55);
                std::cout << str_rolloff << std::endl;
                
                //B320F13 - noise type
                std::cout << "      Noise type: ";
                std::string str_noise_type(std::begin(data) + i + nbbn + 56, std::begin(data) + i + nbbn + 64);
                std::cout << str_noise_type << std::endl;

            }
            
            //find the current blockette's Field 02, next blockette's byte number
            nbbn = (static_cast<int>(data[i + nbbn + 2]) << 8) + static_cast<int>(data[i + nbbn + 3]);
        }

        station_day.push_back(data_record);
    }
    
    for (auto const & r : station_day) {
        output << r << std::endl;
    }
    
    seedfile.close();
    output.close();
    
    return 0;
}