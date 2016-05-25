#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

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
    std::string filename = "/msd/IU_HRV/2015/180/00_LHZ.512.seed";
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
            
            //find the current blockette's Field 02, next blockette's byte number
            nbbn = (static_cast<int>(data[i + nbbn + 2]) << 8) + static_cast<int>(data[i + nbbn + 3]);
            
            if (blockette.type == 300) {
                
            } else 
            if (blockette.type == 310) {
                
            } else
            if (blockette.type == 320) {
                
            }
        }
        
        
        //loop through the blockettes
        //initialize the next_blockette_index to the end of the Fixed Section of Data Header
        // bool more_blockettes = true;
        // while (more_blockettes) {
        //     Blockette blockette;
        //     blockette.index = i + nbbn;
        //     blockette.type = (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]);
        //     data_record.blockettes.push_back(blockette);
        //
        //
        //     if (data_record.sequence_number == 30929) {
        //         std::cout << blockette.type << " next at byte " << nbbn << std::endl;
        //         std::cout << std::endl << std::endl << data_record.sequence_number << std::endl;
        //         std::vector<unsigned char> record_vector(std::begin(data) + i, std::begin(data) + i + 512);
        //         std::cout << std::endl;
        //         int counter = 0;
        //         for (auto & j : record_vector) {
        //             std::cout << std::endl << "[" << counter << "]";
        //             std::cout << std::hex;
        //             std::cout << "\tX";
        //             std::cout << static_cast<int>(j);
        //             std::cout << std::dec;
        //             std::cout << "\tR" << static_cast<int>(j);
        //             counter++;
        //         }
        //         std::cout << std::endl;
        //     }
        //     next_blockette_index = (static_cast<int>(data[i + nbbn + 2]) << 8) +
        //         static_cast<int>(data[i + nbbn + 3]);
        //     // std::cout << std::endl << "NBI: " << next_blockette_index << std::endl;
        //     nbbn = next_blockette_index;
        //     more_blockettes = (next_blockette_index != 0);
        //     std::cout << blockette.type << " next at byte " << nbbn << std::endl;
        //
        // }

        station_day.push_back(data_record);
        // std::cout << data_record << std::endl;
        //
        // std::cout << "\t";
        // for (int j = 48; j < 48 + 2; j++) {
        //     std::cout << data[i + j];
        // }
        // std::cout << std::endl;
        // std::cout << data[cursor + 1] << data[cursor + 2] << " ";
    }
    
    for (auto const & r : station_day) {
        output << r << std::endl;
    }
    // output << std::hex << std::showbase;
    // int counter = 1;
    // for (auto d : data) {
    //     output << std::dec << std::noshowbase;
    //     output << "[" << counter << "]";
    //     output << std::hex << std::showbase;
    //     output << "\t" << static_cast<int>(d) << d << std::endl;
    //     counter++;
    // }
    //
    // output << std::dec << std::noshowbase;
    
    seedfile.close();
    output.close();
    
    return 0;
}