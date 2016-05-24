#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}

struct Blkt {
    int type;
    int index;
};

struct DataRecord {
    int index;
    int sequence_number;
    std::vector<Blkt> blockettes;
};

std::ostream & operator<<(std::ostream & os, Blkt const & blkt) {
    os << blkt.type;
    
    return os;
}

std::ostream & operator<<(std::ostream & os, DataRecord const & dr) {
    os << "Seq#: " << dr.sequence_number << " [" << dr.index << "]";

    for (auto b : dr.blockettes) {
        os << " [Blkt "<< b << "]";
    }

    return os;
}

int main() {
    std::ifstream seedfile("/msd/IU_HRV/2015/180/00_LHZ.512.seed");
    int seedfile_length = filesize("/msd/IU_HRV/2015/180/00_LHZ.512.seed");
    std::ofstream output("output.txt", std::ios::trunc);
    
    //put the data into a vector for indexing
    std::vector<unsigned char> data;
    data.reserve(seedfile_length);
    for (int i = 0; i < seedfile_length; ++i) {
        data.push_back(seedfile.get());
    }
    
    std::vector<DataRecord> station_day;
    int nbbn;
    int next_blockette_index;
    
    for (int i = 0; i < seedfile_length; i += 512) {
        DataRecord rec;
        rec.index = i;
        
        std::string str_sequence_number(std::begin(data) + i, std::begin(data) + i + 6);
        rec.sequence_number = std::stoi(str_sequence_number);
        //there is always at least one blockette, usually 1000
        nbbn = 48;
        
        while (nbbn != 0) {
            Blkt blockette;
            blockette.index = nbbn;
            blockette.type = (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]);
            rec.blockettes.push_back(blockette);
            
            nbbn = (static_cast<int>(data[i + nbbn + 2]) << 8) + static_cast<int>(data[i + nbbn + 3]);
            if (blockette.type != 1000 and blockette.type != 1001) {
                std::cout << rec << std::endl;
            }
        }
        
        
        //loop through the blockettes
        //initialize the next_blockette_index to the end of the Fixed Section of Data Header
        // bool more_blockettes = true;
        // while (more_blockettes) {
        //     Blkt blockette;
        //     blockette.index = i + nbbn;
        //     blockette.type = (static_cast<int>(data[i + nbbn]) << 8) + static_cast<int>(data[i + nbbn + 1]);
        //     rec.blockettes.push_back(blockette);
        //
        //
        //     if (rec.sequence_number == 30929) {
        //         std::cout << blockette.type << " next at byte " << nbbn << std::endl;
        //         std::cout << std::endl << std::endl << rec.sequence_number << std::endl;
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

        station_day.push_back(rec);
        // std::cout << rec << std::endl;
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