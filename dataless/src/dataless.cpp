#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "date.h"

std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}

std::string get_value(std::vector<unsigned char> &dataless, int *index, int length) {
//    std::cout << "[";
//    std::cout << std::hex << std::showbase;
//    for (int i = 0; i < length; i++) {
//        std::cout << dataless[*index + i];
//    }
//    std::cout << std::dec << std::noshowbase;
//    std::cout << "]";
    std::string value(std::begin(dataless) + *index, std::begin(dataless) + *index + length);
    *index += length;
    return value;
}

std::string get_value_until_tilde(std::vector<unsigned char> &dataless, int *index) {
    //returns the value from the starting index to the tilde character
    std::stringstream value;
    while (true) {
        char c = dataless[*index];
        *index += 1;
        if (c != '~') {
            value << c;
        }
        else {
            break;
        }
    }
    
    return value.str();
}

void parse_blockette_50(std::vector<unsigned char> &dataless, int *index) {
    //parses blockette 50 for the fields
    //B50F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B50F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B50F03 - station call letters
    std::string str_station_call_letters = get_value(dataless, index, 5);
    std::cout << "   station: " << str_station_call_letters << std::endl;
    
    //B50F04 - latitude
    std::string str_latitude = get_value(dataless, index, 10);
    std::cout << "  latitude: " << str_latitude << std::endl;
    
    //B50F05 - longitude
    std::string str_longitude = get_value(dataless, index, 11);
    std::cout << " longitude: " << str_longitude << std::endl;
    
    //B50F06 - elevation
    std::string str_elevation = get_value(dataless, index, 7);
    std::cout << " elevation: " << str_elevation << std::endl;
    
    //B50F07 - number of channels
    std::string str_number_of_channels = get_value(dataless, index, 4);
    std::cout << "channels #: " << str_number_of_channels << std::endl;
    
    //B50F08 - number of station comments
    std::string str_number_of_station_comments = get_value(dataless, index, 3);
    std::cout << "sta cmts #: " << str_number_of_station_comments << std::endl;
    
    //B50F09 - site name
    std::string str_site_name = get_value_until_tilde(dataless, index);
    std::cout << " site name: " << str_site_name << std::endl;
    
    //B50F10 - network identifier code
    std::string str_network_identifier_code = get_value(dataless, index, 3);
    std::cout << "network id: " << str_network_identifier_code << std::endl;
    
    //B50F11 - 32 bit word order
    std::string str_32_bit_word_order = get_value(dataless, index, 4);
    std::cout << " 32 bit wo: " << str_32_bit_word_order << std::endl;
    
    //B50F12 - 16 bit word order
    std::string str_16_bit_word_order = get_value(dataless, index, 2);
    std::cout << " 16 bit wo: " << str_16_bit_word_order << std::endl;
    
    //B50F13 - start effective date
    std::string str_start_effective_date = get_value_until_tilde(dataless, index);
    std::cout << "start date: " << str_start_effective_date << std::endl;
    
    //B50F14 - end effective date
    std::string str_end_effective_date = get_value_until_tilde(dataless, index);
    std::cout << "  end date: " << str_end_effective_date << std::endl;
    
    //B50F15 - update flag
    std::string str_update_flag = get_value(dataless, index, 1);
    std::cout << "update flg: " << str_update_flag << std::endl;
    
    //B50F16 - network code
    std::string str_network_code = get_value(dataless, index, 2);
    std::cout << "  net code: " << str_network_code << std::endl;
}

void parse_blockette_51(std::vector<unsigned char> &dataless, int *index) {
    //parses blockette 51 for the fields
    //B51F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B51F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B51F03 - beginning effective time
    std::string str_beginning_effective_time = get_value_until_tilde(dataless, index);
    std::cout << "begin time: " << str_beginning_effective_time << std::endl;
    
    //B51F04 - end effective time
    std::string str_end_effective_time = get_value_until_tilde(dataless, index);
    std::cout << "  end time: " << str_end_effective_time << std::endl;
    
    //B51F05 - comment code key
    std::string str_comment_code_key = get_value(dataless, index, 4);
    std::cout << "  cmt code: " << str_comment_code_key << std::endl;
    
    //B51F06 - comment level
    std::string str_comment_level = get_value(dataless, index, 6);
    std::cout << "   cmt lvl: " << str_comment_level << std::endl;
}

void parse_blockette_52(std::vector<unsigned char> &dataless, int *index) {
    //B52F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B52F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B52F03 - location identifier
    std::string str_location_identifier = get_value(dataless, index, 2);
    std::cout << "  location: " << str_location_identifier << std::endl;
    
    //B52F04 - channel identifier
    std::string str_channel_identifier = get_value(dataless, index, 3);
    std::cout << "   channel: " << str_channel_identifier << std::endl;
    
    //B52F05 - subchannel identifier
    std::string str_subchannel_identifier = get_value(dataless, index, 4);
    std::cout << "subchannel: " << str_subchannel_identifier << std::endl;
    
    //B52F06 - instrument identifier
    std::string str_instrument_identifier = get_value(dataless, index, 3);
    std::cout << "instrument: " << str_instrument_identifier << std::endl;
    
    //B52F07 - optional comment
    std::string str_optional_comment = get_value_until_tilde(dataless, index);
    std::cout << "   opt cmt: " << str_optional_comment << std::endl;
    
    //B52F08 - units of signal response
    std::string str_units_of_signal_response = get_value(dataless, index, 3);
    std::cout << "sig rspnse: " << str_units_of_signal_response << std::endl;
    
    //B52F09 - units of calibration input
    std::string str_units_of_calibration_input = get_value(dataless, index, 3);
    std::cout << "  cal unit: " << str_units_of_calibration_input << std::endl;
    
    //B52F10 - latitude
    std::string str_latitude = get_value(dataless, index, 10);
    std::cout << "  latitude: " << str_latitude << std::endl;
    
    //B52F11 - longitude
    std::string str_longitude = get_value(dataless, index, 11);
    std::cout << " longitude: " << str_longitude << std::endl;
    
    //B52F12 - elevation
    std::string str_elevation = get_value(dataless, index, 7);
    std::cout << " elevation: " << str_elevation << std::endl;
    
    //B52F13 - local depth
    std::string str_local_depth = get_value(dataless, index, 5);
    std::cout << "local dpth: " << str_local_depth << std::endl;
    
    //B52F14 - azimuth
    std::string str_azimuth = get_value(dataless, index, 5);
    std::cout << "   azimuth: " << str_azimuth << std::endl;
    
    //B52F15 - dip
    std::string str_dip = get_value(dataless, index, 5);
    std::cout << "       dip: " << str_dip << std::endl;
    
    //B52F16 - data format identifier code
    std::string str_data_format_id_code = get_value(dataless, index, 4);
    std::cout << "  data fmt: " << str_data_format_id_code << std::endl;
    
    //B52F17 - data record length
    std::string str_data_record_length = get_value(dataless, index, 2);
    std::cout << "dat reclen: " << str_data_record_length << std::endl;
    
    //B52F18 - sample rate
    std::string str_sample_rate = get_value(dataless, index, 10);
    std::cout << " sample hz: " << str_sample_rate << std::endl;
    
    //B52F19 - max clock drift
    std::string str_max_clock_drift = get_value(dataless, index, 10);
    std::cout << " max clock: " << str_max_clock_drift << std::endl;
    
    //B52F20 - number of comments
    std::string str_number_of_comments = get_value(dataless, index, 4);
    std::cout << "comments #: " << str_number_of_comments << std::endl;
    
    //B52F21 - channel flags
    std::string str_channel_flags = get_value_until_tilde(dataless, index);
    std::cout << "chan flags: " << str_channel_flags << std::endl;
    
    //B52F22 - start date
    std::string str_start_date = get_value_until_tilde(dataless, index);
    std::cout << "start date: " << str_start_date << std::endl;
    
    //B52F23 - end date
    std::string str_end_date = get_value_until_tilde(dataless, index);
    std::cout << "  end date: " << str_end_date << std::endl;
    
    //B52F24 - update flag
    std::string str_update_flag = get_value(dataless, index, 1);
    std::cout << "update flg: " << str_update_flag << std::endl;
}

void parse_blockette_53(std::vector<unsigned char> & dataless, int * index) {
    //B53F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B53F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B53F03 - transfer function type
    std::string str_transfer_function_type = get_value(dataless, index, 1);
    std::cout << " xfer type: " << str_transfer_function_type << std::endl;
    
    //B53F04 - stage sequence number
    std::string str_stage_sequence_number = get_value(dataless, index, 2);
    std::cout << "stage seq#: " << str_stage_sequence_number << std::endl;
    
    //B53F05 - stage signal input units
    std::string str_stage_signal_input_units = get_value(dataless, index, 3);
    std::cout << "stage inpt: " << str_stage_signal_input_units << std::endl;
    
    //B53F06 - stage signal output units
    std::string str_stage_signal_output_units = get_value(dataless, index, 3);
    std::cout << "stage otpt: " << str_stage_signal_output_units << std::endl;
    int test = std::stoi(str_stage_signal_output_units);
    
    //B53F07 - A0 normalization factor
    std::string str_a0_normalization_factor = get_value(dataless, index, 12);
    std::cout << "a0 norm fr: " << str_a0_normalization_factor << std::endl;
    
    //B53F08 - normalization frequency
    std::string str_normalization_frequency = get_value(dataless, index, 12);
    std::cout << " norm freq: " << str_normalization_frequency << std::endl;
    
    //B53F09 - number of complex zeros
    std::string str_number_of_complex_zeros = get_value(dataless, index, 3);
    int number_of_complex_zeros = std::stoi(str_number_of_complex_zeros);
    std::cout << "cmplx 0s #: " << str_number_of_complex_zeros << std::endl;
    
    //repeat fields 10 - 13 for the number of complex zeros
    for (int i = 0; i < number_of_complex_zeros; i++) {
        //B53F10 - real zero
        std::string str_real_zero = get_value(dataless, index, 12);
        std::cout << " real zero: " << str_real_zero << std::endl;
        
        //B53F11 - imaginary zero
        std::string str_imaginary_zero = get_value(dataless, index, 12);
        std::cout << " imag zero: " << str_imaginary_zero << std::endl;
        
        //B53F12 - real zero error
        std::string str_real_zero_error = get_value(dataless, index, 12);
        std::cout << "real 0 err: " << str_real_zero_error << std::endl;
        
        //B53F13 - imaginary zero error
        std::string str_imaginary_zero_error = get_value(dataless, index, 12);
        std::cout << "imag 0 err: " << str_imaginary_zero_error << std::endl;
    }
    
    //B53F14 - number of complex poles
    std::string str_number_of_complex_poles = get_value(dataless, index, 3);
    int number_of_complex_poles = std::stoi(str_number_of_complex_poles);
    std::cout << "cmplxpls #: " << str_number_of_complex_poles << std::endl;
    
    //repeat fields 15 - 18 for the number of complex poles
    for (int i = 0; i < number_of_complex_poles; i++) {
        //B53F15 - real pole
        std::string str_real_pole = get_value(dataless, index, 12);
        std::cout << " real pole: " << str_real_pole << std::endl;
        
        //B53F16 - imaginary pole
        std::string str_imaginary_pole = get_value(dataless, index, 12);
        std::cout << " imag pole: " << str_imaginary_pole << std::endl;
        
        //B53F17 - real pole error
        std::string str_real_pole_error = get_value(dataless, index, 12);
        std::cout << "real pl er: " << str_real_pole_error << std::endl;
        
        //B53F18 - imaginary pole error
        std::string str_imaginary_pole_error = get_value(dataless, index, 12);
        std::cout << "imag pl er: " << str_imaginary_pole_error << std::endl;
    }
}

void parse_blockette_54(std::vector<unsigned char> &dataless, int *index) {
    //B54F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B54F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B54F03 - response type
    std::string str_response_type = get_value(dataless, index, 1);
    std::cout << " resp type: " << str_response_type << std::endl;
    
    //B54F04 - stage sequence number
    std::string str_stage_sequence_number = get_value(dataless, index, 2);
    std::cout << "stage seq#: " << str_stage_sequence_number << std::endl;
    
    //B54F05 - signal input units
    std::string str_signal_input_units = get_value(dataless, index, 3);
    std::cout << "sig in unt: " << str_signal_input_units << std::endl;
    
    //B54F06 - signal output units
    std::string str_signal_output_units = get_value(dataless, index, 3);
    std::cout << "sig ou unt: " << str_signal_output_units << std::endl;
    
    //B54F07 - number of numerators
    std::string str_number_of_numerators = get_value(dataless, index, 4);
    int number_of_numerators = std::stoi(str_number_of_numerators);
    std::cout << "numeratr #: " << str_number_of_numerators << std::endl;
    
    //repeat fields 8 - 9 for number of numerators
    for (int i = 0; i < number_of_numerators; i++) {
        //B54F08 - numerator coefficient
        std::string str_numerator_coefficient = get_value(dataless, index, 12);
        std::cout << "nmrtr coeff: " << str_numerator_coefficient << std::endl;
        
        //B54F09 - numerator error
        std::string str_numerator_error = get_value(dataless, index, 12);
        std::cout << "nmrtr error: " << str_numerator_error << std::endl;
    }
    
    //B54F10 - number of denominators
    std::string str_number_of_denominators = get_value(dataless, index, 4);
    int number_of_denominators = std::stoi(str_number_of_denominators);
    std::cout << "denmnatr #: " << str_number_of_denominators << std::endl;
    
    //repeat fields 11 - 12 for number of denominators
    for (int i = 0; i < number_of_denominators; i++) {
        //B54F08 - denominator coefficient
        std::string str_denominator_coefficient = get_value(dataless, index, 12);
        std::cout << "denom coeff: " << str_denominator_coefficient << std::endl;
        
        //B54F09 - denominator error
        std::string str_denominator_error = get_value(dataless, index, 12);
        std::cout << "denom error: " << str_denominator_error << std::endl;
    }
}

void parse_blockette_57(std::vector<unsigned char> &dataless, int *index) {
    //B57F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B57F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B57F03 - stage sequence number
    std::string str_stage_sequence_number = get_value(dataless, index, 2);
    std::cout << "stage seq#: " << str_stage_sequence_number << std::endl;
    
    //B57F04 - input sample rate
    std::string str_input_sample_rate = get_value(dataless, index, 10);
    std::cout << "  input hz: " << str_input_sample_rate << std::endl;
    
    //B57F05 - decimation factor
    std::string str_decimation_factor = get_value(dataless, index, 5);
    std::cout << "decim fact: " << str_decimation_factor << std::endl;
    
    //B57F06 - decimation offset
    std::string str_decimation_offset = get_value(dataless, index, 5);
    std::cout << "decim offs: " << str_decimation_offset << std::endl;
    
    //B57F07 - estimated delay
    std::string str_estimated_delay = get_value(dataless, index, 11);
    std::cout << " est delay: " << str_estimated_delay << std::endl;
    
    //B57F08 - correction applied
    std::string str_correction_applied = get_value(dataless, index, 11);
    std::cout << "corr appld: " << str_correction_applied << std::endl;
}

void parse_blockette_58(std::vector<unsigned char> &dataless, int *index) {
    //B58F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B58F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B58F03 - stage sequence number
    std::string str_stage_sequence_number = get_value(dataless, index, 2);
    std::cout << "stage seq#: " << str_stage_sequence_number << std::endl;
    
    //B58F04 - sensitivity/gain
    std::string str_sensitivity_gain = get_value(dataless, index, 12);
    std::cout << " sens/gain: " << str_sensitivity_gain << std::endl;
    
    //B58F05 - frequency
    std::string str_frequency = get_value(dataless, index, 12);
    std::cout << " frequency: " << str_frequency << std::endl;
    
    //B58F06 - number of history values
    std::string str_number_of_history_values = get_value(dataless, index, 2);
    int number_of_history_values = std::stoi(str_number_of_history_values);
    std::cout << "hist vals#: " << str_number_of_history_values << std::endl;
    
    //repeat fields 7 - 9 for number of history values
    for (int i = 0; i < number_of_history_values; i++) {
        //B58F07 - sensitivity for calibration
        std::string str_sensitivity_for_calibration = get_value(dataless, index, 12);
        std::cout << " sens 4 cal: " << str_sensitivity_for_calibration << std::endl;
        
        //B58F08 - frequency of calibration
        std::string str_frequency_of_calibration = get_value(dataless, index, 12);
        std::cout << "freq of cal: " << str_frequency_of_calibration << std::endl;
        
        //B58F09 - time of above calibration
        std::string str_time_of_above_calibration = get_value_until_tilde(dataless, index);
        std::cout << "time of cal: " << str_time_of_above_calibration << std::endl;
    }
}

void parse_blockette_59(std::vector<unsigned char> &dataless, int *index) {
    //B59F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B59F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B59F03 - beginning effective time
    std::string str_beginning_effective_time = get_value_until_tilde(dataless, index);
    std::cout << "begin time: " << str_beginning_effective_time << std::endl;
    
    //B59F04 - end effective time
    std::string str_end_effective_time = get_value_until_tilde(dataless, index);
    std::cout << "  end time: " << str_end_effective_time << std::endl;
    
    //B59F05 - comment code key
    std::string str_comment_code_key = get_value(dataless, index, 4);
    std::cout << "cmt code k: " << str_comment_code_key << std::endl;
    
    //B59F06 - comment level
    std::string str_comment_level = get_value(dataless, index, 6);
    std::cout << " cmt level: " << str_comment_level << std::endl;
}

void parse_blockette_62(std::vector<unsigned char> &dataless, int *index) {
    //B62F01 - blockette type
    std::string str_blkt_number = get_value(dataless, index, 3);
    std::cout << " blkt type: " << str_blkt_number << std::endl;
    
    //B62F02 - length of blockette
    std::string str_blkt_length = get_value(dataless, index, 4);
    std::cout << "  len blkt: " << str_blkt_length << std::endl;
    
    //B62F03 - transfer function type
    std::string str_transfer_function_type = get_value(dataless, index, 1);
    std::cout << "xfer fx tp: " << str_transfer_function_type << std::endl;
    
    //B62F04 - stage sequence number
    std::string str_stage_sequence_number = get_value(dataless, index, 2);
    std::cout << "stage seq#: " << str_stage_sequence_number << std::endl;
    
    //B62F05 - stage signal input units
    std::string str_stage_signal_input_units = get_value(dataless, index, 3);
    std::cout << "sig in unt: " << str_stage_signal_input_units << std::endl;
    
    //B62F06 - stage signal output units
    std::string str_stage_signal_output_units = get_value(dataless, index, 3);
    std::cout << "sig out ut: " << str_stage_signal_output_units << std::endl;
    
    //B62F07 - polynomial approximation type
    std::string str_polynomial_approximation_type = get_value(dataless, index, 1);
    std::cout << "ply aprx t: " << str_polynomial_approximation_type << std::endl;
    
    //B62F08 - valid frequency units
    std::string str_valid_frequency_units = get_value(dataless, index, 1);
    std::cout << "vld freq u: " << str_valid_frequency_units << std::endl;
    
    //B62F09 - lower valid frequency bound
    std::string str_lower_valid_frequency_bound = get_value(dataless, index, 12);
    std::cout << "lwr vld hz: " << str_lower_valid_frequency_bound << std::endl;
    
    //B62F10 - upper valid frequency bound
    std::string str_upper_valid_frequency_bound = get_value(dataless, index, 12);
    std::cout << "upr vld hz: " << str_upper_valid_frequency_bound << std::endl;
    
    //B62F11 - lower bound of approximation
    std::string str_lower_bound_of_approximation = get_value(dataless, index, 12);
    std::cout << " lwr bound: " << str_lower_bound_of_approximation << std::endl;
    
    //B62F12 - upper bound of approximation
    std::string str_upper_bound_of_approximation = get_value(dataless, index, 12);
    std::cout << " upr bound: " << str_upper_bound_of_approximation << std::endl;
    
    //B62F13 - maximum absolute error
    std::string str_maximum_absolute_error = get_value(dataless, index, 12);
    std::cout << "max abs er: " << str_maximum_absolute_error << std::endl;
    
    //B62F14 - number of polynomial coefficients
    std::string str_number_of_polynomial_coefficients = get_value(dataless, index, 3);
    int number_of_polynomial_coefficients = std::stoi(str_number_of_polynomial_coefficients);
    std::cout << "poly coef#: " << str_number_of_polynomial_coefficients << std::endl;
    
    //repeat fields 15 - 16 for number of polynomial coefficients
    for (int i = 0; i < number_of_polynomial_coefficients; i++) {
        //B62F15 - polynomial coefficient
        std::string str_polynomial_coefficient = get_value(dataless, index, 12);
        std::cout << " poly coeff: " << str_polynomial_coefficient << std::endl;
        
        //B62F16 - polynomial coefficient error
        std::string str_polynomial_coefficient_error = get_value(dataless, index, 12);
        std::cout << " poly coerr: " << str_polynomial_coefficient_error << std::endl;
    }
}

void parse_blockette(std::vector<unsigned char> &dataless, int *index) {
    //advance to the next character
    //prevents it from hanging on empty space
    std::string x(std::begin(dataless) + *index, std::begin(dataless) + *index + 1);
    while (static_cast<int>(x.at(0)) == 32) {
        *index += 1;
        std::string x(std::begin(dataless) + *index, std::begin(dataless) + *index + 1);
        if (static_cast<int>(x.at(0)) == 48) {
            break;
        }
    }
    
    std::string str_blkt_number = get_value(dataless, index, 3);
    int blkt_number = std::stoi(str_blkt_number);
    *index -= 3;
    std::cout << std::endl << "BLOCKETTE " << str_blkt_number << " [" << *index << "]" << std::endl;

    switch (blkt_number) {
        case 50:
            parse_blockette_50(dataless, index);
            break;
        case 51:
            parse_blockette_51(dataless, index);
            break;
        case 52:
            parse_blockette_52(dataless, index);
            break;
        case 53:
            parse_blockette_53(dataless, index);
            break;
        case 54:
            parse_blockette_54(dataless, index);
            break;
        case 57:
            parse_blockette_57(dataless, index);
            break;
        case 58:
            parse_blockette_58(dataless, index);
            break;
        case 59:
            parse_blockette_59(dataless, index);
            break;
        case 62:
            parse_blockette_62(dataless, index);
            break;
        default:
            std::cout << "NOTHING FOR BLKT " << str_blkt_number << " [" << *index << "]" << std::endl;
    }
}

void station_blockettes_skeleton(std::vector<unsigned char> &dataless, int *index) {
    //advance to the next character
    //prevents it from hanging on empty spaces
    std::string x(std::begin(dataless) + *index, std::begin(dataless) + *index + 1);
    while (static_cast<int>(x.at(0)) == 32) {
        *index += 1;
        std::string x(std::begin(dataless) + *index, std::begin(dataless) + *index + 1);
        if (static_cast<int>(x.at(0)) == 48) {
            break;
        }
    }
    
    std::string str_blkt_number = get_value(dataless, index, 3);
    int blkt_number = std::stoi(str_blkt_number);
    *index -= 3;
    if (blkt_number == 50) {std::cout << std::endl << "------------" << std::endl; }
    std::cout << "BLOCKETTE " << str_blkt_number << " [" << *index << "]" << std::endl;
    
    std::string str_next_blkt_offset(std::begin(dataless) + *index + 3, std::begin(dataless) + *index + 7);
//    std::cout << "next blockette at: +" << str_next_blkt_offset << std::endl;
    *index += std::stoi(str_next_blkt_offset);
}

int main(int argc, char *argv[]) {
    std::string filename = "/Users/ambaker/Documents/seed/IU.dataless";
    
    std::ifstream dataless_file(filename);
    std::ifstream dataless_new_file(filename);
    
    int dataless_length = filesize(filename.c_str());
    int dataless_new_length = dataless_length - (dataless_length / 4096) * 8;
    
    std::cout << "Length: " << dataless_length << std::endl;
    std::cout << "Length: " << dataless_new_length << std::endl;
    
//    std::vector<unsigned char> dataless;
//    dataless.reserve(dataless_length);
//    for (int i = 0; i < dataless_length; i++) {
//        dataless.push_back(dataless_file.get());
//    }
    
    //remove the logical sequence record numbers while filling the vector
    std::vector<unsigned char> dataless;
    dataless.reserve(dataless_length);
    int countdown = 0;
    for (int i = 0; i < dataless_length; i++) {
        if (i % 4096 == 0) {
            countdown = 8;
        }
        if (countdown > 0) {
            dataless_file.get();
            countdown--;
        }
        else {
            dataless.push_back(dataless_file.get());
        }
    }
    
    //remove the trailing spaces at the end of the dataless file
    std::cout << "Size before: " << dataless.size() << std::endl;
    while (true) {
        if (static_cast<int>(dataless.back()) == 32) {
            dataless.pop_back();
        }
        else {
            break;
        }
    }
    dataless.shrink_to_fit();
    std::cout << "Size after:  " << dataless.size() << std::endl;
    
    
    
    //find the first record for blockette 50
    int total_records = dataless_length / 4088;
    int index;
    for (int r = 0; r < total_records; r++) {
        index = r * 4088;
        std::string str_blkt_number = get_value(dataless, &index, 3);
        
        if (str_blkt_number == "050") {
            index -= 3;
            break;
        }
    }
    
//    while (index < dataless_length) {
//        std::string str_blkt_number(std::begin(dataless) + index, std::begin(dataless) + index + 3);
//        std::string str_next_blkt_offset(std::begin(dataless) + index + 3, std::begin(dataless) + index + 7);
//        if (str_blkt_number == "050") { std::cout << "---------------------------------" << std::endl;};
//        std::cout << "Blkt: " << str_blkt_number << " " << index << " +" << str_next_blkt_offset << " = " << std::endl;
//        
//        std::string x(std::begin(dataless) + index, std::begin(dataless) + index + 1);
//        while (static_cast<int>(x.at(0)) == 32) {
//            index += 1;
//            std::string x(std::begin(dataless) + index, std::begin(dataless) + index + 1);
//            if (static_cast<int>(x.at(0)) == 48) {
//                break;
//            }
//        }
//        index += std::stoi(str_next_blkt_offset);
//    }

//    //parse a certain amount
//    for (int i = 0; i < 248; i++) {
//        parse_blockette(dataless, &index);
//    }



    //parse through them all
    while (index < dataless.size()) {
        station_blockettes_skeleton(dataless, &index);
    }








//    std::cout << static_cast<int>(dataless[32704]) << std::endl;

//    //print all
//    for (int i = 0; i < dataless.size(); i++) {
//        std::cout << i << "\t\t" << dataless[i] << "\t\t";
//        std::cout << std::hex << std::showbase;
//        std::cout << static_cast<int>(dataless[i]);
//        std::cout << std::dec << std::noshowbase;
//        std::cout << std::endl;
//    }

//    //print all
//    for (int i = 32650; i < 32750; i++) {
//        std::cout << i << "\t\t" << dataless[i] << "\t\t";
//        std::cout << std::hex << std::showbase;
//        std::cout << dataless[i];
//        std::cout << std::dec << std::noshowbase;
//        std::cout << std::endl;
//    }
    
//    std::cout << "X" << dataless[32695] << dataless[32696] << "X" << std::endl;
    
    dataless_file.close();
}