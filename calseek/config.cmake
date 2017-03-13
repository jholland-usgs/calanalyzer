cmake_minimum_required(VERSION 2.8.11)
project(calseek)

# CMake include guard
if(calseek_config_included)
  return()
endif(calseek_config_included)
set(calseek_config_included true)

set(DIR_CALSEEK ${CMAKE_CURRENT_LIST_DIR})

# ----- DATE LIBRARY ----- #
# include_directories(${DIR_MZN}/libs/date/)

# ----- MD5 LIBRARY ----- #
# include_directories(${DIR_MZN}/libs/md5/inc)
# set(SOURCES_MD5 ${DIR_MZN}/libs/md5/src/md5.cpp)

# ----- COMPILER FLAGS ----- #
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall")

# ----- CONFIGURATION VARIABLE ----- #
include_directories(${DIR_CALSEEK})