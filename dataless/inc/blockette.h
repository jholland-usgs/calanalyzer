#ifndef _Blockette_H_
#define _Blockette_H_

#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "blockette50.h"
#include "blockette51.h"
#include "blockette52.h"
#include "blockette53.h"
#include "blockette54.h"
#include "blockette57.h"
#include "blockette58.h"
#include "blockette59.h"
#include "blockette62.h"
#include "date.h"

class Blockette {
public:
    int type;
    int length;
    
    void Parse();
};

#endif  //_Blockette_H_