#ifndef _MR_UTILITY
#define _MR_UTILITY

#include <iostream>
#include <iomanip>
#include <utility>

void printKey( const unsigned char* key, unsigned int size );
bool cmpKey( const unsigned char* keyA, const unsigned char* keyB, unsigned int size );
bool cmpKeyInverse(const std::pair<unsigned char*,unsigned int> keyl, const std::pair<unsigned char*,unsigned int> keyr, unsigned int size);

#endif
