#include <iostream>
#include <iomanip>
#include <utility>

#include "Utility.h"
#include "Common.h"


void printKey( const unsigned char* key, unsigned int size )
{
  for ( unsigned long int i = 0; i < size; i++ ) {
    cout << setw( 2 ) << setfill( '0' ) << hex << (int) key[ i ] << " ";
  }
  cout << dec;
}


bool cmpKey( const unsigned char* keyA, const unsigned char* keyB, unsigned int size )
{
  for ( unsigned long i = 0; i < size; i++ ) {
    if ( keyA[ i ] < keyB[ i ] ) {
      return true;
    }
    else if ( keyA[ i ] > keyB[ i ] ) {
      return false;
    }
  }
  return false;
}

bool cmpKeyInverse(const std::pair<unsigned char*,unsigned int> keyl, std::pair<unsigned char*,unsigned int> keyr, unsigned int size){

  for ( unsigned long i = 0; i < size; i++ ) {
    if ( keyl.first[i] < keyr.first[ i ] ) {
      return false;
    }
    else if ( keyl.first[ i ] > keyr.first[ i ] ) {
      return true;
    }
  }
  return false;

}
