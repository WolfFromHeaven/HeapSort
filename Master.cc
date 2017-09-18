#include <iostream>
#include <assert.h>
#include <mpi.h>
#include <iomanip>
#include <algorithm>
#include <string.h>

#include "Master.h"
#include "Common.h"
#include "Configuration.h"
#include "PartitionSampling.h"
#include "Trie.h"

using namespace std;

void Master::run()
{
  if ( totalNode != 1 + conf.getNumReducer() ) {
    cout << "The number of workers mismatches the number of processes.\n";
    assert( false );
  }

  // GENERATE LIST OF PARTITIONS.
  PartitionSampling partitioner;
  partitioner.setConfiguration( &conf );
  //PartitionList* partitionList = partitioner.createPartitions();
	PartitionList rangeList;
	PartitionList partitionList;
	
  // BROADCAST CONFIGURATION TO WORKERS
  MPI::COMM_WORLD.Bcast( &conf, sizeof( Configuration ), MPI::CHAR, 0 );
  // Note: this works because the number of partitions can be derived from the number of workers in the configuration.


/*  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList->begin(); it != partitionList->end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
  }*/

  
  // TIME BUFFER
  int numWorker = conf.getNumReducer();
  double rcvTime[ numWorker + 1 ];  
  double rTime = 0;
  double avgTime;
  double maxTime;


	// RECIEVING TOPK DATASET RANGE
	//unsigned char partition[ conf.getKeySize()] = {" "};
  for ( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
		for(int j = 0; j < 2; j++){
/*		  unsigned char* buff1 = new unsigned char[ conf.getKeySize()];
		  MPI::COMM_WORLD.Gather( partition, conf.getKeySize(), MPI::UNSIGNED_CHAR, buff1, conf.getKeySize(), MPI::UNSIGNED_CHAR, 0 );
		  rangeList.push_back( buff1 ); //rangeList.push_back( buff2 );
			MPI::COMM_WORLD.Gather( partition, conf.getKeySize(), MPI::UNSIGNED_CHAR, buff1, conf.getKeySize(), MPI::UNSIGNED_CHAR, 0 );
			rangeList.push_back( buff1 );
*/
			unsigned char* buff = new unsigned char[ conf.getKeySize()];
			MPI::COMM_WORLD.Recv( buff, conf.getKeySize(), MPI::UNSIGNED_CHAR, i, 0);
			rangeList.push_back( buff );
		}
  }	
	//cout<<rangeList.size()<<"\n";
	sort( rangeList.begin(), rangeList.end(), Sorter(conf.getKeySize()) ) ;
	unsigned char* minKey = rangeList.front();
	unsigned char* maxKey = rangeList.back();
	//cout<<minKey[0]<<" "<<maxKey[0]<<"\n";
	unsigned long long dig1 = 0; unsigned long long dig2 = 0;
	//converting the stings to integer values
	for(unsigned int i = 0; i < conf.getKeySize(); i++){
			unsigned long long temp = pow(95,conf.getKeySize()-i-1);
			//cout<<minKey[i]<<" "<<maxKey[i]<<"\n";
			dig1 += (minKey[i]-32)*temp;
			dig2 += (maxKey[i]-32)*temp;
			//cout<<dig1<<" "<<dig2<<"\n";
	}
	//cout<<dig1<<" "<<dig2;
	unsigned long long part[conf.getNumReducer()-1];
	unsigned long long range = dig2 - dig1;
	unsigned long long partitionSize = round(range/conf.getNumReducer());
	for( unsigned int i = 1; i < conf.getNumReducer(); i++){
			part[i-1] = (i*partitionSize) + dig1;
	}
	//cout<<part[0]<<" "<<part[1];
	unsigned char tempArr[conf.getKeySize()];
	for( unsigned int i = 0; i < 2; i++){
		unsigned long long num = part[i];cout<<i<<"\n";
		unsigned char* keyBuff = new unsigned char[ conf.getKeySize()];
		for(unsigned int j = 0; j < conf.getKeySize(); j++){
				unsigned long long temp = pow(95,conf.getKeySize()-j-1);
				unsigned long long quotient = num / temp;
				unsigned long long rem = num % temp;
				num = rem;
				tempArr[j] = (unsigned char) (quotient + 32);cout<<tempArr[j];
		}
		cout<<"\n";
		memcpy(keyBuff, tempArr,conf.getKeySize());
		partitionList.push_back(keyBuff);
	}

  // BROADCAST PARTITIONS TO WORKERS
  for ( auto it = partitionList.begin(); it != partitionList.end(); it++ ) {
    unsigned char* partition = *it;
    MPI::COMM_WORLD.Bcast( partition, conf.getKeySize(), MPI::UNSIGNED_CHAR, 0 );
  }

  // COMPUTE MAP TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": MAP     | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;
	//cout<<"MAP phase successful"<<"\n";

  // COMPUTE PACKING TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": PACK    | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;  
	//cout<<"PACKING successful"<<"\n";

  
  // COMPUTE SHUFFLE TIME
  double txRate = 0;
  double avgRate = 0;
  for( unsigned int i = 1; i <= conf.getNumReducer(); i++ ) {
    MPI::COMM_WORLD.Barrier();
    MPI::COMM_WORLD.Barrier();
    MPI::COMM_WORLD.Recv( &rTime, 1, MPI::DOUBLE, i, 0 );
    avgTime += rTime;
    MPI::COMM_WORLD.Recv( &txRate, 1, MPI::DOUBLE, i, 0 );
    avgRate += txRate;
  }
  cout << rank << ": SHUFFLE | Sum = " << setw(10) << avgTime
       << "   Rate = " << setw(10) << avgRate/numWorker << " Mbps" << endl;
	//cout<<"SHUFFLE phase successful"<<"\n";  


  // COMPUTE UNPACK TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
  }
  cout << rank << ": UNPACK  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;
  //cout<<"UNPACKING successful"<<"\n";
  
  
  // COMPUTE REDUCE TIME
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, rcvTime, 1, MPI::DOUBLE, 0 );
  avgTime = 0;
  maxTime = 0;
  for( int i = 1; i <= numWorker; i++ ) {
    avgTime += rcvTime[ i ];
    maxTime = max( maxTime, rcvTime[ i ] );
		cout<<i<<":"<<rcvTime[i]<<"\n";
  }
  cout << rank << ": REDUCE  | Avg = " << setw(10) << avgTime/numWorker
       << "   Max = " << setw(10) << maxTime << endl;      
  //cout<<"REDUCE phase successful"<<"\n";

  // CLEAN UP MEMORY
  for ( auto it = partitionList.begin(); it != partitionList.end(); it++ ) {
    delete [] *it;
  }
}

/*bool Master::Sorter( const unsigned char* keyl, const unsigned char* keyr, unsigned int size )
{
  for ( unsigned long i = 0; i < size; i++ ) {
    if ( keyl[i] < keyr[i] ) {
      return true;
    }
    else if ( keyl[ i ] > keyr[ i ] ) {
      return false;
    }
  }
  return true;
}*/
