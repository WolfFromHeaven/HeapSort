#include <iostream>
#include <mpi.h>
#include <iomanip>
#include <fstream>
#include <cstdio>
#include <map>
#include <assert.h>
#include <algorithm>
#include <ctime>
#include <cstring>
#include <list>
#include <vector>
#include <utility>

#include "Worker.h"
#include "Configuration.h"
#include "Common.h"
#include "Utility.h"

using namespace std;

Worker::~Worker()
{
  delete conf;
  for ( auto it = partitionList.begin(); it != partitionList.end(); ++it ) {
    delete [] *it;
  }

  LineList* ll = partitionCollection[ rank - 1 ];
  for( auto lit = ll->begin(); lit != ll->end(); lit++ ) {
    delete [] *lit;
  }
  delete ll;
  
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
     delete [] *it;
  }

  delete trie;
}

void Worker::run()
{
  // RECEIVE CONFIGURATION FROM MASTER
  conf = new Configuration;
  MPI::COMM_WORLD.Bcast( (void*) conf, sizeof( Configuration ), MPI::CHAR, 0 );


/*  // RECEIVE PARTITIONS FROM MASTER
  for ( unsigned int i = 1; i < conf->getNumReducer(); i++ ) {
    unsigned char* buff = new unsigned char[ conf->getKeySize() + 1 ];
    MPI::COMM_WORLD.Bcast( buff, conf->getKeySize() + 1, MPI::UNSIGNED_CHAR, 0 );
    partitionList.push_back( buff );
  }*/


  // EXECUTE MAP PHASE
  clock_t time;
  double rTime;
  execMap();

  
  // SHUFFLING PHASE
  unsigned int lineSize = conf->getLineSize();
  for ( unsigned int i = 1; i <= conf->getNumReducer(); i++ ) {
    if ( i == rank ) {
      clock_t txTime = 0;
      unsigned long long tolSize = 0;
      MPI::COMM_WORLD.Barrier();
      time = clock();  	            
      // Sending from node i
      for ( unsigned int j = 1; j <= conf->getNumReducer(); j++ ) {
	if ( j == i ) {
	  continue;
	}
	TxData& txData = partitionTxData[ j - 1 ];
	txTime -= clock();
	MPI::COMM_WORLD.Send( &( txData.numLine ), 1, MPI::UNSIGNED_LONG_LONG, j, 0 );
	MPI::COMM_WORLD.Send( txData.data, txData.numLine * lineSize, MPI::UNSIGNED_CHAR, j, 0 );
	txTime += clock();
	tolSize += txData.numLine * lineSize + sizeof(unsigned long long);
	//cout<<tolSize<<"\n";
	delete [] txData.data;
      }
      MPI::COMM_WORLD.Barrier();
      time = clock() - time;
      rTime = double( time ) / CLOCKS_PER_SEC;        
      double txRate = ( tolSize * 8 * 1e-6 ) / ( double( txTime ) / CLOCKS_PER_SEC );
      MPI::COMM_WORLD.Send( &rTime, 1, MPI::DOUBLE, 0, 0 );
      MPI::COMM_WORLD.Send( &txRate, 1, MPI::DOUBLE, 0, 0 );
      //cout << rank << ": Avg sending rate is " << ( tolSize * 8 ) / ( rtxTime * 1e6 ) << " Mbps, Data size is " << tolSize / 1e6 << " MByte\n";
    }
    else {
      MPI::COMM_WORLD.Barrier();
      // Receiving from node i
      TxData& rxData = partitionRxData[ i - 1 ];
      MPI::COMM_WORLD.Recv( &( rxData.numLine ), 1, MPI::UNSIGNED_LONG_LONG, i, 0 );
      rxData.data = new unsigned char[ rxData.numLine * lineSize ];
      MPI::COMM_WORLD.Recv( rxData.data, rxData.numLine*lineSize, MPI::UNSIGNED_CHAR, i, 0 );
      MPI::COMM_WORLD.Barrier();
    }
  }


	//unsigned int numElements[conf->getNumReducer()] = {0};//stores number of elemnts from each node at rank node

  // UNPACK PHASE
  time = -clock();
	LineList CompleteList[conf->getNumReducer()]; //stores all the local and non-local data in an array corresponding to each worker

  // append local partition to localList
  for ( auto it = partitionCollection[ rank - 1 ]->begin(); it != partitionCollection[ rank - 1 ]->end(); ++it ) {
    unsigned char* buff = new unsigned char[ conf->getLineSize() ];
    memcpy( buff, *it, conf->getLineSize() );
    localList.push_back( buff );
		CompleteList[rank-1].push_back( buff );
  }

  // append data from other workers
  for( unsigned int i = 1; i <= conf->getNumReducer(); i++ ) {
    if( i == rank ) {
      continue;
    }
    TxData& rxData = partitionRxData[ i - 1 ];
    for( unsigned long long lc = 0; lc < rxData.numLine; lc++ ) {
      unsigned char* buff = new unsigned char[ lineSize ];
      memcpy( buff, rxData.data + lc*lineSize, lineSize );
      localList.push_back( buff );
			CompleteList[i-1].push_back( buff );
    }
    delete [] rxData.data;
  }
	
	//for(auto it = CompleteList[2].begin(); it != CompleteList[2].end(); ++it) cout<<*it<<"\n";

	//for(unsigned int i = 0; i < conf->getNumReducer(); i++) cout<<CompleteList[i].size()<<rank<<"\n";
  time += clock();
  rTime = double( time ) / CLOCKS_PER_SEC;    
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );      

  // REDUCE PHASE
  time = clock();  
  execReduce(CompleteList);
  time = clock() - time;
  rTime = double( time ) / CLOCKS_PER_SEC;    
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );    
  
  outputLocalList();
	outputLocalListKeys();
  //printLocalList();
}


void Worker::execMap()
{
  clock_t time = 0;
  double rTime = 0;
  time -= clock();
  
  // READ INPUT FILE 
  char filePath[ MAX_FILE_PATH ];
  sprintf( filePath, "%s_%d", conf->getInputPath(), rank - 1 );
  ifstream inputFile( filePath, ios::in | ios::binary | ios::ate );
  if ( !inputFile.is_open() ) {
    cout << rank << ": Cannot open input file " << conf->getInputPath() << endl;
    assert( false );
  }

  int fileSize = inputFile.tellg();
  unsigned long int lineSize = conf->getLineSize();
  unsigned long int numLine = fileSize / lineSize;
  inputFile.seekg( 0, ios::beg );


  // MAP
  // Put each line to associated collection according to partition list
/*  for ( unsigned long i = 0; i < numLine; i++ ) {
    unsigned char* buff = new unsigned char[ lineSize ];
    inputFile.read( ( char * ) buff, lineSize );
    unsigned int wid = trie->findPartition( buff );
    partitionCollection.at( wid )->push_back( buff );
  }*/
	
	//STORING INPUT DATA
	//unsigned char key[conf->getKeySize() + 1];
  LineList keyValueArray;
	for(unsigned int i = 0; i < numLine; i++){
		unsigned char* buff = new unsigned char[ lineSize ];    
		inputFile.read( ( char * ) buff, lineSize );            
    keyValueArray.push_back( buff );
	} 
  inputFile.close();  

	//SORTING THE KEYS
	//sort( keyValueArray.begin(), keyValueArray.end(), Sorter(conf->getKeySize()) );
	
	unsigned int K = conf->getK();
	cout<<rank<<" : ";
	clock_t t3 = 0;
	t3 = clock();
	LineList top_K = FindtopK(keyValueArray,K); 
	t3 = clock() - t3;
	cout<<"TopK extraction time : "<<((float)t3)/CLOCKS_PER_SEC<<"\n";

/*	cout<<rank << ": ";
	for(unsigned int i = 0; i < K; i++){
		bool flag = cmpKey(topK[i], top_K[i], conf->getKeySize());
		//if( flag == 1) cout<<"topK "<<topK[i]<<"\n"<<"top_K "<<top_K[i];
		cout<<flag<<" ";
	}
	cout<<"\n";*/

	//MPI::COMM_WORLD.Barrier();

	//SEND THE TOPK VALUES FROM EACH SERVER TO THE MASTER NODE
	LineList minMax;
	minMax.push_back(top_K.front());
	minMax.push_back(top_K.back());

	for(auto it = minMax.begin(); it != minMax.end(); ++it){
			unsigned char* partition = *it;
			//MPI::COMM_WORLD.Gather( partition, conf->getKeySize(), MPI::UNSIGNED_CHAR, NULL, 0, MPI::UNSIGNED_CHAR, 0 );
			MPI::COMM_WORLD.Send( partition, conf->getKeySize(), MPI::UNSIGNED_CHAR, 0, 0 );

	}
   

	// RECEIVE PARTITIONS FROM MASTER
  for ( unsigned int i = 1; i < conf->getNumReducer(); i++ ) {
    unsigned char* buff = new unsigned char[ conf->getKeySize() + 1 ];
    MPI::COMM_WORLD.Bcast( buff, conf->getKeySize() , MPI::UNSIGNED_CHAR, 0 );
    partitionList.push_back( buff );//cout<<buff<<"\n";
  }

  // Build trie
  unsigned char prefix[ conf->getKeySize() ];
  trie = buildTrie( &partitionList, 0, partitionList.size(), prefix, 0, 2 );

  // Create lists of lines
  for ( unsigned int i = 0; i < conf->getNumReducer(); i++ ) {
    partitionCollection.insert( pair< unsigned int, LineList* >( i, new LineList ) );
  }


	//PARTITIONING LOCAL DATA
	//cout<<rank<<":";
	for ( unsigned long i = 0; i < K; i++ ) {
    unsigned int wid = trie->findPartition( top_K[i] );
		//cout<<wid<<" ";
    partitionCollection.at( wid )->push_back( top_K[i] );
	}
	//cout<<"\n";

/*  for( unsigned int i = 0; i < conf->getNumReducer(); i++ ) {
    unsigned long long numLine = partitionCollection[ i ]->size();
		cout<<numLine<<" ";
  }
	cout<<"\n";*/

  time += clock();
  rTime = double( time ) / CLOCKS_PER_SEC;  
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );    

	// PACK PARTITIONED DATA TO A CHUNK
  time = -clock();
  //cout<<rank<<":";
  for( unsigned int i = 0; i < conf->getNumReducer(); i++ ) {
    if( i == rank - 1 ) {
      continue;
    }
    unsigned long long numLine = partitionCollection[ i ]->size();
    partitionTxData[ i ].data = new unsigned char[ numLine * lineSize ];
    partitionTxData[ i ].numLine = numLine;
		//cout<<numLine<<" ";
    auto lit = partitionCollection[ i ]->begin();
    for( unsigned long long j = 0; j < numLine * lineSize; j += lineSize ) {
      memcpy( partitionTxData[ i ].data + j, *lit, lineSize );
      delete [] *lit;
      lit++;
    }
    delete partitionCollection[ i ];
  }
	//cout<<"\n";
  time += clock();
  rTime = double( time ) / CLOCKS_PER_SEC;
  MPI::COMM_WORLD.Gather( &rTime, 1, MPI::DOUBLE, NULL, 1, MPI::DOUBLE, 0 );    
}


//void Worker::execReduce()
 void Worker::execReduce(LineList array[])
{
  // if( rank == 1) {
  //   cout << rank << ":Sort " << localList.size() << " lines\n";
  // }
   
	//stable_sort( localList.begin(), localList.end(), Sorter( conf->getKeySize() ) );
  
	//divide the localList into k(# of worker nodes arrays) depending on which values were received from which node
	//sort the k sorted arrays
	
	LineList FinalSortedList = SortSorted(array);
	localList = FinalSortedList;
	cout << rank << ":Sorted " << localList.size() << " lines\n";

//sort( localList.begin(), localList.end(), Sorter( conf->getKeySize() ) );
}


void Worker::printLocalList()
{
  unsigned long int i = 0;
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
    cout << rank << ": " << i++ << "| ";
    printKey( *it, conf->getKeySize() );
    cout << endl;
  }
}


void Worker::printPartitionCollection()
{
  for ( auto it = partitionCollection.begin(); it != partitionCollection.end(); ++it ) {
    unsigned int c = it->first;
    LineList* list = it->second;
    unsigned long i = 0;
    for ( auto lit = list->begin(); lit != list->end(); ++lit ) {
      cout << rank << ": " << c << "| " << i++ << "| ";
      printKey( *lit, conf->getKeySize() );
      cout << endl;
    }
  }
}


void Worker::outputLocalList()
{
  char buff[ MAX_FILE_PATH ];
  sprintf( buff, "%s_%u_%u", conf->getOutputPath(), rank - 1, conf->getK() );
  ofstream outputFile( buff, ios::out | ios::binary | ios::trunc );
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
    outputFile.write( ( char* ) *it, conf->getLineSize() );
		//outputFile.write( ( char* ) *it, conf->getKeySize() );
		//outputFile.write("\n",1);  
	}
  outputFile.close();
  //cout << rank << ": outputFile " << buff << " is saved.\n";
}

void Worker::outputLocalListKeys()
{
  char buff[ MAX_FILE_PATH ];
  sprintf( buff, "%s%s_%u_%u", conf->getOutputPath(), "Keys", rank - 1, conf->getK() );
  ofstream outputFile( buff, ios::out | ios::binary | ios::trunc );
  for ( auto it = localList.begin(); it != localList.end(); ++it ) {
    //outputFile.write( ( char* ) *it, conf->getLineSize() );
		outputFile.write( ( char* ) *it, conf->getKeySize() );
		outputFile.write("\n",1);  
	}
  outputFile.close();
  //cout << rank << ": outputFile " << buff << " is saved.\n";
}


TrieNode* Worker::buildTrie( PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize, int maxDepth )
{
  if ( prefixSize >= maxDepth || lower == upper ) {
    return new LeafTrieNode( prefixSize, partitionList, lower, upper );
  }
  InnerTrieNode* result = new InnerTrieNode( prefixSize );
  int curr = lower;
  for ( unsigned char ch = 0; ch < 255; ch++ ) {
    prefix[ prefixSize ] = ch;
    lower = curr;
    while( curr < upper ) {
      if( cmpKey( prefix, partitionList->at( curr ), prefixSize + 1 ) ) {
	break;
      }
      curr++;
    }
    result->setChild( ch, buildTrie( partitionList, lower, curr, prefix, prefixSize + 1, maxDepth ) );
  }
  prefix[ prefixSize ] = 255;
  result->setChild( 255, buildTrie( partitionList, curr, upper, prefix, prefixSize + 1, maxDepth ) );
  return result;
}

//ADDED BY ME

LineList Worker::merge(LineList in1, LineList in2){
  LineList sorted;
  unsigned int i=0; unsigned int j = 0; 
   while (i < in1.size() && j < in2.size())
    {
        if (cmpKey(in1[i],in2[j],conf->getKeySize()))
        {
            sorted.push_back(in1[i]); i++;
        }
        else
        {
            sorted.push_back(in2[j]); j++;
        }
    }
 
    while (i < in1.size())
    {   
        sorted.push_back(in1[i]); i++; 
    }
 
    while (j < in2.size())
    {   
        sorted.push_back(in2[j]); j++;
    }
    return sorted;
}

LineList Worker::SortSorted(LineList array[]){
    LineList temp;
    int arrSize = conf->getNumReducer(); //number of reducers
    temp = merge(array[0], array[1]);
    for(int i = 2; i< arrSize; i++){
      temp = merge(temp, array[i]);
    }
  return temp;

}

/*
LineList Worker::FindtopK(LineList array, unsigned int k){
	map<unsigned char*,unsigned int> Id;
  LineList charVectorarray[k];
  
  for(unsigned int i = 0; i < array.size(); i++){
      charVectorarray[i % k].push_back(array[i]);
      Id[array[i]] = i % k;
  }
  clock_t t1 = 0;
  LineList topK;
	t1 = clock();
  for(unsigned int i = 0; i < k; i++){
    sort(charVectorarray[i].begin(), charVectorarray[i].end(), Sorter(conf->getKeySize()));
    topK.push_back(charVectorarray[i].front());
  }
  t1 = clock() - t1;
	cout<<"Time taken by k sorts : "<<((float)t1)/CLOCKS_PER_SEC<<"\n";
	make_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));

  LineList finaltopK;
  for(unsigned int i = 0; i < k; i++){

      unsigned int index = Id[topK.front()];
      finaltopK.push_back(topK.front());
			charVectorarray[index].erase(charVectorarray[index].begin());

//			if(charVectorarray[index].empty()){
//				pop_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
//				index = Id[topK.front()];
//			}

			pop_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
			topK.pop_back();
			if(!charVectorarray[index].empty()){
				//topK.erase(topK.begin());
		    topK.push_back(charVectorarray[index].front());
		    push_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
			}
			//else pop_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
  }

	return finaltopK;
}*/


LineList Worker::FindtopK(LineList array, unsigned int k){
  DataK charVectorarray[k];
//	clock_t t1 = 0; clock_t t2 = 0;
//	t1 = clock();

	DataK num;
  for(unsigned int i = 0 ; i < array.size(); i++){
      num.push_back(make_pair(array[i],i%k));
  }
	
  for(auto it = num.begin(); it != num.end(); ++it ){
      charVectorarray[(*it).second].push_back(*it);
  }
// 	t1 = clock() - t1;
//	cout<<"Time taken for dividing into k parts : "<<((float)t1)/CLOCKS_PER_SEC<<"\n";

  DataK topK;
	//t1 = clock();
  for(unsigned int i = 0; i < k; i++){
    topK.push_back( *max_element( charVectorarray[i].begin(), charVectorarray[i].end(), reverseSorter(conf->getKeySize()) ));
  }
  //t1 = clock() - t1;
 	//cout<<"Time taken by k sorts : "<<((float)t1)/CLOCKS_PER_SEC<<"\n";

	//t2 = clock();
	make_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
	//t2 = clock() - t2;
	//cout<<"Time for Initial Heap : "<<((float)t1)/CLOCKS_PER_SEC<<"\n";

  LineList finaltopK;
	
	double totalpush = 0.00; double totalpop = 0.00;
	double pop[k], push[k];
	//t1 = 0; t1 = clock();
  for(unsigned int i = 0; i < k; i++){
			//t2 = 0; t1 = 0;
      unsigned int index = (topK.front()).second;
      finaltopK.push_back((topK.front()).first);
			charVectorarray[index].erase(find (charVectorarray[index].begin(), charVectorarray[index].end(), topK.front()));
			
			//t1 = clock();
			pop_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
			topK.pop_back();
			//t1 = clock() - t1;			
			//pop[i] = ((float)t1)/CLOCKS_PER_SEC;
			//totalpop = totalpop + pop[i];
			if(!charVectorarray[index].empty()){
				//t2 = clock();
		    topK.push_back(*max_element( charVectorarray[index].begin(), charVectorarray[index].end(), reverseSorter(conf->getKeySize()) ));
		    push_heap(topK.begin(), topK.end(), reverseSorter(conf->getKeySize()));
				//t2 = clock() - t2;
				//push[i] = ((float)t2)/CLOCKS_PER_SEC;
				//totalpush = totalpush + push[i];
			}
			
  }
	//t1 = clock() - t1;
	//cout<<"After heap : "<<((float)t1)/CLOCKS_PER_SEC<<"\n";
	//cout<<"Time taken to readjust heap after removing an element : "<<totalpop<<"\n";
	//cout<<"Time taken to readjust heap after inserting an element : "<<totalpush<<"\n";
	return finaltopK;
}




