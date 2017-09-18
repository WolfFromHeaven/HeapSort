#ifndef _MR_CONFIGURATION
#define _MR_CONFIGURATION

class Configuration {

 protected:
  unsigned int numReducer;
  unsigned int numInput;  
  
  const char *inputPath;
  const char *outputPath;
  const char *partitionPath;
  unsigned long numSamples;
	unsigned int K;
  
 public:
  Configuration() {
    numReducer = 3;
    numInput = numReducer;    
    K = 600000;
    //inputPath = "./Input/file.txt";
		//outputPath = "./Output/Output_10kB";
    //partitionPath = "./Partition/Partition_10kB";
    inputPath = "./Input/Input1GB.txt";
		outputPath = "./Output/Output_1GB";
    partitionPath = "./Partition/Partition_1GB";
    numSamples = 10000000;
  }
  ~Configuration() {}
  const static unsigned int KEY_SIZE = 10;
  const static unsigned int VALUE_SIZE = 90;  
  
  unsigned int getNumReducer() const { return numReducer; }
  unsigned int getNumInput() const { return numInput; } 
	unsigned int getK() const { return K; } 
  const char *getInputPath() const { return inputPath; }
  const char *getOutputPath() const { return outputPath; }
  const char *getPartitionPath() const { return partitionPath; }
  unsigned int getKeySize() const { return KEY_SIZE; }
  unsigned int getValueSize() const { return VALUE_SIZE; }
  unsigned int getLineSize() const { return KEY_SIZE + VALUE_SIZE; }
  unsigned long getNumSamples() const { return numSamples; }  
};

#endif
