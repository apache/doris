inline void pauser(){
    /// a portable way to pause a program
    std::string dummy;
    std::cout << "Press enter to continue...";
    std::getline(std::cin, dummy);
}

void FailIfThreadsAreZero(uint32_t numConcurrentThreads) {
	if(numConcurrentThreads == 0) {
		std::cout<<"ERROR: Invalid parameter 0 for number of concurrent calls into OpenBLAS!"<<std::endl;
		std::cout<<"CBLAS DGEMV thread safety test FAILED!"<<std::endl;
		exit(-1);
	}
}

void FillMatrices(std::vector<std::vector<double>>& matBlock, std::mt19937_64& PRNG, std::uniform_real_distribution<double>& rngdist, const blasint randomMatSize, const uint32_t numConcurrentThreads, const uint32_t numMat){
	for(uint32_t i=0; i<numMat; i++){
		for(uint32_t j = 0; j < static_cast<uint32_t>(randomMatSize*randomMatSize); j++){
			matBlock[i][j] = rngdist(PRNG);
		}
	}
	for(uint32_t i=numMat; i<(numConcurrentThreads*numMat); i+=numMat){
		for(uint32_t j=0; j<numMat; j++){
			matBlock[i+j] = matBlock[j];
		}
	}
}

void FillVectors(std::vector<std::vector<double>>& vecBlock, std::mt19937_64& PRNG, std::uniform_real_distribution<double>& rngdist, const blasint randomMatSize, const uint32_t numConcurrentThreads, const uint32_t numVec){
	for(uint32_t i=0; i<numVec; i++){
		for(uint32_t j = 0; j < static_cast<uint32_t>(randomMatSize); j++){
			vecBlock[i][j] = rngdist(PRNG);
		}
	}
	for(uint32_t i=numVec; i<(numConcurrentThreads*numVec); i+=numVec){
		for(uint32_t j=0; j<numVec; j++){
			vecBlock[i+j] = vecBlock[j];
		}
	}
}

std::mt19937_64 InitPRNG(){
	std::random_device rd;
	std::mt19937_64 PRNG(rd()); //seed PRNG using /dev/urandom or similar OS provided RNG
	std::uniform_real_distribution<double> rngdist{-1.0, 1.0};
	//make sure the internal state of the PRNG is properly mixed by generating 10M random numbers
	//PRNGs often have unreliable distribution uniformity and other statistical properties before their internal state is sufficiently mixed
	for (uint32_t i=0;i<10000000;i++) rngdist(PRNG);
	return PRNG;
}

void PrintMatrices(const std::vector<std::vector<double>>& matBlock, const blasint randomMatSize, const uint32_t numConcurrentThreads, const uint32_t numMat){
	for (uint32_t i=0;i<numConcurrentThreads*numMat;i++){
		std::cout<<i<<std::endl;
		for (uint32_t j = 0; j < static_cast<uint32_t>(randomMatSize); j++){
			for (uint32_t k = 0; k < static_cast<uint32_t>(randomMatSize); k++){
				std::cout<<matBlock[i][j*randomMatSize + k]<<"  ";
			}
			std::cout<<std::endl;
		}
		std::cout<<std::endl;
	}
}
