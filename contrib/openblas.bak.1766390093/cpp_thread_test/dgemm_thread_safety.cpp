#include <iostream>
#include <vector>
#include <random>
#include <future>
#include <omp.h>
#include "../cblas.h"
#include "cpp_thread_safety_common.h"

void launch_cblas_dgemm(double* A, double* B, double* C, const blasint randomMatSize){
	cblas_dgemm(CblasColMajor, CblasNoTrans, CblasNoTrans, randomMatSize, randomMatSize, randomMatSize, 1.0, A, randomMatSize, B, randomMatSize, 0.1, C, randomMatSize);
}

int main(int argc, char* argv[]){
	blasint randomMatSize = 1024; //dimension of the random square matrices used
	uint32_t numConcurrentThreads = 96; //number of concurrent calls of the functions being tested
	uint32_t numTestRounds = 16; //number of testing rounds before success exit
	uint32_t maxHwThreads = omp_get_max_threads();
	
	if (maxHwThreads < 96)
		numConcurrentThreads = maxHwThreads;

	if (argc > 4){
		std::cout<<"ERROR: too many arguments for thread safety tester"<<std::endl;
		abort();
	}
	
	if(argc == 4){
		std::vector<std::string> cliArgs;
		for (int i = 1; i < argc; i++){
			cliArgs.push_back(argv[i]);
			std::cout<<argv[i]<<std::endl;
		}
		randomMatSize = std::stoul(cliArgs[0]);
		numConcurrentThreads = std::stoul(cliArgs[1]);
		numTestRounds = std::stoul(cliArgs[2]);
	}
	
	std::uniform_real_distribution<double> rngdist{-1.0, 1.0};
	std::vector<std::vector<double>> matBlock(numConcurrentThreads*3);
	std::vector<std::future<void>> futureBlock(numConcurrentThreads);
	
	std::cout<<"*----------------------------*\n";
	std::cout<<"| DGEMM thread safety tester |\n";
	std::cout<<"*----------------------------*\n";
	std::cout<<"Size of random matrices(N=M=K): "<<randomMatSize<<'\n';
	std::cout<<"Number of concurrent calls into OpenBLAS : "<<numConcurrentThreads<<'\n';
	std::cout<<"Number of testing rounds : "<<numTestRounds<<'\n';
	std::cout<<"This test will need "<<(static_cast<uint64_t>(randomMatSize*randomMatSize)*numConcurrentThreads*3*8)/static_cast<double>(1024*1024)<<" MiB of RAM\n"<<std::endl;

	FailIfThreadsAreZero(numConcurrentThreads);
	
	std::cout<<"Initializing random number generator..."<<std::flush;
	std::mt19937_64 PRNG = InitPRNG();
	std::cout<<"done\n";
	
	std::cout<<"Preparing to test CBLAS DGEMM thread safety\n";
	std::cout<<"Allocating matrices..."<<std::flush;
	for(uint32_t i=0; i<(numConcurrentThreads*3); i++){
		matBlock[i].resize(randomMatSize*randomMatSize);
	}
	std::cout<<"done\n";
	//pauser();
	std::cout<<"Filling matrices with random numbers..."<<std::flush;
	FillMatrices(matBlock, PRNG, rngdist, randomMatSize, numConcurrentThreads, 3);
	//PrintMatrices(matBlock, randomMatSize, numConcurrentThreads, 3);
	std::cout<<"done\n";
	std::cout<<"Testing CBLAS DGEMM thread safety\n";
	omp_set_num_threads(numConcurrentThreads);
	for(uint32_t R=0; R<numTestRounds; R++){
		std::cout<<"DGEMM round #"<<R<<std::endl;
		std::cout<<"Launching "<<numConcurrentThreads<<" threads simultaneously using OpenMP..."<<std::flush;
		#pragma omp parallel for default(none) shared(futureBlock, matBlock, randomMatSize, numConcurrentThreads)
		for(uint32_t i=0; i<numConcurrentThreads; i++){
			futureBlock[i] = std::async(std::launch::async, launch_cblas_dgemm, &matBlock[i*3][0], &matBlock[i*3+1][0], &matBlock[i*3+2][0], randomMatSize);
			//launch_cblas_dgemm( &matBlock[i][0], &matBlock[i+1][0], &matBlock[i+2][0]);
		}
		std::cout<<"done\n";
		std::cout<<"Waiting for threads to finish..."<<std::flush;
		for(uint32_t i=0; i<numConcurrentThreads; i++){
			futureBlock[i].get();
		}
		std::cout<<"done\n";
		//PrintMatrices(matBlock, randomMatSize, numConcurrentThreads, 3);
		std::cout<<"Comparing results from different threads..."<<std::flush;
		for(uint32_t i=3; i<(numConcurrentThreads*3); i+=3){ //i is the index of matrix A, for a given thread
			for(uint32_t j = 0; j < static_cast<uint32_t>(randomMatSize*randomMatSize); j++){
				if (std::abs(matBlock[i+2][j] - matBlock[2][j]) > 1.0E-13){ //i+2 is the index of matrix C, for a given thread
					std::cout<<"ERROR: one of the threads returned a different result! Index : "<<i+2<<std::endl;
					std::cout<<"CBLAS DGEMM thread safety test FAILED!"<<std::endl;
					return -1;
				}
			}
		}
		std::cout<<"OK!\n"<<std::endl;
	}
	std::cout<<"CBLAS DGEMM thread safety test PASSED!\n"<<std::endl;
	return 0;
}
