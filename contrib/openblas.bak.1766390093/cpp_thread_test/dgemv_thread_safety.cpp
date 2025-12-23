#include <iostream>
#include <vector>
#include <random>
#include <future>
#include <omp.h>
#include "../cblas.h"
#include "cpp_thread_safety_common.h"

void launch_cblas_dgemv(double* A, double* x, double* y, const blasint randomMatSize)
	{
	const blasint inc = 1;
	cblas_dgemv(CblasColMajor, CblasNoTrans, randomMatSize, randomMatSize, 1.0, A, randomMatSize, x, inc, 0.1, y, inc);
	}

int main(int argc, char* argv[])	
	{
	blasint randomMatSize = 1024; //dimension of the random square matrices and vectors being used
	uint32_t numConcurrentThreads = 52; //number of concurrent calls of the functions being tested
	uint32_t numTestRounds = 16; //number of testing rounds before success exit
	uint32_t maxHwThreads = omp_get_max_threads();
	
	if (maxHwThreads < 52)
		numConcurrentThreads = maxHwThreads;
	
	if (argc > 4)
		{
		std::cout<<"ERROR: too many arguments for thread safety tester"<<std::endl;
		abort();
		}
	if(argc == 4)
		{
		std::vector<std::string> cliArgs;
		for (int i = 1; i < argc; i++)
			{
			cliArgs.push_back(argv[i]);
			std::cout<<argv[i]<<std::endl;
			}
		randomMatSize = std::stoul(cliArgs.at(0));
		numConcurrentThreads = std::stoul(cliArgs.at(1));
		numTestRounds = std::stoul(cliArgs.at(2));
		}
	
	std::uniform_real_distribution<double> rngdist{-1.0, 1.0};
	std::vector<std::vector<double>> matBlock(numConcurrentThreads);
	std::vector<std::vector<double>> vecBlock(numConcurrentThreads*2);
	std::vector<std::future<void>> futureBlock(numConcurrentThreads);
	
	std::cout<<"*----------------------------*\n";
	std::cout<<"| DGEMV thread safety tester |\n";
	std::cout<<"*----------------------------*\n";
	std::cout<<"Size of random matrices and vectors(N=M): "<<randomMatSize<<'\n';
	std::cout<<"Number of concurrent calls into OpenBLAS : "<<numConcurrentThreads<<'\n';
	std::cout<<"Number of testing rounds : "<<numTestRounds<<'\n';
	std::cout<<"This test will need "<<((static_cast<uint64_t>(randomMatSize*randomMatSize)*numConcurrentThreads*8)+(static_cast<uint64_t>(randomMatSize)*numConcurrentThreads*8*2))/static_cast<double>(1024*1024)<<" MiB of RAM\n"<<std::endl;

	FailIfThreadsAreZero(numConcurrentThreads);
	
	std::cout<<"Initializing random number generator..."<<std::flush;
	std::mt19937_64 PRNG = InitPRNG();
	std::cout<<"done\n";
	
	std::cout<<"Preparing to test CBLAS DGEMV thread safety\n";
	std::cout<<"Allocating matrices..."<<std::flush;
	for(uint32_t i=0; i<numConcurrentThreads; i++)
		{
		matBlock.at(i).resize(randomMatSize*randomMatSize);
		}
	std::cout<<"done\n";
	std::cout<<"Allocating vectors..."<<std::flush;
	for(uint32_t i=0; i<(numConcurrentThreads*2); i++)
		{
		vecBlock.at(i).resize(randomMatSize);
		}
	std::cout<<"done\n";
	
	//pauser();
	
	std::cout<<"Filling matrices with random numbers..."<<std::flush;
	FillMatrices(matBlock, PRNG, rngdist, randomMatSize, numConcurrentThreads, 1);
	//PrintMatrices(matBlock, randomMatSize, numConcurrentThreads);
	std::cout<<"done\n";
	std::cout<<"Filling vectors with random numbers..."<<std::flush;
	FillVectors(vecBlock, PRNG, rngdist, randomMatSize, numConcurrentThreads, 2);
	std::cout<<"done\n";
	
	std::cout<<"Testing CBLAS DGEMV thread safety"<<std::endl;
	omp_set_num_threads(numConcurrentThreads);
	for(uint32_t R=0; R<numTestRounds; R++)
		{
		std::cout<<"DGEMV round #"<<R<<std::endl;
		std::cout<<"Launching "<<numConcurrentThreads<<" threads simultaneously using OpenMP..."<<std::flush;
		#pragma omp parallel for default(none) shared(futureBlock, matBlock, vecBlock, randomMatSize, numConcurrentThreads)
		for(uint32_t i=0; i<numConcurrentThreads; i++)
			{
			futureBlock[i] = std::async(std::launch::async, launch_cblas_dgemv, &matBlock[i][0], &vecBlock[i*2][0], &vecBlock[i*2+1][0], randomMatSize);
			}
		std::cout<<"done\n";
		std::cout<<"Waiting for threads to finish..."<<std::flush;
		for(uint32_t i=0; i<numConcurrentThreads; i++)
			{
			futureBlock[i].get();
			}
		std::cout<<"done\n";
		std::cout<<"Comparing results from different threads..."<<std::flush;
		for(uint32_t i=2; i<(numConcurrentThreads*2); i+=2){ //i is the index of vector x, for a given thread
			for(uint32_t j = 0; j < static_cast<uint32_t>(randomMatSize); j++)
				{
				if (std::abs(vecBlock[i+1][j] - vecBlock[1][j]) > 1.0E-13){ //i+1 is the index of vector y, for a given thread
					std::cout<<"ERROR: one of the threads returned a different result! Index : "<<i+1<<std::endl;
					std::cout<<"CBLAS DGEMV thread safety test FAILED!"<<std::endl;
					return -1;
					}
				}
			}
			std::cout<<"OK!\n"<<std::endl;
		}
	std::cout<<"CBLAS DGEMV thread safety test PASSED!\n"<<std::endl;
	return 0;
	}
