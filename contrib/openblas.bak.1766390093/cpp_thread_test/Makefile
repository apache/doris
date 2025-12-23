TOPDIR = ..
include $(TOPDIR)/Makefile.system

all :: dgemv_tester dgemm_tester

dgemv_tester :
	$(CXX) $(COMMON_OPT) -Wall -Wextra -Wshadow -fopenmp -std=c++11 dgemv_thread_safety.cpp ../$(LIBNAME) $(EXTRALIB) $(FEXTRALIB) -o dgemv_tester
	./dgemv_tester

dgemm_tester : dgemv_tester
	$(CXX) $(COMMON_OPT) -Wall -Wextra -Wshadow -fopenmp -std=c++11 dgemm_thread_safety.cpp ../$(LIBNAME) $(EXTRALIB) $(FEXTRALIB) -o dgemm_tester
	./dgemm_tester

clean ::
	rm -f dgemv_tester dgemm_tester
