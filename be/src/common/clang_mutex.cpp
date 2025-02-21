#include "clang_mutex.h"

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic warning "-Wthread-safety"
#endif

class BankAccount {
private:
    SharedMutex mu;
    int balance GUARDED_BY(mu);

    void foo1() {
        balance += 1; // Writing variable 'balance' requires holding mutex 'mu' exclusively
    }

    void foo2() REQUIRES_SHARED(mu) {}

    void foo3() REQUIRES(mu) {}
    void test() {
        {
            SharedLock lock(mu);
            foo2();
            foo3();
        }
        {
            SharedLock lock(mu);
            foo3(); // Calling function 'foo3' requires holding mutex 'mu' exclusively
        }
        {
            UniqueLock lock(mu);
            foo3();
            foo2();
            foo2();
        }
        {
            UniqueLock lock(mu);
            foo2();
        }
        foo3(); //Calling function 'foo3' requires holding mutex 'mu' exclusively
        foo2(); // Calling function 'foo2' requires holding mutex 'mu'
    }
};
