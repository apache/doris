#include <math.h>
#include "libm.h"

int __signgam = 0;
extern __typeof(__signgam) signgam __attribute__((__weak__, __alias__("__signgam")));
