// Copyright 2011 Google Inc.
// All Rights Reserved.
//
//
// Generalizes the plethora of ARM flavors available to an easier to manage set
// Defs reference is at https://wiki.edubuntu.org/ARM/Thumb2PortingHowto

#ifndef ARM_INSTRUCTION_SET_SELECT_H_
#define ARM_INSTRUCTION_SET_SELECT_H_

#if defined(__ARM_ARCH_7__) || defined(__ARM_ARCH_7R__) || defined(__ARM_ARCH_7A__)
#define ARMV7 1
#endif

#if defined(ARMV7) || defined(__ARM_ARCH_6__) || defined(__ARM_ARCH_6J__) ||                 \
        defined(__ARM_ARCH_6K__) || defined(__ARM_ARCH_6Z__) || defined(__ARM_ARCH_6T2__) || \
        defined(__ARM_ARCH_6ZK__)
#define ARMV6 1
#endif

#if defined(ARMV6) || defined(__ARM_ARCH_5T__) || defined(__ARM_ARCH_5E__) || \
        defined(__ARM_ARCH_5TE__) || defined(__ARM_ARCH_5TEJ__)
#define ARMV5 1
#endif

#if defined(ARMV5) || defined(__ARM_ARCH_4__) || defined(__ARM_ARCH_4T__)
#define ARMV4 1
#endif

#if defined(ARMV4) || defined(__ARM_ARCH_3__) || defined(__ARM_ARCH_3M__)
#define ARMV3 1
#endif

#if defined(ARMV3) || defined(__ARM_ARCH_2__)
#define ARMV2 1
#endif

#endif // ARM_INSTRUCTION_SET_SELECT_H_
