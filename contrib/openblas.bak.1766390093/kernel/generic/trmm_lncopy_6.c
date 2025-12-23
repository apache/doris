/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include <stdio.h>
#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, FLOAT *a, BLASLONG lda, BLASLONG posX, BLASLONG posY, FLOAT *b){

    BLASLONG i, js, ii;
    BLASLONG X;

    FLOAT data01, data02, data05, data06;
    FLOAT *ao1, *ao2, *ao3, *ao4, *ao5, *ao6;

    js = (n / 6);

    if (js > 0){
        do {
            X = posX;

            if (posX <= posY) {
                ao1 = a + posY + (posX + 0) * lda;
                ao2 = a + posY + (posX + 1) * lda;
                ao3 = a + posY + (posX + 2) * lda;
                ao4 = a + posY + (posX + 3) * lda;
                ao5 = a + posY + (posX + 4) * lda;
                ao6 = a + posY + (posX + 5) * lda;
            } else {
                ao1 = a + posX + (posY + 0) * lda;
                ao2 = a + posX + (posY + 1) * lda;
                ao3 = a + posX + (posY + 2) * lda;
                ao4 = a + posX + (posY + 3) * lda;
                ao5 = a + posX + (posY + 4) * lda;
                ao6 = a + posX + (posY + 5) * lda;
            }

        i = (m / 6);
        if (i > 0) {
            do {
                if (X > posY) {
                    for (ii = 0; ii < 6; ii++){

                        b[  0] = *(ao1 +  0);
                        b[  1] = *(ao2 +  0);
                        b[  2] = *(ao3 +  0);
                        b[  3] = *(ao4 +  0);
                        b[  4] = *(ao5 +  0);
                        b[  5] = *(ao6 +  0);

                        ao1 ++;
                        ao2 ++;
                        ao3 ++;
                        ao4 ++;
                        ao5 ++;
                        ao6 ++;
                        b += 6;
                    }

                } else if (X < posY) {
                    ao1 += 6 * lda;
                    ao2 += 6 * lda;
                    ao3 += 6 * lda;
                    ao4 += 6 * lda;
                    ao5 += 6 * lda;
                    ao6 += 6 * lda;
                    b += 36;

                } else {
#ifdef UNIT
                    b[  0] = ONE;
#else
                    b[  0] = *(ao1 +  0);
#endif
                    b[  1] = ZERO;
                    b[  2] = ZERO;
                    b[  3] = ZERO;
                    b[  4] = ZERO;
                    b[  5] = ZERO;

                    b[ 6] = *(ao1 +  1);
#ifdef UNIT
                    b[ 7] = ONE;
#else
                    b[ 7] = *(ao2 +  1);
#endif
                    b[ 8] = ZERO;
                    b[ 9] = ZERO;
                    b[10] = ZERO;
                    b[11] = ZERO;

                    b[12] = *(ao1 +  2);
                    b[13] = *(ao2 +  2);
#ifdef UNIT
                    b[14] = ONE;
#else
                    b[14] = *(ao3 +  2);
#endif
                    b[15] = ZERO;
                    b[16] = ZERO;
                    b[17] = ZERO;

                    b[18] = *(ao1 +  3);
                    b[19] = *(ao2 +  3);
                    b[20] = *(ao3 +  3);
#ifdef UNIT
                    b[21] = ONE;
#else
                    b[21] = *(ao4 +  3);
#endif
                    b[22] = ZERO;
                    b[23] = ZERO;

                    b[24] = *(ao1 +  4);
                    b[25] = *(ao2 +  4);
                    b[26] = *(ao3 +  4);
                    b[27] = *(ao4 +  4);
#ifdef UNIT
                    b[28] = ONE;
#else
                    b[28] = *(ao5 +  4);
#endif
                    b[29] = ZERO;

                    b[30] = *(ao1 +  5);
                    b[31] = *(ao2 +  5);
                    b[32] = *(ao3 +  5);
                    b[33] = *(ao4 +  5);
                    b[34] = *(ao5 +  5);
#ifdef UNIT
                    b[35] = ONE;
#else
                    b[35] = *(ao6 +  5);
#endif
                    ao1 += 6;
                    ao2 += 6;
                    ao3 += 6;
                    ao4 += 6;
                    ao5 += 6;
                    ao6 += 6;
                    b += 36;
                }

                X += 6;
                i --;
            } while (i > 0);
        }

        i = (m % 6);
        if (i) {

            if (X > posY) {
                for (ii = 0; ii < i; ii++){
                    b[  0] = *(ao1 +  0);
                    b[  1] = *(ao2 +  0);
                    b[  2] = *(ao3 +  0);
                    b[  3] = *(ao4 +  0);
                    b[  4] = *(ao5 +  0);
                    b[  5] = *(ao6 +  0);

                    ao1 ++;
                    ao2 ++;
                    ao3 ++;
                    ao4 ++;
                    ao5 ++;
                    ao6 ++;
                    b += 6;
                }

            } else if (X < posY) {

                b += 6 * i;

            } else {
#ifdef UNIT
                b[  0] = ONE;
#else
                b[  0] = *(ao1 +  0);
#endif
                b[  1] = ZERO;
                b[  2] = ZERO;
                b[  3] = ZERO;
                b[  4] = ZERO;
                b[  5] = ZERO;
                b += 6;

                if (i >= 2) {
                    b[  0] = *(ao1 +  1);
#ifdef UNIT
                    b[  1] = ONE;
#else
                    b[  1] = *(ao2 +  1);
#endif
                    b[  2] = ZERO;
                    b[  3] = ZERO;
                    b[  4] = ZERO;
                    b[  5] = ZERO;
                    b += 6;
                }

                if (i >= 3) {
                    b[  0] = *(ao1 +  2);
                    b[  1] = *(ao2 +  2);
#ifdef UNIT
                    b[  2] = ONE;
#else
                    b[  2] = *(ao3 +  2);
#endif
                    b[  3] = ZERO;
                    b[  4] = ZERO;
                    b[  5] = ZERO;
                    b += 6;
                }

                if (i >= 4) {
                    b[  0] = *(ao1 +  3);
                    b[  1] = *(ao2 +  3);
                    b[  2] = *(ao3 +  3);
#ifdef UNIT
                    b[  3] = ONE;
#else
                    b[  3] = *(ao4 +  3);
#endif
                    b[  4] = ZERO;
                    b[  5] = ZERO;
                    b += 6;
                }

                if (i >= 5) {
                    b[  0] = *(ao1 +  4);
                    b[  1] = *(ao2 +  4);
                    b[  2] = *(ao3 +  4);
                    b[  3] = *(ao4 +  4);
#ifdef UNIT
                    b[  4] = ONE;
#else
                    b[  4] = *(ao5 +  4);
#endif
                    b[  5] = ZERO;
                    b += 6;
                }
            }
        }

        posY += 6;
        js --;
        } while (js > 0);
    } /* End of main loop */

    if ((n % 6) & 4){
        X = posX;

        if (posX <= posY) {
            ao1 = a + posY + (posX + 0) * lda;
            ao2 = a + posY + (posX + 1) * lda;
            ao3 = a + posY + (posX + 2) * lda;
            ao4 = a + posY + (posX + 3) * lda;
        } else {
            ao1 = a + posX + (posY + 0) * lda;
            ao2 = a + posX + (posY + 1) * lda;
            ao3 = a + posX + (posY + 2) * lda;
            ao4 = a + posX + (posY + 3) * lda;
        }

        i = (m >> 1);
        if (i > 0) {
        do {
            if (X > posY) {
                for (ii = 0; ii < 2; ii++){

                    b[  0] = *(ao1 +  0);
                    b[  1] = *(ao2 +  0);
                    b[  2] = *(ao3 +  0);
                    b[  3] = *(ao4 +  0);

                    ao1 ++;
                    ao2 ++;
                    ao3 ++;
                    ao4 ++;
                    b += 4;
                }
            } else if (X < posY) {
                ao1 += 2 * lda;
                ao2 += 2 * lda;
                ao3 += 2 * lda;
                ao4 += 2 * lda;
                b += 8;
            } else {
#ifdef UNIT
                b[  0] = ONE;
#else
                b[  0] = *(ao1 +  0);
#endif
                b[  1] = ZERO;
                b[  2] = ZERO;
                b[  3] = ZERO;

                b[  4] = *(ao1 +  1);
#ifdef UNIT
                b[  5] = ONE;
#else
                b[  5] = *(ao2 +  1);
#endif
                b[  6] = ZERO;
                b[  7] = ZERO;

                b[  8] = *(ao1 +  2);
                b[  9] = *(ao2 +  2);
#ifdef UNIT
                b[ 10] = ONE;
#else
                b[ 10] = *(ao3 +  2);
#endif
                b[ 11] = ZERO;

                b[ 12] = *(ao1 +  3);
                b[ 13] = *(ao2 +  3);
                b[ 14] = *(ao3 +  3);
#ifdef UNIT
                b[ 15] = ONE;
#else
                b[ 15] = *(ao4 +  3);
#endif

                ao1 += 4;
                ao2 += 4;
                ao3 += 4;
                ao4 += 4;
                b += 16;
                X += 4;
                i -= 2;
                continue;
            }

            X += 2;
            i --;
            } while (i > 0);
        }

        i = (m & 1);
        if (i) {

            if (X > posY) {
                for (ii = 0; ii < i; ii++){

                    b[  0] = *(ao1 +  0);
                    b[  1] = *(ao2 +  0);
                    b[  2] = *(ao3 +  0);
                    b[  3] = *(ao4 +  0);

                    ao1 ++;
                    ao2 ++;
                    ao3 ++;
                    ao4 ++;
                    b += 4;
                }
            } else if (X < posY) {
                /* ao1 += i * lda;
                ao2 += i * lda;
                ao3 += i * lda;
                ao4 += i * lda; */
                b += 4 * i;
            } else {
#ifdef UNIT
                b[  0] = ONE;
#else
                b[  0] = *(ao1 +  0);
#endif
                b[  1] = ZERO;
                b[  2] = ZERO;
                b[  3] = ZERO;
                b += 4;
            }
        }

        posY += 4;
    }


    if ((n % 6) & 2){
        X = posX;

        if (posX <= posY) {
            ao1 = a + posY + (posX + 0) * lda;
            ao2 = a + posY + (posX + 1) * lda;
        } else {
            ao1 = a + posX + (posY + 0) * lda;
            ao2 = a + posX + (posY + 1) * lda;
        }

        i = (m >> 1);
        if (i > 0) {
            do {
                if (X > posY) {
                    data01 = *(ao1 + 0);
                    data02 = *(ao1 + 1);
                    data05 = *(ao2 + 0);
                    data06 = *(ao2 + 1);

                    b[ 0] = data01;
                    b[ 1] = data05;
                    b[ 2] = data02;
                    b[ 3] = data06;

                    ao1 += 2;
                    ao2 += 2;
                    b += 4;

                } else if (X < posY) {
                    ao1 += 2 * lda;
                    ao2 += 2 * lda;
                    b += 4;
                } else {
#ifdef UNIT
                    data02 = *(ao1 + 1);

                    b[ 0] = ONE;
                    b[ 1] = ZERO;
                    b[ 2] = data02;
                    b[ 3] = ONE;
#else
                    data01 = *(ao1 + 0);
                    data02 = *(ao1 + 1);
                    data06 = *(ao2 + 1);

                    b[ 0] = data01;
                    b[ 1] = ZERO;
                    b[ 2] = data02;
                    b[ 3] = data06;
#endif
                    ao1 += 2;
                    ao2 += 2;

                    b += 4;
                }

                X += 2;
                i --;
            } while (i > 0);
        }

        i = (m & 1);
        if (i) {

            if (X > posY) {
                data01 = *(ao1 + 0);
                data02 = *(ao2 + 0);
                b[ 0] = data01;
                b[ 1] = data02;

                ao1 += 1;
                ao2 += 1;
                b += 2;
            } else if (X < posY) {
                ao1 += lda;
                b += 2;
            } else {
#ifdef UNIT
                data05 = *(ao2 + 0);

                b[ 0] = ONE;
                b[ 1] = data05;
#else
                data01 = *(ao1 + 0);
                data05 = *(ao2 + 0);

                b[ 0] = data01;
                b[ 1] = data05;
#endif
                b += 2;
            }
        }
        posY += 2;
    }

    if ((n % 6) & 1){
        X = posX;

        if (posX <= posY) {
            ao1 = a + posY + (posX + 0) * lda;
        } else {
            ao1 = a + posX + (posY + 0) * lda;
        }

        i = m;
        if (i > 0) {
            do {
                if (X > posY) {
                    data01 = *(ao1 + 0);
                    b[ 0] = data01;
                    b += 1;
                    ao1 += 1;
                } else if (X < posY) {
                    b += 1;
                    ao1 += lda;
                } else {
#ifdef UNIT
                    b[ 0] = ONE;
#else
                    data01 = *(ao1 + 0);
                    b[ 0] = data01;
#endif
                    b += 1;
                    ao1 += 1;
                }

                X ++;
                i --;
            } while (i > 0);
        }

        posY += 1;
    }

    return 0;
}
