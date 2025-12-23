/*******************************************************************************
Copyright (c) 2023, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/

/************** Dgemm Kernel 16x4 ****************/
.macro KERNEL2x16x4
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D2,  U10, U12, D2
    xvfmadd.d  D3,  U11, U12, D3

    xvld     U2,   A0,    0x40
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvld     U3,   A0,    0x60
    xvfmadd.d  D6,  U10, U13, D6
    xvfmadd.d  D7,  U11, U13, D7

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D8,  U8, U14, D8
    xvfmadd.d  D9,  U9, U14, D9

    preld      0,   B0, B_PRE
    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D10, U10, U14, D10
    xvfmadd.d  D11, U11, U14, D11

    preld      0,   A0, A_PRE
    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D12, U8, U15, D12
    xvfmadd.d  D13, U9, U15, D13

    preld      0,   A0, A_PRE + 0x40
    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D14, U10, U15, D14
    xvfmadd.d  D15, U11, U15, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvld     U9,   A0,    0x20
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvld     U10,   A0,    0x40
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvld     U11,   A0,    0x60
    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7

    xvldrepl.d U12,  B0, 0x00
    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9

    preld      0,   B0, B_PRE
    xvldrepl.d U13,  B0, 0x08
    xvfmadd.d  D10, U2, U6, D10
    xvfmadd.d  D11, U3, U6, D11

    preld      0,   A0, A_PRE
    xvldrepl.d U14,  B0, 0x10
    xvfmadd.d  D12, U0, U7, D12
    xvfmadd.d  D13, U1, U7, D13

    preld      0,   A0, A_PRE + 0x40
    xvldrepl.d U15,  B0, 0x18
    xvfmadd.d  D14, U2, U7, D14
    xvfmadd.d  D15, U3, U7, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x16x4_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D2,  U10, U12, D2
    xvfmadd.d  D3,  U11, U12, D3

    xvld     U2,   A0,    0x40
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvld     U3,   A0,    0x60
    xvfmadd.d  D6,  U10, U13, D6
    xvfmadd.d  D7,  U11, U13, D7

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D8,  U8, U14, D8
    xvfmadd.d  D9,  U9, U14, D9

    preld      0,   B0, B_PRE
    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D10, U10, U14, D10
    xvfmadd.d  D11, U11, U14, D11

    preld      0,   A0, A_PRE
    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D12, U8, U15, D12
    xvfmadd.d  D13, U9, U15, D13

    preld      0,   A0, A_PRE + 0x40
    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D14, U10, U15, D14
    xvfmadd.d  D15, U11, U15, D15

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x20

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7

    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9

    preld      0,   B0, B_PRE
    xvfmadd.d  D10, U2, U6, D10
    xvfmadd.d  D11, U3, U6, D11

    preld      0,   A0, A_PRE
    xvfmadd.d  D12, U0, U7, D12
    xvfmadd.d  D13, U1, U7, D13

    preld      0,   A0, A_PRE + 0x40
    xvfmadd.d  D14, U2, U7, D14
    xvfmadd.d  D15, U3, U7, D15
.endm

.macro KERNEL8x16x4
.rept 4
    KERNEL2x16x4
.endr
.endm

.macro KERNEL8x16x4_END
.rept 3
    KERNEL2x16x4
.endr
    KERNEL2x16x4_END
.endm

.macro KERNEL2x8x4
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D8,  U8, U14, D8
    xvfmadd.d  D9,  U9, U14, D9

    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D12, U8, U15, D12
    xvfmadd.d  D13, U9, U15, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20

    xvldrepl.d U12, B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U13, B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvldrepl.d U14, B0, 0x10
    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9

    xvldrepl.d U15,  B0, 0x18
    xvfmadd.d  D12, U0, U7, D12
    xvfmadd.d  D13, U1, U7, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x8x4_END
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D8,  U8, U14, D8
    xvfmadd.d  D9,  U9, U14, D9

    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D12, U8, U15, D12
    xvfmadd.d  D13, U9, U15, D13

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x20

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9

    xvfmadd.d  D12, U0, U7, D12
    xvfmadd.d  D13, U1, U7, D13
.endm

.macro KERNEL8x8x4
.rept 4
    KERNEL2x8x4
.endr
.endm

.macro KERNEL8x8x4_END
.rept 3
    KERNEL2x8x4
.endr
    KERNEL2x8x4_END
.endm

.macro KERNEL2x4x4
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U8, U12, D0

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D8,  U8, U14, D8

    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D12, U8, U15, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    xvld     U8,   A0,    0x00

    xvldrepl.d U12, B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U13, B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4

    xvldrepl.d U14, B0, 0x10
    xvfmadd.d  D8,  U0, U6, D8

    xvldrepl.d U15,  B0, 0x18
    xvfmadd.d  D12, U0, U7, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x4x4_END
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U8, U12, D0

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D8,  U8, U14, D8

    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D12, U8, U15, D12

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x20

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D12, U0, U7, D12
.endm

.macro KERNEL8x4x4
.rept 4
    KERNEL2x4x4
.endr
.endm

.macro KERNEL8x4x4_END
.rept 3
    KERNEL2x4x4
.endr
    KERNEL2x4x4_END
.endm

.macro KERNEL2x2x4
    xvldrepl.d     U0,   A0,    0x00
    xvldrepl.d     U1,   A0,    0x08

    xvfmadd.d   D0,     U8,     U12,    D0
    xvfmadd.d   D1,     U9,     U12,    D1

    xvld    U4,  B0, 0x00
    addi.d  A0,  A0, 0x10
    addi.d  B0,  B0, 0x20

    xvldrepl.d     U8,   A0,    0x00
    xvldrepl.d     U9,   A0,    0x08

    xvfmadd.d   D0,     U0,     U4,    D0
    xvfmadd.d   D1,     U1,     U4,    D1

    xvld       U12, B0, 0x00
    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x2x4_END
    xvldrepl.d     U0,   A0,    0x00
    xvldrepl.d     U1,   A0,    0x08

    xvfmadd.d   D0,     U8,     U12,    D0
    xvfmadd.d   D1,     U9,     U12,    D1

    xvld    U4,  B0, 0x00
    addi.d  A0,  A0, 0x10
    addi.d  B0,  B0, 0x20

    xvfmadd.d   D0,     U0,     U4,    D0
    xvfmadd.d   D1,     U1,     U4,    D1
.endm

.macro KERNEL8x2x4
.rept 4
    KERNEL2x2x4
.endr
.endm

.macro KERNEL8x2x4_END
.rept 3
    KERNEL2x2x4
.endr
    KERNEL2x2x4_END
.endm

.macro KERNEL2x1x4
    xvldrepl.d  U0,     A0,     0x00
    xvfmadd.d   D0,     U8,     U12,    D0
    xvld        U4,     B0,     0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

    xvldrepl.d  U8,     A0,     0x00
    xvfmadd.d   D0,     U0,     U4,     D0
    xvld        U12,    B0,     0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20
.endm

.macro KERNEL2x1x4_END
    xvldrepl.d  U0,     A0,     0x00
    xvfmadd.d   D0,     U8,     U12,    D0
    xvld        U4,     B0,     0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x20

    xvfmadd.d   D0,     U0,     U4,     D0
.endm

.macro KERNEL8x1x4
.rept 4
    KERNEL2x1x4
.endr
.endm

.macro KERNEL8x1x4_END
.rept 3
    KERNEL2x1x4
.endr
    KERNEL2x1x4_END
.endm

.macro KERNEL2x16x2
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D2,  U10, U12, D2
    xvfmadd.d  D3,  U11, U12, D3

    xvld     U2,   A0,    0x40
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvld     U3,   A0,    0x60
    xvfmadd.d  D6,  U10, U13, D6
    xvfmadd.d  D7,  U11, U13, D7

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvld     U9,   A0,    0x20
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvld     U10,   A0,    0x40
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvld     U11,   A0,    0x60
    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7

    xvldrepl.d U12,  B0, 0x00
    xvldrepl.d U13,  B0, 0x08

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x16x2_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D2,  U10, U12, D2
    xvfmadd.d  D3,  U11, U12, D3

    xvld     U2,   A0,    0x40
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvld     U3,   A0,    0x60
    xvfmadd.d  D6,  U10, U13, D6
    xvfmadd.d  D7,  U11, U13, D7

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x10

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7
.endm

.macro KERNEL8x16x2
.rept 4
    KERNEL2x16x2
.endr
.endm

.macro KERNEL8x16x2_END
.rept 3
    KERNEL2x16x2
.endr
    KERNEL2x16x2_END
.endm

.macro KERNEL2x8x2
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvld     U9,   A0,    0x20
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvldrepl.d U12,  B0, 0x00
    xvldrepl.d U13,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x8x2_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D4,  U8, U13, D4
    xvfmadd.d  D5,  U9, U13, D5

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x10

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5
.endm

.macro KERNEL8x8x2
.rept 4
    KERNEL2x8x2
.endr
.endm

.macro KERNEL8x8x2_END
.rept 3
    KERNEL2x8x2
 .endr
    KERNEL2x8x2_END
.endm

.macro KERNEL2x4x2
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4

    xvldrepl.d U12,  B0, 0x00
    xvldrepl.d U13,  B0, 0x08

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x4x2_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x10

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4
.endm

.macro KERNEL8x4x2
.rept 4
    KERNEL2x4x2
.endr
.endm

.macro KERNEL8x4x2_END
.rept 3
    KERNEL2x4x2
.endr
    KERNEL2x4x2_END
.endm

.macro KERNEL2x2x2
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4

    xvldrepl.d U12,  B0, 0x00
    xvldrepl.d U13,  B0, 0x08

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x2x2_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x10

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4
.endm

.macro KERNEL8x2x2
.rept 4
    KERNEL2x2x2
.endr
.endm

.macro KERNEL8x2x2_END
.rept 3
    KERNEL2x2x2
.endr
    KERNEL2x2x2_END
.endm

.macro KERNEL2x1x2
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4

    xvldrepl.d U12,  B0, 0x00
    xvldrepl.d U13,  B0, 0x08

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10
.endm

.macro KERNEL2x1x2_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D4,  U8, U13, D4

    xvldrepl.d U4,  B0, 0x00
    xvldrepl.d U5,  B0, 0x08

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D4,  U0, U5, D4
.endm

.macro KERNEL8x1x2
.rept 4
    KERNEL2x1x2
.endr
.endm

.macro KERNEL8x1x2_END
.rept 3
    KERNEL2x1x2
.endr
    KERNEL2x1x2_END
.endm

.macro KERNEL2x16x1
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D2,  U10, U12, D2
    xvfmadd.d  D3,  U11, U12, D3

    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvld     U9,   A0,    0x20
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvld     U10,   A0,    0x40
    xvld     U11,   A0,    0x60

    xvldrepl.d U12,  B0, 0x00

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x16x1_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1

    xvld     U1,   A0,    0x20
    xvfmadd.d  D2,  U10, U12, D2
    xvfmadd.d  D3,  U11, U12, D3

    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x80
    addi.d     B0,  B0, 0x08

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3
.endm

.macro KERNEL8x16x1
.rept 4
    KERNEL2x16x1
.endr
.endm

.macro KERNEL8x16x1_END
.rept 3
    KERNEL2x16x1
.endr
    KERNEL2x16x1_END
.endm

.macro KERNEL2x8x1
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1
    xvld     U1,   A0,    0x20
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvld     U9,   A0,    0x20
    xvldrepl.d U12,  B0, 0x00

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x8x1_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvfmadd.d  D1,  U9, U12, D1
    xvld     U1,   A0,    0x20
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x40
    addi.d     B0,  B0, 0x08

    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
.endm

.macro KERNEL8x8x1
.rept 4
    KERNEL2x8x1
.endr
.endm

.macro KERNEL8x8x1_END
.rept 3
    KERNEL2x8x1
.endr
    KERNEL2x8x1_END
.endm

.macro KERNEL2x4x1
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvldrepl.d U12,  B0, 0x00

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x4x1_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x20
    addi.d     B0,  B0, 0x08

    xvfmadd.d  D0,  U0, U4, D0
.endm

.macro KERNEL8x4x1
.rept 4
    KERNEL2x4x1
.endr
.endm

.macro KERNEL8x4x1_END
.rept 3
    KERNEL2x4x1
.endr
    KERNEL2x4x1_END
.endm

.macro KERNEL2x2x1
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvldrepl.d U12,  B0, 0x00

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x2x1_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x10
    addi.d     B0,  B0, 0x08

    xvfmadd.d  D0,  U0, U4, D0
.endm

.macro KERNEL8x2x1
.rept 4
    KERNEL2x2x1
.endr
.endm

.macro KERNEL8x2x1_END
.rept 3
    KERNEL2x2x1
.endr
    KERNEL2x2x1_END
.endm

.macro KERNEL2x1x1
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    xvld     U8,   A0,    0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvldrepl.d U12,  B0, 0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08
.endm

.macro KERNEL2x1x1_END
    xvld     U0,   A0,    0x00
    xvfmadd.d  D0,  U8, U12, D0
    xvldrepl.d U4,  B0, 0x00

    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    xvfmadd.d  D0,  U0, U4, D0
.endm

.macro KERNEL8x1x1
.rept 4
    KERNEL2x1x1
.endr
.endm

.macro KERNEL8x1x1_END
.rept 3
    KERNEL2x1x1
.endr
    KERNEL2x1x1_END
.endm

.macro dgemm_16x4
.L_dgemm_16x4: // See dgemm_kernel_16x4.S
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    xvldrepl.d     U5, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U5
    xvfmul.d  D5,  U1, U5
    xvfmul.d  D6,  U2, U5
    xvfmul.d  D7,  U3, U5

    xvldrepl.d     U6, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U6
    xvfmul.d  D9,  U1, U6
    xvfmul.d  D10, U2, U6
    xvfmul.d  D11, U3, U6

    xvldrepl.d     U7, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U7
    xvfmul.d  D13, U1, U7
    xvfmul.d  D14, U2, U7
    xvfmul.d  D15, U3, U7

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x80
    PTR_ADDI    B0,  B0, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_L7 */
    beq       ZERO,TL, .L_dgemm_16x4_L7

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20
    xvld     U10,  A0,    0x40
    xvld     U11,  A0,    0x60

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    xvldrepl.d  U14,   B0,  0x10
    xvldrepl.d  U15,   B0,  0x18
    PTR_ADDI     A0,  A0, 0x80
    PTR_ADDI     B0,  B0, 0x20

    beq    ZERO,    TL,  .L_dgemm_16x4_TL1_END
.align 5
.L_dgemm_16x4_TL1:
    KERNEL8x16x4
    PTR_ADDI  TL,    TL,     -1
    blt     ZERO,  TL,     .L_dgemm_16x4_TL1
.L_dgemm_16x4_TL1_END:
    KERNEL8x16x4_END
.L_dgemm_16x4_L7:
    andi      TL,  L,    7
    beq       TL,  ZERO, .L_dgemm_16x4_L0
.align 5
.L_dgemm_16x4_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5
    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7

    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9
    xvfmadd.d  D10, U2, U6, D10
    xvfmadd.d  D11, U3, U6, D11

    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D12, U0, U7, D12
    xvfmadd.d  D13, U1, U7, D13
    xvfmadd.d  D14, U2, U7, D14
    xvfmadd.d  D15, U3, U7, D15

    PTR_ADDI     A0,  A0, 0x80
    PTR_ADDI     B0,  B0, 0x20

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_16x4_L71
.L_dgemm_16x4_L0:
    // Load C
    GLD xv, , U0,  C0, 0x00, U1,  C0, 0x20, U2,  C0, 0x40, U3,  C0, 0x60
    GLD xv, , U4,  C1, 0x00, U5,  C1, 0x20, U6,  C1, 0x40, U7,  C1, 0x60
    GLD xv, , U8,  C2, 0x00, U9,  C2, 0x20, U10, C2, 0x40, U11, C2, 0x60
    GLD xv, , U12, C3, 0x00, U13, C3, 0x20, U14, C3, 0x40, U15, C3, 0x60
    GSUB xvf, d, U0,  U0,  D0,  U1,  U1,  D1,  U2,  U2,  D2,  U3,  U3,  D3,  \
                 U4,  U4,  D4,  U5,  U5,  D5,  U6,  U6,  D6,  U7,  U7,  D7,  \
                 U8,  U8,  D8,  U9,  U9,  D9,  U10, U10, D10, U11, U11, D11, \
                 U12, U12, D12, U13, U13, D13, U14, U14, D14, U15, U15, D15
.endm

.macro dgemm_1x4
.L_dgemm_1x4: // See dgemm_kernel_16x4.S
    xvldrepl.d  U0,     A0,     0x00
    xvld        U4,     B0,     0x00
    xvfmul.d    D0,     U0,     U4

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x08
    PTR_ADDI    B0,  B0, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M1_L7 */
    beq       ZERO,TL, .L_dgemm_1x4_M1_L7
    xvldrepl.d  U8,     A0,     0x00

    PTR_ADDI     TL,  TL,  -1
    xvld       U12, B0,  0x00
    PTR_ADDI     A0,  A0,  0x08
    PTR_ADDI     B0,  B0,  0x20

    beq    ZERO,    TL,  .L_dgemm_1x4_M1_TL1_END
.align 5
.L_dgemm_1x4_M1_TL1:
    KERNEL8x1x4
    PTR_ADDI  TL,     TL,     -1
    blt     ZERO,TL, .L_dgemm_1x4_M1_TL1
.L_dgemm_1x4_M1_TL1_END:
    KERNEL8x1x4_END
.L_dgemm_1x4_M1_L7:
    /* if (!(L & 7)) goto L_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_1x4_M1_L0
.align 5
.L_dgemm_1x4_M1_L71:
    xvldrepl.d  U0,     A0,     0x00
    xvld        U4,     B0,     0x00
    xvfmadd.d   D0,     U0,     U4,    D0

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x08
    PTR_ADDI     B0,  B0, 0x20

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_1x4_M1_L71
.L_dgemm_1x4_M1_L0:
    // Load C
    fld.d       $f0,    C0,     0x00
    fld.d       $f1,    C1,     0x00
    fld.d       $f2,    C2,     0x00
    fld.d       $f3,    C3,     0x00
    xvinsve0.d  U0,     U1,     0x01
    xvinsve0.d  U0,     U2,     0x02
    xvinsve0.d  U0,     U3,     0x03
    GSUB xvf, d, U0, U0, D0
.endm

.macro dgemm_2x4
.L_dgemm_2x4:
    /* Load 2 * 64 from A0 */
    xvldrepl.d     U0,   A0,    0x00
    xvldrepl.d     U1,   A0,    0x08
    xvld    U4,     B0,     0x00
    xvfmul.d    D0,     U0,     U4
    xvfmul.d    D1,     U1,     U4
    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x10
    PTR_ADDI    B0,  B0, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M2_L7 */
    beq       ZERO,TL, .L_dgemm_2x4_M2_L7

    xvldrepl.d     U8,   A0,    0x00
    xvldrepl.d     U9,   A0,    0x08

    PTR_ADDI    TL,  TL,  -1

    xvld       U12, B0, 0x00
    PTR_ADDI     A0,  A0, 0x10
    PTR_ADDI     B0,  B0, 0x20

    beq    ZERO,    TL,  .L_dgemm_2x4_M2_TL1_END
.align 5
.L_dgemm_2x4_M2_TL1:
    KERNEL8x2x4

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_2x4_M2_TL1
.L_dgemm_2x4_M2_TL1_END:
    KERNEL8x2x4_END

.L_dgemm_2x4_M2_L7:
    /* if (!(L & 7)) goto L_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_2x4_M2_L0
.align 5
.L_dgemm_2x4_M2_L71:
    xvldrepl.d     U0,   A0,    0x00
    xvldrepl.d     U1,   A0,    0x08

    xvld    U4,  B0, 0x00

    xvfmadd.d   D0,     U0,     U4,    D0
    xvfmadd.d   D1,     U1,     U4,    D1
    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x10
    PTR_ADDI     B0,  B0, 0x20

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_2x4_M2_L71
.L_dgemm_2x4_M2_L0:
    xvpackev.d  D4,     D1,     D0
    xvpackod.d  D5,     D1,     D0
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
    /* Load C2  */
    xvld      U2,  C2,  0x00
    /* Load C3  */
    xvld      U3,  C3,  0x00

    xvpermi.q   U0, U2, 0x02
    xvpermi.q   U1, U3, 0x02

    GSUB xvf, d, U0, U0, D4, U1, U1, D5
.endm

.macro dgemm_4x4
.L_dgemm_4x4:
    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U5, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U5

    xvldrepl.d     U6, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U6

    xvldrepl.d     U7, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U7

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x20
    PTR_ADDI    B0,  B0, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M4_L7 */
    beq       ZERO,TL, .L_dgemm_4x4_M4_L7

    xvld     U8,   A0,    0x00

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    xvldrepl.d  U14,   B0,  0x10
    xvldrepl.d  U15,   B0,  0x18
    PTR_ADDI     A0,  A0, 0x20
    PTR_ADDI     B0,  B0, 0x20

    beq    ZERO,    TL,  .L_dgemm_4x4_M4_TL1_END
.align 5
.L_dgemm_4x4_M4_TL1: /* TL-- */
    KERNEL8x4x4

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_4x4_M4_TL1
.L_dgemm_4x4_M4_TL1_END:
    KERNEL8x4x4_END
.L_dgemm_4x4_M4_L7:
    /* if (!(L & 7)) goto L_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_4x4_M4_L0
.align 5
.L_dgemm_4x4_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U4,  B0, 0x08
    xvfmadd.d  D4,  U0, U4, D4

    xvldrepl.d U4,  B0, 0x10
    xvfmadd.d  D8,  U0, U4, D8

    xvldrepl.d U4,  B0, 0x18
    xvfmadd.d  D12, U0, U4, D12

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x20
    PTR_ADDI     B0,  B0, 0x20

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_4x4_M4_L71
 .L_dgemm_4x4_M4_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
    /* Load C2  */
    xvld      U2,  C2,  0x00
    /* Load C3  */
    xvld      U3,  C3,  0x00

    GSUB xvf, d, U0, U0, D0, U1, U1, D4, U2, U2, D8, U3, U3, D12
.endm

.macro dgemm_8x4
.L_dgemm_8x4:
    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    xvldrepl.d     U5, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U5
    xvfmul.d  D5,  U1, U5

    xvldrepl.d     U6, B0, 0x10
    /* line 3 */
    xvfmul.d  D8,  U0, U6
    xvfmul.d  D9,  U1, U6

    xvldrepl.d     U7, B0, 0x18
    /* line 4 */
    xvfmul.d  D12, U0, U7
    xvfmul.d  D13, U1, U7

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x40
    PTR_ADDI    B0,  B0, 0x20
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_M8_L7 */
    beq       ZERO,TL, .L_dgemm_8x4_M8_L7

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    xvldrepl.d  U14,   B0,  0x10
    xvldrepl.d  U15,   B0,  0x18
    PTR_ADDI     A0,  A0, 0x40
    PTR_ADDI     B0,  B0, 0x20

    beq    ZERO,    TL,  .L_dgemm_8x4_M8_TL1_END
.align 5
.L_dgemm_8x4_M8_TL1: /* TL-- */
    KERNEL8x8x4

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_8x4_M8_TL1

.L_dgemm_8x4_M8_TL1_END:
    KERNEL8x8x4_END

.L_dgemm_8x4_M8_L7:
    /* if (!(L & 7)) goto L_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_8x4_M8_L0
.align 5
.L_dgemm_8x4_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    xvldrepl.d U6,  B0, 0x10
    xvfmadd.d  D8,  U0, U6, D8
    xvfmadd.d  D9,  U1, U6, D9

    xvldrepl.d U7,  B0, 0x18
    xvfmadd.d  D12, U0, U7, D12
    xvfmadd.d  D13, U1, U7, D13

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x40
    PTR_ADDI     B0,  B0, 0x20

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_8x4_M8_L71
.L_dgemm_8x4_M8_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20

    /* Load C1  */
    xvld      U2,  C1,  0x00
    xvld      U3,  C1,  0x20

    /* Load C2  */
    xvld      U4,  C2,  0x00
    xvld      U5,  C2,  0x20

    /* Load C3  */
    xvld      U6,  C3,  0x00
    xvld      U7,  C3,  0x20

    GSUB xvf, d, U0, U0, D0,  U1, U1, D1, \
                 U2, U2, D4,  U3, U3, D5, \
                 U4, U4, D8,  U5, U5, D9, \
                 U6, U6, D12, U7, U7, D13
.endm

.macro dgemm_4x2
.L_dgemm_4x2:
    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U5, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U5

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x20
    PTR_ADDI    B0,  B0, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_4x2_N3_M4_L7 */
    beq       ZERO,TL, .L_dgemm_4x2_N3_M4_L7

    xvld     U8,   A0,    0x00

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    PTR_ADDI     A0,  A0, 0x20
    PTR_ADDI     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_dgemm_4x2_N3_M4_TL1_END
.align 5
.L_dgemm_4x2_N3_M4_TL1: /* TL-- */
    KERNEL8x4x2

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_4x2_N3_M4_TL1
.L_dgemm_4x2_N3_M4_TL1_END:
    KERNEL8x4x2_END

.L_dgemm_4x2_N3_M4_L7:
    /* if (!(L & 7)) goto L_dgemm_4x2_N3_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_4x2_N3_M4_L0
.align 5
.L_dgemm_4x2_N3_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x20
    PTR_ADDI     B0,  B0, 0x10

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_4x2_N3_M4_L71

.L_dgemm_4x2_N3_M4_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
    GSUB xvf, d, U0, U0, D0, U1, U1, D4
.endm

.macro dgemm_2x2
.L_dgemm_2x2:
    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x10
    PTR_ADDI    B0,  B0, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_2x2_N3_M2_L7 */
    beq       ZERO,TL, .L_dgemm_2x2_N3_M2_L7

    xvld     U8,   A0,    0x00

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    PTR_ADDI     A0,  A0, 0x10
    PTR_ADDI     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_dgemm_2x2_N3_M2_TL1_END
.align 5
.L_dgemm_2x2_N3_M2_TL1: /* TL-- */
    KERNEL8x2x2

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_2x2_N3_M2_TL1
.L_dgemm_2x2_N3_M2_TL1_END:
    KERNEL8x2x2_END

.L_dgemm_2x2_N3_M2_L7:
    /* if (!(L & 7)) goto L_dgemm_2x2_N3_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_2x2_N3_M2_L0
.align 5
.L_dgemm_2x2_N3_M2_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x10
    PTR_ADDI     B0,  B0, 0x10

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_2x2_N3_M2_L71
.L_dgemm_2x2_N3_M2_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    /* Load C1  */
    xvld      U1,  C1,  0x00
    GSUB xvf, d, U0, U0, D0, U1, U1, D4
.endm

.macro dgemm_8x2
.L_dgemm_8x2:
    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    xvldrepl.d     U5, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U5
    xvfmul.d  D5,  U1, U5

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x40
    PTR_ADDI    B0,  B0, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_8x2_N3_M8_L7 */
    beq       ZERO,TL, .L_dgemm_8x2_N3_M8_L7

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    PTR_ADDI     A0,  A0, 0x40
    PTR_ADDI     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_dgemm_8x2_N3_M8_TL1_END
.align 5
.L_dgemm_8x2_N3_M8_TL1: /* TL-- */
    KERNEL8x8x2

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_8x2_N3_M8_TL1
.L_dgemm_8x2_N3_M8_TL1_END:
    KERNEL8x8x2_END

.L_dgemm_8x2_N3_M8_L7:
    /* if (!(L & 7)) goto L_dgemm_8x2_N3_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_8x2_N3_M8_L0
.align 5
.L_dgemm_8x2_N3_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x40
    PTR_ADDI     B0,  B0, 0x10

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_8x2_N3_M8_L71

.L_dgemm_8x2_N3_M8_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    /* Load C1  */
    xvld      U2,  C1,  0x00
    xvld      U3,  C1,  0x20
    GSUB xvf, d, U0, U0, D0, U1, U1, D1, U2, U2, D4, U3, U3, D5
.endm

.macro dgemm_16x2
.L_dgemm_16x2:
    /* Load 16 * 64 from A0
     * U0 = {a3,  a2,  a1,  a0}
     * U1 = {a7,  a6,  a5,  a4}
     * U2 = {a11, a10, a9,  a8}
     * U3 = {a15, a14, a13, a12}
     */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    xvldrepl.d     U5, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U5
    xvfmul.d  D5,  U1, U5
    xvfmul.d  D6,  U2, U5
    xvfmul.d  D7,  U3, U5

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x80
    PTR_ADDI    B0,  B0, 0x10
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_L7 */
    beq       ZERO,TL, .L_dgemm_16x2_N3_L7

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20
    xvld     U10,  A0,    0x40
    xvld     U11,  A0,    0x60

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    PTR_ADDI     A0,  A0, 0x80
    PTR_ADDI     B0,  B0, 0x10

    beq    ZERO,    TL,  .L_dgemm_16x2_N3_TL1_END
.align 5
.L_dgemm_16x2_N3_TL1: /* TL-- */
    KERNEL8x16x2

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_16x2_N3_TL1
.L_dgemm_16x2_N3_TL1_END:
    KERNEL8x16x2_END

.L_dgemm_16x2_N3_L7:
    /* if (!(L & 7)) goto L_dgemm_16x2_N3_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_16x2_N3_L0
.align 5
.L_dgemm_16x2_N3_L71:
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4
    xvfmadd.d  D5,  U1, U5, D5
    xvfmadd.d  D6,  U2, U5, D6
    xvfmadd.d  D7,  U3, U5, D7
    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x80
    PTR_ADDI     B0,  B0, 0x10

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_16x2_N3_L71

.L_dgemm_16x2_N3_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    /* Load C1  */
    xvld      U4,  C1,  0x00
    xvld      U5,  C1,  0x20
    xvld      U6,  C1,  0x40
    xvld      U7,  C1,  0x60
    GSUB xvf, d, U0, U0, D0, U1, U1, D1, U2, U2, D2, U3, U3, D3, \
                 U4, U4, D4, U5, U5, D5, U6, U6, D6, U7, U7, D7
.endm

.macro dgemm_2x1
.L_dgemm_2x1:
    /* Load 2 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x10
    PTR_ADDI    B0,  B0, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_2x1_N1_M2_L7 */
    beq       ZERO,TL, .L_dgemm_2x1_N1_M2_L7

    xvld     U8,   A0,    0x00

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    PTR_ADDI     A0,  A0, 0x10
    PTR_ADDI     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_dgemm_2x1_N1_M2_TL1_END
.align 5
.L_dgemm_2x1_N1_M2_TL1: /* TL-- */
    KERNEL8x2x1

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_2x1_N1_M2_TL1
.L_dgemm_2x1_N1_M2_TL1_END:
    KERNEL8x2x1_END

.L_dgemm_2x1_N1_M2_L7:
    /* if (!(L & 7)) goto L_dgemm_2x1_N1_M2_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_2x1_N1_M2_L0
.align 5
.L_dgemm_2x1_N1_M2_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x10
    PTR_ADDI     B0,  B0, 0x08

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_2x1_N1_M2_L71
.L_dgemm_2x1_N1_M2_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    GSUB xvf, d, U0, U0, D0
.endm

.macro dgemm_4x1
.L_dgemm_4x1:
    /* Load 4 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x20
    PTR_ADDI    B0,  B0, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_4x1_N1_M4_L7 */
    beq       ZERO,TL, .L_dgemm_4x1_N1_M4_L7

    xvld     U8,   A0,    0x00

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    PTR_ADDI     A0,  A0, 0x20
    PTR_ADDI     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_dgemm_4x1_N1_M4_TL1_END
.align 5
.L_dgemm_4x1_N1_M4_TL1: /* TL-- */
    KERNEL8x4x1

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_4x1_N1_M4_TL1
.L_dgemm_4x1_N1_M4_TL1_END:
    KERNEL8x4x1_END

.L_dgemm_4x1_N1_M4_L7:
    /* if (!(L & 7)) goto L_dgemm_4x1_N1_M4_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_4x1_N1_M4_L0
.align 5
.L_dgemm_4x1_N1_M4_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x20
    PTR_ADDI     B0,  B0, 0x08

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_4x1_N1_M4_L71
.L_dgemm_4x1_N1_M4_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    GSUB xvf, d, U0, U0, D0
.endm

.macro dgemm_8x1
.L_dgemm_8x1:
    /* Load 8 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x40
    PTR_ADDI    B0,  B0, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_8x1_N1_M8_L7 */
    beq       ZERO,TL, .L_dgemm_8x1_N1_M8_L7

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    PTR_ADDI     A0,  A0, 0x40
    PTR_ADDI     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_dgemm_8x1_N1_M8_TL1_END
.align 5
.L_dgemm_8x1_N1_M8_TL1: /* TL-- */
    KERNEL8x8x1

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_8x1_N1_M8_TL1

.L_dgemm_8x1_N1_M8_TL1_END:
    KERNEL8x8x1_END

.L_dgemm_8x1_N1_M8_L7:
    /* if (!(L & 7)) goto L_dgemm_8x1_N1_M8_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_8x1_N1_M8_L0
.align 5
.L_dgemm_8x1_N1_M8_L71:
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x40
    PTR_ADDI     B0,  B0, 0x08

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_8x1_N1_M8_L71
.L_dgemm_8x1_N1_M8_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    GSUB xvf, d, U0, U0, D0, U1, U1, D1
.endm

.macro dgemm_16x1
.L_dgemm_16x1:
    /* Load 16 * 64 from A0
     * U0 = {a3,  a2,  a1,  a0}
     * U1 = {a7,  a6,  a5,  a4}
     * U2 = {a11, a10, a9,  a8}
     * U3 = {a15, a14, a13, a12}
     */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4
    xvfmul.d  D1,  U1, U4
    xvfmul.d  D2,  U2, U4
    xvfmul.d  D3,  U3, U4

    /* Add stride for A0 and B0 */
    PTR_ADDI    A0,  A0, 0x80
    PTR_ADDI    B0,  B0, 0x08
    /* Reduce L */
    PTR_ADDI    L,   L,  -1
    PTR_SRAI    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_dgemm_16x1_N1_L7 */
    beq       ZERO,TL, .L_dgemm_16x1_N1_L7

    xvld     U8,   A0,    0x00
    xvld     U9,   A0,    0x20
    xvld     U10,  A0,    0x40
    xvld     U11,  A0,    0x60

    PTR_ADDI    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    PTR_ADDI     A0,  A0, 0x80
    PTR_ADDI     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_dgemm_16x1_N1_TL1_END
.align 5
.L_dgemm_16x1_N1_TL1: /* TL-- */
    KERNEL8x16x1

    PTR_ADDI    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_16x1_N1_TL1
.L_dgemm_16x1_N1_TL1_END:
    KERNEL8x16x1_END

.L_dgemm_16x1_N1_L7:
    /* if (!(L & 7)) goto L_dgemm_16x1_N1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_16x1_N1_L0
.align 5
.L_dgemm_16x1_N1_L71:
    /* Load 16 * 64 from A0 */
    xvld     U0,   A0,    0x00
    xvld     U1,   A0,    0x20
    xvld     U2,   A0,    0x40
    xvld     U3,   A0,    0x60

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0
    xvfmadd.d  D1,  U1, U4, D1
    xvfmadd.d  D2,  U2, U4, D2
    xvfmadd.d  D3,  U3, U4, D3

    /* Add stride for A0, B0 */
    PTR_ADDI     A0,  A0, 0x80
    PTR_ADDI     B0,  B0, 0x08

    PTR_ADDI     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_16x1_N1_L71
.L_dgemm_16x1_N1_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    xvld      U1,  C0,  0x20
    xvld      U2,  C0,  0x40
    xvld      U3,  C0,  0x60
    GSUB xvf, d, U0, U0, D0, U1, U1, D1, U2, U2, D2, U3, U3, D3
.endm

.macro dgemm_1x2
.L_dgemm_1x2: // See dgemm_kernel_16x4.S
    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    xvldrepl.d     U4, B0, 0x08
    /* line 2 */
    xvfmul.d  D4,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x10
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N3_M1_L7 */
    beq       ZERO,TL, .L_dgemm_1x2_N3_M1_L7

    xvld     U8,   A0,    0x00

    addi.d    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    xvldrepl.d  U13,   B0,  0x08
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10
    beq    ZERO,    TL,  .L_dgemm_1x2_N3_M1_TL1_END
.L_dgemm_1x2_N3_M1_TL1: /* TL-- */
    KERNEL8x1x2
    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_dgemm_1x2_N3_M1_TL1
.L_dgemm_1x2_N3_M1_TL1_END:
    KERNEL8x1x2_END
.L_dgemm_1x2_N3_M1_L7:
    /* if (!(L & 7)) goto L_dgemm_1x2_N3_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_dgemm_1x2_N3_M1_L0
.L_dgemm_1x2_N3_M1_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    xvldrepl.d U5,  B0, 0x08
    xvfmadd.d  D4,  U0, U5, D4

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x10

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_dgemm_1x2_N3_M1_L71
.L_dgemm_1x2_N3_M1_L0:
    xvld      U0,  C0,  0x00
    xvld      U1,  C1,  0x00
    xvinsve0.d  U0,     U1,     0x01
    xvinsve0.d  D0,     D4,     0x01
    GSUB xvf, d, U0, U0, D0
.endm

.macro dgemm_1x1
.L_dgemm_1x1:
    /* Load 1 * 64 from A0 */
    xvld     U0,   A0,    0x00

    xvldrepl.d     U4, B0, 0x00
    /* line 1 */
    xvfmul.d  D0,  U0, U4

    /* Add stride for A0 and B0 */
    addi.d    A0,  A0, 0x08
    addi.d    B0,  B0, 0x08
    /* Reduce L */
    addi.d    L,   L,  -1
    srai.d    TL,  L,  3  /* TL = (L-1) >> 3 */
    /* if (TL < 1) goto L_N1_M1_L7 */
    beq       ZERO,TL, .L_N1_M1_L7

    xvld     U8,   A0,    0x00

    addi.d    TL,  TL,  -1

    xvldrepl.d  U12,   B0,  0x00
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    beq    ZERO,    TL,  .L_N1_M1_TL1_END
.L_N1_M1_TL1: /* TL-- */
    KERNEL8x1x1
    addi.d    TL,  TL, -1 /* TL-- */
    blt       ZERO,TL, .L_N1_M1_TL1
.L_N1_M1_TL1_END:
    KERNEL8x1x1_END
.L_N1_M1_L7:
    /* if (!(L & 7)) goto L_N1_M1_L0 */
    andi      TL,  L,   7
    beq       TL,  ZERO,.L_N1_M1_L0

.L_N1_M1_L71:
    xvld     U0,   A0,    0x00

    xvldrepl.d U4,  B0, 0x00
    xvfmadd.d  D0,  U0, U4, D0

    /* Add stride for A0, B0 */
    addi.d     A0,  A0, 0x08
    addi.d     B0,  B0, 0x08

    addi.d     TL,  TL, -1
    blt        ZERO,TL, .L_N1_M1_L71
.L_N1_M1_L0:
    /* Load C0  */
    xvld      U0,  C0,  0x00
    GSUB xvf, d, U0, U0, D0
.endm
