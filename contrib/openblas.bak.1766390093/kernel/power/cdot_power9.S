#define ASSEMBLER
#include "common.h"
/*
.file	"cdot.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl cdot_k
	.type	cdot_k, @function
*/
	PROLOGUE
	
cdot_k:
.LCF0:
0:	mr. 9,3
	ble 0,.L10
	cmpdi 7,5,1
	beq 7,.L18
.L3:
	mtctr 9
	xxlxor 2,2,2
	sldi 5,5,3
	sldi 7,7,3
#ifdef CONJ
	fmr 12,2
#endif
	fmr 8,2
#ifndef CONJ
	fmr 9,2
#endif
	fmr 1,2
	.p2align 4,,15
.L9:
#ifdef CONJ
	lfs 9,0(4)
	lfs 11,0(6)
	lfs 10,4(6)
	lfs 0,4(4)
	add 6,6,7
	add 4,4,5
	fmadds 1,9,11,1
	fmadds 12,9,10,12
	fmadds 8,0,10,8
	fmadds 2,11,0,2
#else
	lfs 10,0(4)
	lfs 12,0(6)
	lfs 11,4(6)
	lfs 0,4(4)
	add 6,6,7
	add 4,4,5
	fmadds 1,10,12,1
	fmadds 8,10,11,8
	fmadds 9,0,11,9
	fmadds 2,12,0,2
#endif
	bdnz .L9
.L7:
#ifdef CONJ
	fsubs 2,12,2
	fadds 1,1,8
#else
	fadds 2,2,8
	fsubs 1,1,9
#endif
	blr
	.p2align 4,,15
.L18:
	cmpdi 7,7,1
	bne 7,.L3
	rldicr. 10,9,0,60
	bne 0,.L19
	xxlxor 2,2,2
	li 8,0
#ifdef CONJ
	fmr 12,2
#endif
	fmr 8,2
#ifndef CONJ
	fmr 9,2
#endif
	fmr 1,2
.L4:
	addi 7,10,1
	sldi 8,8,2
	subf 10,10,9
	cmpd 7,7,9
	mtctr 10
	add 4,4,8
	add 6,6,8
	bgt 7,.L16
	li 10,-1
	rldicr 10,10,0,0
	cmpd 7,9,10
	beq 7,.L16
	.p2align 4,,15
.L8:
#ifdef CONJ
	lfs 9,0(4)
	lfs 11,0(6)
	lfs 10,4(6)
	lfs 0,4(4)
	addi 6,6,8
	addi 4,4,8
	fmadds 1,9,11,1
	fmadds 12,9,10,12
	fmadds 8,0,10,8
	fmadds 2,11,0,2
#else
	lfs 10,0(4)
	lfs 12,0(6)
	lfs 11,4(6)
	lfs 0,4(4)
	addi 6,6,8
	addi 4,4,8
	fmadds 1,10,12,1
	fmadds 8,10,11,8
	fmadds 9,0,11,9
	fmadds 2,12,0,2
#endif
	bdnz .L8
	b .L7
	.p2align 4,,15
.L10:
	xxlxor 1,1,1
	fmr 2,1
	blr
.L19:
	addis 8,2,.LANCHOR0@toc@ha
	sradi. 3,10,1
	xxspltib 42,0
	addi 8,8,.LANCHOR0@toc@l
	lxv 32,0(8)
	beq 0,.L12
	xxlor 6,42,42
	xxlor 4,42,42
	xxlor 0,42,42
	xxlor 7,42,42
	xxlor 5,42,42
	xxlor 3,42,42
	xxlor 12,42,42
	mr 7,4
	mr 8,6
	li 5,0
	.p2align 4,,15
.L6:
	lxv 43,0(8)
	lxv 44,16(8)
	addi 5,5,4
	addi 8,8,64
	addi 7,7,64
	lxv 45,-32(8)
	lxv 33,-16(8)
	lxv 8,-64(7)
	lxv 9,-48(7)
	cmpd 7,3,5
	lxv 10,-32(7)
	lxv 11,-16(7)
	vpermr 6,11,11,0
	vpermr 7,12,12,0
	vpermr 8,13,13,0
	vpermr 9,1,1,0
	xvmaddasp 12,43,8
	xvmaddasp 3,44,9
	xvmaddasp 0,8,38
	xvmaddasp 4,9,39
	xvmaddasp 6,10,40
	xvmaddasp 5,45,10
	xvmaddasp 42,11,41
	xvmaddasp 7,33,11
	bgt 7,.L6
	xvaddsp 12,12,3
	xvaddsp 0,0,4
	xvaddsp 12,12,5
	xvaddsp 0,0,6
	xvaddsp 12,12,7
	xvaddsp 42,0,42
.L5:
#ifdef CONJ
	xxpermdi 8,12,12,2
	xxpermdi 0,42,42,2
	cmpd 7,9,10
	sldi 8,10,1
	xvaddsp 8,8,12
	xvaddsp 0,0,42
	xxsldwi 1,8,8,3
	xxsldwi 12,0,0,3
	xxsldwi 8,8,8,2
	xxsldwi 0,0,0,2
	xscvspdp 1,1
	xscvspdp 12,12
	xscvspdp 8,8
#else
	xxpermdi 9,12,12,2
	xxpermdi 0,42,42,2
	cmpd 7,9,10
	sldi 8,10,1
	xvaddsp 9,9,12
	xvaddsp 0,0,42
	xxsldwi 1,9,9,3
	xxsldwi 2,0,0,3
	xxsldwi 9,9,9,2
	xxsldwi 0,0,0,2
	xscvspdp 8,2
	xscvspdp 1,1
	xscvspdp 9,9
#endif
	xscvspdp 2,0
	bgt 7,.L4
	b .L7
.L12:
	xxlor 12,42,42
	b .L5
.L16:
	li 9,1
	mtctr 9
	b .L8
	.long 0
	.byte 0,0,0,0,0,0,0,0
	.size	cdot_k,.-cdot_k
	.section	.rodata
	.align 4
	.set	.LANCHOR0,. + 0
	.type	swap_mask_arr, @object
	.size	swap_mask_arr, 16
swap_mask_arr:
	.byte	4
	.byte	5
	.byte	6
	.byte	7
	.byte	0
	.byte	1
	.byte	2
	.byte	3
	.byte	12
	.byte	13
	.byte	14
	.byte	15
	.byte	8
	.byte	9
	.byte	10
	.byte	11
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.section	.note.GNU-stack,"",@progbits
