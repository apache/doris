#define ASSEMBLER
#include "common.h"
/*
	.file	"caxpy.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl caxpy_k
	.type	caxpy_k, @function
*/

	PROLOGUE

#if _CALL_ELF ==2
#ifdef CONJ
caxpyc_k:
#else
caxpy_k:
#endif
#endif
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
#if _CALL_ELF ==2
#ifdef CONJ
	.localentry	caxpyc_k,.-caxpyc_k
#else
	.localentry	caxpy_k,.-caxpy_k
#endif
#endif
	mr. 7,3
	ble 0,.L33
	cmpdi 7,9,1
	beq 7,.L41
.L3:
	mtctr 7
	ld 7,96(1)
	sldi 9,9,3
	sldi 7,7,3
	.p2align 4,,15
.L14:
	lfs 10,4(8)
	lfs 11,0(8)
	lfs 12,0(10)
	lfs 0,4(10)
	fmuls 10,2,10
#ifdef CONJ	
	fmadds 11,11,1,10
#else	
	fmsubs 11,11,1,10
#endif
	fadds 12,12,11
	stfs 12,0(10)
	lfs 11,0(8)
	lfs 12,4(8)
	add 8,8,9
	fmuls 11,2,11
#ifdef CONJ	
	fmsubs 12,12,1,11
	fsubs 0,0,12
#else	
	fmadds 12,12,1,11
	fadds 0,0,12
#endif
	stfs 0,4(10)
	add 10,10,7
	bdnz .L14
.L33:
	li 3,0
	blr
	.p2align 4,,15
.L41:
	ld 6,96(1)
	cmpdi 7,6,1
	bne 7,.L3
	rldicr. 4,7,0,59
	std 31,-8(1)
	li 11,0
	bne 0,.L42
.L4:
	addi 6,11,8
	subf 0,4,7
	sldi 6,6,2
	addi 9,6,-32
	add 5,10,6
	add 3,8,9
	add 6,8,6
	subfc 5,5,3
	add 9,10,9
	subfe 5,5,5
	subfc 6,6,9
	subfe 31,31,31
	addi 6,5,1
	addi 5,31,1
	or 6,6,5
	rlwinm 6,6,0,0xff
	cmpwi 7,6,0
	beq 7,.L7
	sradi 6,4,63
	srdi 5,7,63
	subfc 31,7,4
	adde 6,5,6
	subfic 31,0,3
	subfe 31,31,31
	xori 6,6,0x1
	neg 31,31
	and 6,6,31
	rlwinm 6,6,0,0xff
	cmpwi 7,6,0
	beq 7,.L7
	cmpd 7,4,7
	li 6,1
	blt 7,.L43
.L9:
	addi 0,7,-1
	subf 0,4,0
	subfic 0,0,3
	subfe 31,31,31
	addi 0,31,1
	rlwinm 0,0,0,0xff
	cmpwi 7,0,0
	bne 7,.L10
	sradi 0,4,63
	subfc 31,7,4
	adde 5,5,0
	rlwinm 5,5,0,0xff
	cmpwi 7,5,0
	bne 7,.L10
	addi 0,6,-1
	addis 31,2,.LC3@toc@ha
	std 30,-16(1)
	xscvdpspn 12,1
	xscvdpspn 11,2
	srdi. 30,0,2
	addis 6,2,.LC2@toc@ha
	addi 6,6,.LC2@toc@l
	mtctr 30
	addi 31,31,.LC3@toc@l
	lxvd2x 42,0,6
	li 5,16
	li 6,0
	lxvd2x 41,0,31
	xxspltw 12,12,0
	xxspltw 11,11,0
	xxpermdi 42,42,42,2
	xxpermdi 41,41,41,2
	beq 0,.L44
	.p2align 4,,15
.L11:
#ifdef CONJ
	lxvd2x 44,3,6
	lxvd2x 45,3,5
	lxvd2x 33,9,6
	lxvd2x 0,9,5
	xxpermdi 44,44,44,2
	xxpermdi 45,45,45,2
	xxpermdi 32,33,33,2
	xxpermdi 33,0,0,2
	vperm 11,13,12,10
	vperm 13,13,12,9
	vperm 12,1,0,10
	vperm 1,1,0,9
	xvmulsp 0,11,43
	xvmulsp 32,11,45
	xvmsubmsp 45,12,0
	xvmaddasp 32,12,43
	xvaddsp 44,32,44
	xvsubsp 32,33,45
	vmrglw 1,0,12
	vmrghw 0,0,12
#else
	lxvd2x 45,3,6
	lxvd2x 33,3,5
	lxvd2x 43,9,6
	lxvd2x 0,9,5
	xxpermdi 45,45,45,2
	xxpermdi 33,33,33,2
	xxpermdi 32,43,43,2
	xxpermdi 43,0,0,2
	vperm 12,1,13,10
	vperm 1,1,13,9
	vperm 13,11,0,10
	vperm 11,11,0,9
	xvmulsp 0,11,44
	xvmulsp 32,11,33
	xvmaddmsp 33,12,0
	xvmsubasp 32,12,44
	xvaddsp 45,32,45
	xvaddsp 32,33,43
	vmrglw 1,0,13
	vmrghw 0,0,13
#endif
	xxpermdi 0,33,33,2
	xxpermdi 32,32,32,2
	stxvd2x 0,9,6
	addi 6,6,32
	stxvd2x 32,9,5
	addi 5,5,32
	bdnz .L11
	rldicr 0,0,0,61
	ld 30,-16(1)
	sldi 9,0,1
	add 4,4,0
	add 11,11,9
.L10:
	sldi 6,11,2
	addi 9,4,1
	addi 5,6,4
	cmpd 7,7,9
	lfsx 12,8,6
	lfsx 0,10,6
	addi 9,11,2
	lfsx 11,8,5
	fmuls 11,2,11
#ifdef CONJ	
	fmadds 12,12,1,11
#else	
	fmsubs 12,12,1,11
#endif	
	fadds 0,0,12
	stfsx 0,10,6
	lfsx 11,8,6
	lfsx 12,8,5
	lfsx 0,10,5
	fmuls 11,2,11
#ifdef CONJ	
	fmsubs 12,12,1,11
	fsubs 0,0,12
#else	
	fmadds 12,12,1,11
	fadds 0,0,12
#endif
	stfsx 0,10,5
	ble 7,.L39
	sldi 9,9,2
	addi 6,4,2
	addi 5,9,4
	cmpd 7,7,6
	lfsx 12,8,9
	lfsx 0,10,9
	addi 6,11,4
	lfsx 11,8,5
	fmuls 11,2,11
#ifdef CONJ	
	fmadds 12,1,12,11
#else	
	fmsubs 12,1,12,11
#endif	
	fadds 0,0,12
	stfsx 0,10,9
	lfsx 11,8,9
	lfsx 12,8,5
	lfsx 0,10,5
	fmuls 11,2,11
#ifdef CONJ
	fmsubs 12,1,12,11
	fsubs 0,0,12
#else
	fmadds 12,1,12,11
	fadds  0,0,12
#endif
	stfsx 0,10,5
	ble 7,.L39
	sldi 6,6,2
	addi 4,4,3
	addi 5,6,4
	cmpd 7,7,4
	lfsx 12,8,6
	lfsx 0,10,6
	addi 9,11,6
	lfsx 11,8,5
	fmuls 11,2,11
#ifdef CONJ	
	fmadds 12,1,12,11
#else
	fmsubs 12,1,12,11
#endif
	fadds 0,0,12
	stfsx 0,10,6
	lfsx 11,8,6
	lfsx 12,8,5
	lfsx 0,10,5
	fmuls 11,2,11
#ifdef CONJ	
	fmsubs 12,1,12,11
	fsubs 0,0,12
#else	
	fmadds 12,1,12,11
	fadds 0,0,12
#endif	
	stfsx 0,10,5
	ble 7,.L39
	sldi 9,9,2
	ld 31,-8(1)
	addi 7,9,4
	lfsx 12,8,9
	lfsx 0,10,9
	lfsx 11,8,7
	fmuls 11,2,11
#ifdef CONJ	
	fmadds 12,1,12,11
#else	
	fmsubs 12,1,12,11
#endif	
	fadds 0,0,12
	stfsx 0,10,9
	lfsx 11,8,9
	lfsx 12,8,7
	lfsx 0,10,7
	fmuls 2,2,11
#ifdef CONJ	
	fmsubs 1,1,12,2
	fsubs 1,0,1
#else	
	fmadds 1,1,12,2
	fadds 1,0,1
#endif	
	stfsx 1,10,7
	b .L33
.L43:
	mr 6,0
	b .L9
.L7:
	addi 10,4,1
	cmpd 7,10,7
	subf 10,4,7
	mtctr 10
	bgt 7,.L26
	li 10,-1
	rldicr 10,10,0,0
	cmpd 7,7,10
	beq 7,.L26
	.p2align 4,,15
.L13:
	lfs 10,4(3)
	lfs 11,0(3)
	addi 9,9,8
	addi 3,3,8
	lfs 12,-8(9)
	lfs 0,-4(9)
	fmuls 10,2,10
#ifdef CONJ	
	fmadds 11,1,11,10
#else	
	fmsubs 11,1,11,10
#endif	
	fadds 12,12,11
	stfs 12,-8(9)
	lfs 11,-8(3)
	lfs 12,-4(3)
	fmuls 11,2,11
#ifdef CONJ	
	fmsubs 12,1,12,11
	fsubs 0,0,12
#else	
	fmadds 12,1,12,11
	fadds 0,0,12
#endif	
	stfs 0,-4(9)
	bdnz .L13
.L39:
	ld 31,-8(1)
	b .L33
.L42:
#ifdef CONJ
	fneg 0,1
	xxpermdi 32,1,1,0
	addis 9,2,.LANCHOR0@toc@ha
	std 28,-32(1)
	sradi. 28,4,1
	addi 9,9,.LANCHOR0@toc@l
	xscvdpspn 5,2
	xvcvdpsp 32,32
	lxvd2x 12,0,9
	xxpermdi 39,0,0,0
	xxspltw 5,5,0
	xvcvdpsp 39,39
#else
	fneg 0,2
	xxpermdi 39,2,2,0
	addis 9,2,.LANCHOR0@toc@ha
	std 28,-32(1)
	sradi. 28,4,1
	addi 9,9,.LANCHOR0@toc@l
	xscvdpspn 5,1
	xvcvdpsp 39,39
	lxvd2x 12,0,9
	xxpermdi 32,0,0,0
	xxspltw 5,5,0
	xvcvdpsp 32,32
#endif
	xxpermdi 12,12,12,2
	vmrgew 7,7,0
	beq 0,.L5
	xxlnor 38,12,12
	std 29,-24(1)
	std 30,-16(1)
	mr 6,8
	mr 9,10
	li 29,0
	li 30,16
	li 31,32
	li 12,48
	li 0,64
	li 11,80
	li 3,96
	li 5,112
	.p2align 4,,15
.L6:
	lxvd2x 6,0,9
	lxvd2x 40,0,6
	addi 29,29,8
	lxvd2x 41,6,30
	lxvd2x 42,6,31
	cmpd 7,28,29
	lxvd2x 43,6,12
	lxvd2x 44,6,0
	lxvd2x 45,6,11
	lxvd2x 33,6,3
	lxvd2x 32,6,5
	lxvd2x 7,9,30
	addi 6,6,128
	lxvd2x 8,9,31
	lxvd2x 9,9,12
	xxpermdi 40,40,40,2
	xxpermdi 6,6,6,2
	lxvd2x 10,9,0
	lxvd2x 11,9,11
	xxpermdi 41,41,41,2
	xxpermdi 42,42,42,2
	lxvd2x 12,9,3
	lxvd2x 0,9,5
	xxpermdi 43,43,43,2
	xxpermdi 44,44,44,2
	xxpermdi 45,45,45,2
	xxpermdi 33,33,33,2
	xxpermdi 32,32,32,2
	xxpermdi 7,7,7,2
	xxpermdi 8,8,8,2
	xxpermdi 9,9,9,2
	xxpermdi 10,10,10,2
	xxpermdi 11,11,11,2
	xxpermdi 12,12,12,2
	xxpermdi 0,0,0,2
#ifndef CONJ	
	xvmaddasp 6,5,40
	xvmaddasp 7,5,41
	xvmaddasp 8,5,42
	xvmaddasp 9,5,43
	xvmaddasp 10,5,44
	xvmaddasp 11,5,45
	xvmaddasp 12,5,33
	xvmaddasp 0,5,32
	vperm 8,8,8,6
	vperm 9,9,9,6
	vperm 10,10,10,6
	vperm 11,11,11,6
	vperm 12,12,12,6
	vperm 13,13,13,6
	vperm 1,1,1,6
	vperm 0,0,0,6
#endif
	xvmaddasp 6,39,40
	xvmaddasp 7,39,41
	xvmaddasp 8,39,42
	xvmaddasp 9,39,43
	xvmaddasp 10,39,44
	xvmaddasp 11,39,45
	xvmaddasp 12,39,33
	xvmaddasp 0,39,32
#ifdef CONJ	
	vperm 8,8,8,6
	vperm 9,9,9,6
	vperm 10,10,10,6
	vperm 11,11,11,6
	vperm 12,12,12,6
	vperm 13,13,13,6
	vperm 1,1,1,6
	vperm 0,0,0,6
	xvmaddasp 6,5,40
	xvmaddasp 7,5,41
	xvmaddasp 8,5,42
	xvmaddasp 9,5,43
	xvmaddasp 10,5,44
	xvmaddasp 11,5,45
	xvmaddasp 12,5,33
	xvmaddasp 0,5,32
#endif
	xxpermdi 6,6,6,2
	xxpermdi 7,7,7,2
	xxpermdi 8,8,8,2
	xxpermdi 9,9,9,2
	stxvd2x 6,0,9
	xxpermdi 10,10,10,2
	stxvd2x 7,9,30
	xxpermdi 11,11,11,2
	stxvd2x 8,9,31
	xxpermdi 12,12,12,2
	stxvd2x 9,9,12
	xxpermdi 0,0,0,2
	stxvd2x 10,9,0
	stxvd2x 11,9,11
	stxvd2x 12,9,3
	stxvd2x 0,9,5
	addi 9,9,128
	bgt 7,.L6
	ld 29,-24(1)
	ld 30,-16(1)
.L5:
	cmpd 7,7,4
	ble 7,.L36
	sldi 11,4,1
	ld 28,-32(1)
	b .L4
.L36:
	ld 28,-32(1)
	ld 31,-8(1)
	b .L33
.L44:
	li 31,1
	mtctr 31
	b .L11
.L26:
	li 10,1
	mtctr 10
	b .L13
	.long 0
	.byte 0,0,0,0,0,4,0,0
#if _CALL_ELF ==2
#ifdef CONJ
	.size	caxpyc_k,.-caxpyc_k
#else
	.size	caxpy_k,.-caxpy_k
#endif
#endif
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
	.section	.rodata.cst16,"aM",@progbits,16
	.align 4
.LC2:
	.byte	31
	.byte	30
	.byte	29
	.byte	28
	.byte	23
	.byte	22
	.byte	21
	.byte	20
	.byte	15
	.byte	14
	.byte	13
	.byte	12
	.byte	7
	.byte	6
	.byte	5
	.byte	4
.LC3:
	.byte	27
	.byte	26
	.byte	25
	.byte	24
	.byte	19
	.byte	18
	.byte	17
	.byte	16
	.byte	11
	.byte	10
	.byte	9
	.byte	8
	.byte	3
	.byte	2
	.byte	1
	.byte	0
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.gnu_attribute 4, 1
	.section	.note.GNU-stack,"",@progbits
