/*
	.file	"isamin.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl isamin_k
	.type	isamin_k, @function
*/
#define ASSEMBLER
#include "common.h"

        PROLOGUE

isamin_k:
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
	.localentry	isamin_k,.-isamin_k
	mr. 11,3
	ble 0,.L36
	cmpdi 7,5,0
	li 3,0
	blelr 7
	lfs 0,0(4)
	cmpdi 7,5,1
	stxv 61,-64(1)
	stxv 62,-48(1)
	stxv 63,-32(1)
	fabs 0,0
	beq 7,.L62
	rldicr. 6,11,0,61
	beq 0,.L40
	sldi 8,5,1
	sldi 0,5,2
	neg 12,5
	std 31,-8(1)
	sldi 3,5,4
	sldi 31,5,3
	li 9,0
	li 10,0
	add 5,8,5
	add 7,4,0
	sldi 12,12,2
	sldi 5,5,2
	b .L24
	.p2align 4,,15
.L41:
	mr 10,9
.L25:
	add 7,7,3
	fmr 0,12
.L24:
	lfs 12,0(7)
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L26
	fmr 0,12
	addi 10,9,1
.L26:
	add 8,7,12
	lfsx 12,8,31
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L28
	fmr 0,12
	addi 10,9,2
.L28:
	lfsx 12,8,5
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L30
	fmr 0,12
	addi 10,9,3
.L30:
	addi 9,9,4
	cmpd 7,6,9
	ble 7,.L63
	lfsx 12,8,3
	fabs 12,12
	fcmpu 7,12,0
	blt 7,.L41
	fmr 12,0
	b .L25
	.p2align 4,,15
.L36:
	li 3,0
	blr
	.p2align 4,,15
.L63:
	addi 6,6,-1
	ld 31,-8(1)
	srdi 6,6,2
	addi 6,6,1
	sldi 9,6,2
	mulld 6,0,6
	cmpd 7,11,9
	ble 7,.L33
.L23:
	addi 8,9,1
	sldi 6,6,2
	subf 7,9,11
	cmpd 7,8,11
	mtctr 7
	add 4,4,6
	bgt 7,.L52
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L52
	.p2align 4,,15
.L35:
	lfs 12,0(4)
	add 4,4,0
	fabs 12,12
	fcmpu 7,12,0
	bnl 7,.L34
	fmr 0,12
	mr 10,9
.L34:
	addi 9,9,1
	bdnz .L35
.L33:
	lxv 61,-64(1)
	lxv 62,-48(1)
	addi 3,10,1
	lxv 63,-32(1)
	blr
	.p2align 4,,15
.L62:
	rldicr. 8,11,0,57
	li 10,0
	bne 0,.L64
.L4:
	addi 7,8,1
	sldi 9,8,2
	subf 6,8,11
	cmpd 7,7,11
	mtctr 6
	add 4,4,9
	bgt 7,.L51
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L51
	.p2align 4,,15
.L22:
	lfs 12,0(4)
	addi 4,4,4
	fabs 12,12
	fcmpu 7,0,12
	bng 7,.L21
	fmr 0,12
	mr 10,8
.L21:
	addi 8,8,1
	bdnz .L22
	lxv 61,-64(1)
	lxv 62,-48(1)
	addi 3,10,1
	lxv 63,-32(1)
	blr
	.p2align 4,,15
.L64:
	lxv 0,0(4)
	xxspltib 47,16
	addis 6,2,.LC2@toc@ha
	addis 7,2,.LC3@toc@ha
	addis 10,2,.LC4@toc@ha
	addis 9,2,.LC5@toc@ha
	xxspltib 63,32
	xxspltib 46,64
	addi 6,6,.LC2@toc@l
	addi 10,10,.LC4@toc@l
	addi 7,7,.LC3@toc@l
	std 31,-8(1)
	addi 9,9,.LC5@toc@l
	xxspltib 50,0
	vextsb2w 15,15
	lxv 48,0(6)
	lxv 51,0(10)
	vextsb2w 31,31
	vextsb2w 14,14
	xvabssp 4,0
	lxv 34,0(9)
	lxv 49,0(7)
	mr 9,4
	li 10,0
	xxlor 35,48,48
	xxlor 40,4,4
	b .L6
	.p2align 4,,15
.L65:
	lxv 0,0(9)
	xvabssp 40,0
.L6:
	lxv 0,16(9)
	vadduwm 29,18,31
	lxv 12,240(9)
	addi 10,10,64
	addi 9,9,256
	cmpd 7,8,10
	xvabssp 5,0
	lxv 0,-224(9)
	xvabssp 12,12
	xvabssp 32,0
	lxv 0,-208(9)
	xvcmpgtsp 42,40,5
	xvabssp 9,0
	lxv 0,-192(9)
	xxsel 5,40,5,42
	xvabssp 44,0
	lxv 0,-176(9)
	xvcmpgtsp 62,32,9
	xvabssp 6,0
	lxv 0,-160(9)
	xxsel 9,32,9,62
	xxsel 32,35,49,42
	xvabssp 1,0
	lxv 0,-144(9)
	xxsel 62,51,34,62
	xvcmpgtsp 42,5,9
	xvcmpgtsp 37,44,6
	xvabssp 11,0
	lxv 0,-128(9)
	xxsel 9,5,9,42
	xxsel 42,32,62,42
	xxsel 6,44,6,37
	xxsel 37,35,49,37
	xvabssp 13,0
	lxv 0,-112(9)
	xvcmpgtsp 36,1,11
	xvabssp 7,0
	lxv 0,-96(9)
	xxsel 11,1,11,36
	xxsel 36,51,34,36
	xvabssp 2,0
	lxv 0,-80(9)
	xvcmpgtsp 45,6,11
	xvcmpgtsp 39,13,7
	xvabssp 10,0
	lxv 0,-64(9)
	xxsel 7,13,7,39
	xxsel 39,35,49,39
	xvabssp 3,0
	lxv 0,-48(9)
	xvcmpgtsp 38,2,10
	xvabssp 8,0
	lxv 0,-32(9)
	xxsel 10,2,10,38
	xxsel 38,51,34,38
	xvabssp 0,0
	xvcmpgtsp 43,7,10
	xvcmpgtsp 41,3,8
	xvcmpgtsp 33,0,12
	xxsel 8,3,8,41
	xxsel 41,35,49,41
	xxsel 0,0,12,33
	xxsel 40,51,34,33
	xxsel 12,6,11,45
	xxsel 11,7,10,43
	xvcmpgtsp 33,8,0
	xxsel 45,37,36,45
	xvcmpgtsp 32,9,12
	xxsel 43,39,38,43
	vadduwm 13,13,15
	xxsel 0,8,0,33
	xxsel 33,41,40,33
	xxsel 12,9,12,32
	xxsel 32,42,45,32
	xvcmpgtsp 44,11,0
	vadduwm 1,1,15
	vadduwm 0,18,0
	vadduwm 18,18,14
	xxsel 0,11,0,44
	xxsel 33,43,33,44
	xvcmpgtsp 45,12,0
	vadduwm 1,29,1
	xxsel 0,12,0,45
	xxsel 32,32,33,45
	xvcmpgtsp 33,4,0
	xxsel 48,48,32,33
	xxsel 4,4,0,33
	bgt 7,.L65
	xxsldwi 0,4,4,3
	xxsldwi 11,4,4,2
	li 9,0
	li 10,12
	xxsldwi 12,4,4,1
	xscvspdp 4,4
	vextuwrx 3,9,16
	li 9,4
	xscvspdp 0,0
	xscvspdp 11,11
	xscvspdp 12,12
	vextuwrx 6,9,16
	li 9,8
	vextuwrx 7,9,16
	vextuwrx 9,10,16
	rldicl 31,6,0,32
	rldicl 10,3,0,32
	rldicl 5,7,0,32
	rldicl 0,9,0,32
	fcmpu 7,0,11
	fmr 10,12
	beq 7,.L66
	bng 7,.L9
	mr 10,31
	fmr 0,11
.L9:
	fcmpu 7,12,4
	bne 7,.L12
	cmplw 7,7,9
	ble 7,.L13
	mr 7,9
.L13:
	rldicl 5,7,0,32
.L14:
	fcmpu 7,0,10
	beq 7,.L67
	bng 7,.L19
	mr 10,5
	fmr 0,10
.L19:
	cmpd 7,11,8
	ld 31,-8(1)
	bgt 7,.L4
	b .L33
	.p2align 4,,15
.L66:
	cmplw 7,3,6
	ble 7,.L8
	mr 3,6
.L8:
	rldicl 10,3,0,32
	b .L9
	.p2align 4,,15
.L40:
	sldi 0,5,2
	li 10,0
	li 9,0
	b .L23
	.p2align 4,,15
.L12:
	bng 7,.L14
	mr 5,0
	fmr 10,4
	b .L14
	.p2align 4,,15
.L67:
	cmpd 7,10,5
	ble 7,.L19
	mr 10,5
	b .L19
.L51:
	li 9,1
	mtctr 9
	b .L22
.L52:
	li 8,1
	mtctr 8
	b .L35
	.long 0
	.byte 0,0,0,0,0,1,0,0
	.size	isamin_k,.-isamin_k
	.section	.rodata.cst16,"aM",@progbits,16
	.align 4
.LC2:
	.long	0
	.long	1
	.long	2
	.long	3
.LC3:
	.long	4
	.long	5
	.long	6
	.long	7
.LC4:
	.long	8
	.long	9
	.long	10
	.long	11
.LC5:
	.long	12
	.long	13
	.long	14
	.long	15
	.ident	"GCC: (SUSE Linux) 7.3.1 20180323 [gcc-7-branch revision 258812]"
	.section	.note.GNU-stack,"",@progbits
