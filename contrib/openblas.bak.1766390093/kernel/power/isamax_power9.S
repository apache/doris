/*
	.file	"isamax.c"
	.abiversion 2
	.section	".text"
	.align 2
	.p2align 4,,15
	.globl isamax_k
	.type	isamax_k, @function
*/
#define ASSEMBLER
#include "common.h"

        PROLOGUE

isamax_k:
.LCF0:
0:	addis 2,12,.TOC.-.LCF0@ha
	addi 2,2,.TOC.-.LCF0@l
	.localentry	isamax_k,.-isamax_k
	mr. 11,3
	ble 0,.L36
	cmpdi 7,5,0
	li 3,0
	blelr 7
	cmpdi 7,5,1
	beq 7,.L69
	rldicr. 7,11,0,61
	beq 0,.L40
	sldi 10,5,1
	sldi 6,5,2
	sldi 0,5,4
	sldi 3,5,3
	mr 9,4
	xxlxor 0,0,0
	li 8,0
	add 5,10,5
	li 10,0
	sldi 5,5,2
	.p2align 4,,15
.L31:
	lfs 12,0(9)
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L23
	fmr 0,12
	mr 8,10
.L23:
	lfsx 12,9,6
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L25
	fmr 0,12
	addi 8,10,1
.L25:
	lfsx 12,9,3
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L27
	fmr 0,12
	addi 8,10,2
.L27:
	lfsx 12,9,5
	add 9,9,0
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L29
	fmr 0,12
	addi 8,10,3
.L29:
	addi 10,10,4
	cmpd 7,7,10
	bgt 7,.L31
	addi 7,7,-1
	srdi 7,7,2
	addi 7,7,1
	sldi 9,7,2
	mulld 7,6,7
	cmpd 7,11,9
	ble 7,.L67
.L22:
	addi 10,9,1
	sldi 7,7,2
	subf 5,9,11
	cmpd 7,10,11
	mtctr 5
	add 4,4,7
	bgt 7,.L54
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L54
	.p2align 4,,15
.L35:
	lfs 12,0(4)
	add 4,4,6
	fabs 12,12
	fcmpu 7,12,0
	bng 7,.L33
	fmr 0,12
	mr 8,9
.L33:
	addi 9,9,1
	bdnz .L35
.L67:
	addi 3,8,1
	blr
	.p2align 4,,15
.L36:
	li 3,0
	blr
	.p2align 4,,15
.L69:
	rldicr. 10,11,0,57
	bne 0,.L70
	addi 7,10,1
	sldi 9,10,2
	subf 6,10,11
	li 8,0
	xxlxor 12,12,12
	cmpd 7,7,11
	mtctr 6
	add 4,4,9
	bgt 7,.L60
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L60
	.p2align 4,,15
.L61:
	lfs 0,0(4)
	addi 4,4,4
	fabs 0,0
	fcmpu 7,0,12
	bng 7,.L63
	fmr 12,0
	mr 8,10
.L63:
	addi 10,10,1
	bdnz .L61
	b .L67
	.p2align 4,,15
.L70:
	addis 6,2,.LC2@toc@ha
	addis 7,2,.LC3@toc@ha
	addis 8,2,.LC4@toc@ha
	addis 9,2,.LC5@toc@ha
	xxspltib 46,0
	stxv 61,-48(1)
	stxv 62,-32(1)
	addi 6,6,.LC2@toc@l
	addi 7,7,.LC3@toc@l
	stxv 63,-16(1)
	xxspltib 61,32
	xxspltib 63,16
	xxspltib 62,64
	addi 8,8,.LC4@toc@l
	addi 9,9,.LC5@toc@l
	lxv 47,0(6)
	xxspltib 34,0
	lxv 48,0(7)
	xxlor 51,46,46
	lxv 49,0(8)
	lxv 50,0(9)
	li 8,0
	mr 9,4
	vextsb2w 29,29
	vextsb2w 31,31
	vextsb2w 30,30
	stxv 59,-80(1)
	stxv 60,-64(1)
	.p2align 4,,15
.L5:
	lxv 0,0(9)
	vadduwm 27,19,29
	lxv 12,240(9)
	addi 8,8,64
	addi 9,9,256
	cmpd 7,10,8
	xvabssp 44,0
	lxv 0,-240(9)
	xvabssp 12,12
	xvabssp 5,0
	lxv 0,-224(9)
	xvabssp 32,0
	lxv 0,-208(9)
	xvcmpgtsp 35,5,44
	xvabssp 9,0
	lxv 0,-192(9)
	xxsel 5,44,5,35
	xxsel 35,47,48,35
	xvabssp 1,0
	lxv 0,-176(9)
	xvcmpgtsp 60,9,32
	xvabssp 6,0
	lxv 0,-160(9)
	xxsel 9,32,9,60
	xxsel 60,49,50,60
	xvabssp 13,0
	lxv 0,-144(9)
	xvcmpgtsp 42,9,5
	xvcmpgtsp 37,6,1
	xvabssp 11,0
	lxv 0,-128(9)
	xxsel 9,5,9,42
	xxsel 42,35,60,42
	xxsel 6,1,6,37
	xxsel 37,47,48,37
	xvabssp 2,0
	lxv 0,-112(9)
	xvcmpgtsp 36,11,13
	xvabssp 7,0
	lxv 0,-96(9)
	xxsel 11,13,11,36
	xxsel 36,49,50,36
	xvabssp 3,0
	lxv 0,-80(9)
	xvcmpgtsp 45,11,6
	xvcmpgtsp 39,7,2
	xvabssp 10,0
	lxv 0,-64(9)
	xxsel 7,2,7,39
	xxsel 39,47,48,39
	xvabssp 4,0
	lxv 0,-48(9)
	xvcmpgtsp 38,10,3
	xvabssp 8,0
	lxv 0,-32(9)
	xxsel 10,3,10,38
	xxsel 38,49,50,38
	xvabssp 0,0
	xvcmpgtsp 43,10,7
	xvcmpgtsp 41,8,4
	xvcmpgtsp 40,12,0
	xxsel 8,4,8,41
	xxsel 41,47,48,41
	xxsel 0,0,12,40
	xxsel 12,6,11,45
	xxsel 11,7,10,43
	xxsel 45,37,36,45
	xvcmpgtsp 33,0,8
	xvcmpgtsp 32,12,9
	vadduwm 13,13,31
	xxsel 40,49,50,40
	xxsel 43,39,38,43
	xxsel 0,8,0,33
	xxsel 12,9,12,32
	xxsel 33,41,40,33
	xxsel 32,42,45,32
	xvcmpgtsp 44,0,11
	vadduwm 1,1,31
	vadduwm 0,19,0
	vadduwm 19,19,30
	xxsel 0,11,0,44
	xxsel 33,43,33,44
	xvcmpgtsp 45,0,12
	vadduwm 1,27,1
	xxsel 0,12,0,45
	xxsel 32,32,33,45
	xvcmpgtsp 33,0,34
	xxsel 46,46,32,33
	xxsel 34,34,0,33
	bgt 7,.L5
	xxsldwi 12,34,34,3
	xxsldwi 11,34,34,2
	li 9,0
	li 8,12
	xxsldwi 0,34,34,1
	xscvspdp 34,34
	vextuwrx 3,9,14
	li 9,4
	xscvspdp 12,12
	xscvspdp 11,11
	xscvspdp 0,0
	vextuwrx 6,9,14
	li 9,8
	vextuwrx 7,9,14
	vextuwrx 9,8,14
	rldicl 12,6,0,32
	rldicl 8,3,0,32
	rldicl 0,7,0,32
	rldicl 5,9,0,32
	fcmpu 7,12,11
	fmr 10,0
	beq 7,.L71
	bnl 7,.L8
	mr 8,12
	fmr 12,11
.L8:
	xscmpudp 7,0,34
	bne 7,.L11
	cmplw 7,7,9
	ble 7,.L12
	mr 7,9
.L12:
	rldicl 5,7,0,32
.L13:
	fcmpu 7,12,10
	beq 7,.L72
	bnl 7,.L17
	mr 8,5
	fmr 12,10
.L17:
	cmpd 7,11,10
	ble 7,.L16
	addi 7,10,1
	sldi 9,10,2
	subf 6,10,11
	cmpd 7,7,11
	mtctr 6
	add 4,4,9
	bgt 7,.L53
	li 3,-1
	rldicr 3,3,0,0
	cmpd 7,11,3
	beq 7,.L53
	.p2align 4,,15
.L21:
	lfs 0,0(4)
	addi 4,4,4
	fabs 0,0
	fcmpu 7,0,12
	bng 7,.L19
	fmr 12,0
	mr 8,10
.L19:
	addi 10,10,1
	bdnz .L21
.L16:
	lxv 59,-80(1)
	lxv 60,-64(1)
	addi 3,8,1
	lxv 61,-48(1)
	lxv 62,-32(1)
	lxv 63,-16(1)
	blr
	.p2align 4,,15
.L71:
	cmplw 7,3,6
	ble 7,.L7
	mr 3,6
.L7:
	rldicl 8,3,0,32
	b .L8
	.p2align 4,,15
.L40:
	sldi 6,5,2
	li 8,0
	li 9,0
	xxlxor 0,0,0
	b .L22
	.p2align 4,,15
.L11:
	blt 7,.L39
	mr 5,0
	b .L13
	.p2align 4,,15
.L72:
	cmpd 7,8,5
	ble 7,.L17
	mr 8,5
	b .L17
	.p2align 4,,15
.L39:
	xscpsgndp 10,34,34
	b .L13
.L53:
	li 9,1
	mtctr 9
	b .L21
.L54:
	li 10,1
	mtctr 10
	b .L35
.L60:
	li 9,1
	mtctr 9
	b .L61
	.long 0
	.byte 0,0,0,0,0,0,0,0
	.size	isamax_k,.-isamax_k
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
