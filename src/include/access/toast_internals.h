/*-------------------------------------------------------------------------
 *
 * toast_internals.h
 *	  Internal definitions for the TOAST system.
 *
 * Copyright (c) 2000-2026, PostgreSQL Global Development Group
 *
 * src/include/access/toast_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TOAST_INTERNALS_H
#define TOAST_INTERNALS_H

#include "access/toast_compression.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "varatt.h"

/*
 * Decompression method for decoded toast pointers. Separate from
 * ToastCompressionId (2-bit on-disk encoding) to allow future methods.
 */
typedef enum ToastDecompressMethod
{
	TOAST_DECOMP_NONE = 0,
	TOAST_DECOMP_PGLZ = 1,
	TOAST_DECOMP_LZ4 = 2
} ToastDecompressMethod;

/*
 * Flags for DecodedExternalToast.
 *
 * HAS_TCINFO: Payload starts with tcinfo header. True for PGLZ/LZ4 external;
 *   false for uncompressed or future methods storing raw compressed data.
 * IS_ONDISK: Set for VARTAG_ONDISK (for future vartag extension).
 */
#define TOAST_EXT_HAS_TCINFO	0x01
#define TOAST_EXT_IS_ONDISK		0x02

/*
 * Decoded representation of an external on-disk TOAST pointer.
 * Normalizes vartag/va_extinfo variations; decode once, use throughout.
 *
 * HAS_TCINFO indicates payload format (has tcinfo header), distinct from
 * "is compressed" (extsize < rawsize) - future methods may omit tcinfo.
 */
typedef struct DecodedExternalToast
{
	Oid			toastrelid;
	Oid			valueid;
	uint32		rawsize;		/* Decompressed size; for future methods without tcinfo */
	uint32		extsize;		/* On-disk payload size */
	ToastDecompressMethod method;
	uint8		flags;
} DecodedExternalToast;

/*
 *	The information at the start of the compressed toast data.
 */
typedef struct toast_compress_header
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		tcinfo;			/* 2 bits for compression method and 30 bits
								 * external size; see va_extinfo */
} toast_compress_header;

/*
 * Utilities for manipulation of header information for compressed
 * toast entries.
 */
#define TOAST_COMPRESS_EXTSIZE(ptr) \
	(((toast_compress_header *) (ptr))->tcinfo & VARLENA_EXTSIZE_MASK)
#define TOAST_COMPRESS_METHOD(ptr) \
	(((toast_compress_header *) (ptr))->tcinfo >> VARLENA_EXTSIZE_BITS)

#define TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(ptr, len, cm_method) \
	do { \
		Assert((len) > 0 && (len) <= VARLENA_EXTSIZE_MASK); \
		Assert((cm_method) == TOAST_PGLZ_COMPRESSION_ID || \
			   (cm_method) == TOAST_LZ4_COMPRESSION_ID); \
		((toast_compress_header *) (ptr))->tcinfo = \
			(len) | ((uint32) (cm_method) << VARLENA_EXTSIZE_BITS); \
	} while (0)

extern Datum toast_compress_datum(Datum value, char cmethod);
extern Oid	toast_get_valid_index(Oid toastoid, LOCKMODE lock);

extern void toast_delete_datum(Relation rel, Datum value, bool is_speculative);
extern Datum toast_save_datum(Relation rel, Datum value,
							  struct varlena *oldexternal, int options);

extern int	toast_open_indexes(Relation toastrel,
							   LOCKMODE lock,
							   Relation **toastidxs,
							   int *num_indexes);
extern void toast_close_indexes(Relation *toastidxs, int num_indexes,
								LOCKMODE lock);
extern Snapshot get_toast_snapshot(void);

#endif							/* TOAST_INTERNALS_H */
