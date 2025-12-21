/*-------------------------------------------------------------------------
 *
 * toast_compression.c
 *	  Functions for toast compression.
 *
 * Copyright (c) 2021-2025, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/toast_compression.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

#ifdef USE_ZSTD
#include <zstd.h>
#endif

#include "access/detoast.h"
#include "access/toast_compression.h"
#include "access/toast_internals.h"
#include "common/pg_lzcompress.h"
#include "varatt.h"

/* GUC */
int			default_toast_compression = TOAST_PGLZ_COMPRESSION;

#define NO_COMPRESSION_SUPPORT(method) \
	ereport(ERROR, \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
			 errmsg("compression method %s not supported", method), \
			 errdetail("This functionality requires the server to be built with %s support.", method)))

/*
 * Compress a varlena using PGLZ.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
pglz_compress_datum(const struct varlena *value)
{
	int32		valsize,
				len;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * No point in wasting a palloc cycle if value size is outside the allowed
	 * range for compression.
	 */
	if (valsize < PGLZ_strategy_default->min_input_size ||
		valsize > PGLZ_strategy_default->max_input_size)
		return NULL;

	/*
	 * Figure out the maximum possible size of the pglz output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	tmp = (struct varlena *) palloc(PGLZ_MAX_OUTPUT(valsize) +
									VARHDRSZ_COMPRESSED);

	len = pglz_compress(VARDATA_ANY(value),
						valsize,
						(char *) tmp + VARHDRSZ_COMPRESSED,
						NULL);
	if (len < 0)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
}

/*
 * Decompress a varlena that was compressed using PGLZ.
 */
struct varlena *
pglz_decompress_datum(const struct varlena *value)
{
	struct varlena *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  VARDATA_COMPRESSED_GET_EXTSIZE(value), true);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Decompress part of a varlena that was compressed using PGLZ.
 */
struct varlena *
pglz_decompress_datum_slice(const struct varlena *value,
							int32 slicelength)
{
	struct varlena *result;
	int32		rawsize;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = pglz_decompress((char *) value + VARHDRSZ_COMPRESSED,
							  VARSIZE(value) - VARHDRSZ_COMPRESSED,
							  VARDATA(result),
							  slicelength, false);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed pglz data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
}

/*
 * Compress a varlena using LZ4.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
lz4_compress_datum(const struct varlena *value)
{
#ifndef USE_LZ4
	NO_COMPRESSION_SUPPORT("lz4");
	return NULL;				/* keep compiler quiet */
#else
	int32		valsize;
	int32		len;
	int32		max_size;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * Figure out the maximum possible size of the LZ4 output, add the bytes
	 * that will be needed for varlena overhead, and allocate that amount.
	 */
	max_size = LZ4_compressBound(valsize);
	tmp = (struct varlena *) palloc(max_size + VARHDRSZ_COMPRESSED);

	len = LZ4_compress_default(VARDATA_ANY(value),
							   (char *) tmp + VARHDRSZ_COMPRESSED,
							   valsize, max_size);
	if (len <= 0)
		elog(ERROR, "lz4 compression failed");

	/* data is incompressible so just free the memory and return NULL */
	if (len > valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

	return tmp;
#endif
}

/*
 * Decompress a varlena that was compressed using LZ4.
 */
struct varlena *
lz4_decompress_datum(const struct varlena *value)
{
#ifndef USE_LZ4
	NO_COMPRESSION_SUPPORT("lz4");
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	struct varlena *result;

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(VARDATA_COMPRESSED_GET_EXTSIZE(value) + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe((char *) value + VARHDRSZ_COMPRESSED,
								  VARDATA(result),
								  VARSIZE(value) - VARHDRSZ_COMPRESSED,
								  VARDATA_COMPRESSED_GET_EXTSIZE(value));
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));


	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using LZ4.
 */
struct varlena *
lz4_decompress_datum_slice(const struct varlena *value, int32 slicelength)
{
#ifndef USE_LZ4
	NO_COMPRESSION_SUPPORT("lz4");
	return NULL;				/* keep compiler quiet */
#else
	int32		rawsize;
	struct varlena *result;

	/* slice decompression not supported prior to 1.8.3 */
	if (LZ4_versionNumber() < 10803)
		return lz4_decompress_datum(value);

	/* allocate memory for the uncompressed data */
	result = (struct varlena *) palloc(slicelength + VARHDRSZ);

	/* decompress the data */
	rawsize = LZ4_decompress_safe_partial((char *) value + VARHDRSZ_COMPRESSED,
										  VARDATA(result),
										  VARSIZE(value) - VARHDRSZ_COMPRESSED,
										  slicelength,
										  slicelength);
	if (rawsize < 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed lz4 data is corrupt")));

	SET_VARSIZE(result, rawsize + VARHDRSZ);

	return result;
#endif
}

/* ----------
 * zstd compression/decompression routines
 *
 * ZSTD uses VARTAG_ONDISK_ZSTD for external storage, not cmid=3.
 * TOAST_ZSTD_COMPRESSION_ID exists only for introspection (SQL functions).
 * ----------
 */

/*
 * Compress a varlena using ZSTD.
 *
 * Returns the compressed varlena, or NULL if compression fails.
 */
struct varlena *
zstd_compress_datum(const struct varlena *value)
{
#ifndef USE_ZSTD
	NO_COMPRESSION_SUPPORT("zstd");
	return NULL;				/* keep compiler quiet */
#else
	int32		valsize;
	size_t		len;
	size_t		max_size;
	struct varlena *tmp = NULL;

	valsize = VARSIZE_ANY_EXHDR(value);

	/*
	 * No point in wasting a zstd header on empty or very short inputs.
	 */
	if (unlikely(valsize < 32))
		return NULL;

	/*
	 * Allocate buffer for compressed output. Return a plain varlena containing
	 * just the ZSTD compressed frame. toast_save_datum() will store this to
	 * external TOAST without adding tcinfo header (compression method is
	 * identified by VARTAG_ONDISK_ZSTD instead).
	 */
	max_size = ZSTD_compressBound(valsize);
	tmp = (struct varlena *) palloc(max_size + VARHDRSZ);

	len = ZSTD_compress((char *) tmp + VARHDRSZ,
						max_size,
						VARDATA_ANY(value),
						valsize,
						3);		/* compression level 3 for balanced speed/ratio */

	if (unlikely(ZSTD_isError(len)))
		elog(ERROR, "zstd compression failed: %s", ZSTD_getErrorName(len));

	/* data is incompressible so just free the memory and return NULL */
	if (len >= (size_t) valsize)
	{
		pfree(tmp);
		return NULL;
	}

	SET_VARSIZE(tmp, len + VARHDRSZ);

	return tmp;
#endif
}

/*
 * Decompress a varlena that was compressed using ZSTD.
 */
struct varlena *
zstd_decompress_datum(const struct varlena *value, int32 rawsize)
{
#ifndef USE_ZSTD
	NO_COMPRESSION_SUPPORT("zstd");
	return NULL;				/* keep compiler quiet */
#else
	size_t		decomp_size;
	struct varlena *result;

	result = (struct varlena *) palloc(rawsize + VARHDRSZ);

	decomp_size = ZSTD_decompress(VARDATA(result),
								   rawsize,
								   (char *) value + VARHDRSZ,
								   VARSIZE(value) - VARHDRSZ);

	if (unlikely(ZSTD_isError(decomp_size)))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt: %s",
								 ZSTD_getErrorName(decomp_size))));

	SET_VARSIZE(result, decomp_size + VARHDRSZ);

	return result;
#endif
}

/*
 * Decompress part of a varlena that was compressed using ZSTD.
 *
 * Note: We decompress the full datum then return the requested slice.
 * This is necessary because detoast_attr_slice() calls toast_fetch_datum()
 * first (which fetches all compressed TOAST chunks), so the real bottleneck
 * is TOAST I/O, not decompression method. ZSTD doesn't support true random
 * access within compressed frames, and streaming APIs don't help when the
 * full compressed input is already materialized in memory.
 */
struct varlena *
zstd_decompress_datum_slice(const struct varlena *value, int32 rawsize, int32 slicelength)
{
#ifndef USE_ZSTD
	NO_COMPRESSION_SUPPORT("zstd");
	return NULL;				/* keep compiler quiet */
#else
	size_t		decomp_size;
	struct varlena *result;

	/* Limit to actual size if slice request is larger */
	if (slicelength >= rawsize)
		return zstd_decompress_datum(value, rawsize);

	/* Decompress the full data */
	result = (struct varlena *) palloc(rawsize + VARHDRSZ);

	decomp_size = ZSTD_decompress(VARDATA(result),
								   rawsize,
								   (char *) value + VARHDRSZ,
								   VARSIZE(value) - VARHDRSZ);

	if (unlikely(ZSTD_isError(decomp_size)))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg_internal("compressed zstd data is corrupt: %s",
								 ZSTD_getErrorName(decomp_size))));

	/* Truncate to requested size */
	SET_VARSIZE(result, slicelength + VARHDRSZ);

	return result;
#endif
}

/*
 * Extract compression ID from a varlena.
 *
 * Returns TOAST_INVALID_COMPRESSION_ID if the varlena is not compressed.
 */
ToastCompressionId
toast_get_compression_id(struct varlena *attr)
{
	ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

	/*
	 * If it is stored externally then fetch the compression method id from
	 * the external toast pointer.  If compressed inline, fetch it from the
	 * toast compression header.
	 *
	 * For ZSTD external data, VARTAG_ONDISK_ZSTD indicates compression,
	 * so we return TOAST_ZSTD_COMPRESSION_ID directly without checking
	 * va_extinfo bits.
	 */
	if (VARATT_IS_EXTERNAL_ONDISK_ZSTD(attr))
	{
		/* ZSTD external data uses vartag to indicate compression */
		cmid = TOAST_ZSTD_COMPRESSION_ID;
	}
	else if (VARATT_IS_EXTERNAL_ONDISK(attr))
	{
		struct varatt_external toast_pointer;

		VARATT_EXTERNAL_GET_POINTER(toast_pointer, attr);

		if (VARATT_EXTERNAL_IS_COMPRESSED(toast_pointer))
			cmid = VARATT_EXTERNAL_GET_COMPRESS_METHOD(toast_pointer);
	}
	else if (VARATT_IS_COMPRESSED(attr))
		cmid = VARDATA_COMPRESSED_GET_COMPRESS_METHOD(attr);

	return cmid;
}

/*
 * CompressionNameToMethod - Get compression method from compression name
 *
 * Search in the available built-in methods.  If the compression not found
 * in the built-in methods then return InvalidCompressionMethod.
 */
char
CompressionNameToMethod(const char *compression)
{
	if (strcmp(compression, "pglz") == 0)
		return TOAST_PGLZ_COMPRESSION;
	else if (strcmp(compression, "lz4") == 0)
	{
#ifndef USE_LZ4
		NO_COMPRESSION_SUPPORT("lz4");
#endif
		return TOAST_LZ4_COMPRESSION;
	}
	else if (strcmp(compression, "zstd") == 0)
	{
#ifndef USE_ZSTD
		NO_COMPRESSION_SUPPORT("zstd");
#endif
		return TOAST_ZSTD_COMPRESSION;
	}

	return InvalidCompressionMethod;
}

/*
 * GetCompressionMethodName - Get compression method name
 */
const char *
GetCompressionMethodName(char method)
{
	switch (method)
	{
		case TOAST_PGLZ_COMPRESSION:
			return "pglz";
		case TOAST_LZ4_COMPRESSION:
			return "lz4";
		case TOAST_ZSTD_COMPRESSION:
			return "zstd";
		default:
			elog(ERROR, "invalid compression method %c", method);
			return NULL;		/* keep compiler quiet */
	}
}
