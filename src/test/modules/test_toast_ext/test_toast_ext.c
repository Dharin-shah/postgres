/*-------------------------------------------------------------------------
 *
 * test_toast_ext.c
 *		Test module for extended TOAST header structures.
 *
 * Copyright (c) 2025, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/detoast.h"
#include "access/toast_compression.h"
#include "varatt.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_toast_structure_sizes);
PG_FUNCTION_INFO_V1(test_toast_flag_validation);
PG_FUNCTION_INFO_V1(test_toast_compression_ids);

/*
 * Verify TOAST structure sizes match expected values.
 * Errors out if any size is wrong (catches ABI issues).
 */
Datum
test_toast_structure_sizes(PG_FUNCTION_ARGS)
{
	/* Standard structure must be 16 bytes */
	if (sizeof(varatt_external) != 16)
		elog(ERROR, "varatt_external is %zu bytes, expected 16",
			 sizeof(varatt_external));

	/* Extended structure must be 20 bytes */
	if (sizeof(varatt_external_extended) != 20)
		elog(ERROR, "varatt_external_extended is %zu bytes, expected 20",
			 sizeof(varatt_external_extended));

	/* TOAST pointer sizes (include 2-byte external header) */
	if (TOAST_POINTER_SIZE != 18)
		elog(ERROR, "TOAST_POINTER_SIZE is %zu, expected 18",
			 (Size) TOAST_POINTER_SIZE);

	if (TOAST_POINTER_SIZE_EXTENDED != 22)
		elog(ERROR, "TOAST_POINTER_SIZE_EXTENDED is %zu, expected 22",
			 (Size) TOAST_POINTER_SIZE_EXTENDED);

	/* Verify field offsets (no unexpected padding) */
	if (offsetof(varatt_external_extended, va_rawsize) != 0)
		elog(ERROR, "va_rawsize offset is %zu, expected 0",
			 offsetof(varatt_external_extended, va_rawsize));
	if (offsetof(varatt_external_extended, va_extinfo) != 4)
		elog(ERROR, "va_extinfo offset is %zu, expected 4",
			 offsetof(varatt_external_extended, va_extinfo));
	if (offsetof(varatt_external_extended, va_flags) != 8)
		elog(ERROR, "va_flags offset is %zu, expected 8",
			 offsetof(varatt_external_extended, va_flags));
	if (offsetof(varatt_external_extended, va_data) != 9)
		elog(ERROR, "va_data offset is %zu, expected 9",
			 offsetof(varatt_external_extended, va_data));
	if (offsetof(varatt_external_extended, va_valueid) != 12)
		elog(ERROR, "va_valueid offset is %zu, expected 12",
			 offsetof(varatt_external_extended, va_valueid));
	if (offsetof(varatt_external_extended, va_toastrelid) != 16)
		elog(ERROR, "va_toastrelid offset is %zu, expected 16",
			 offsetof(varatt_external_extended, va_toastrelid));

	PG_RETURN_VOID();
}

/*
 * Verify flag validation macros work correctly.
 */
Datum
test_toast_flag_validation(PG_FUNCTION_ARGS)
{
	/* Valid flags should pass */
	if (!ExtendedFlagsAreValid(0x00))
		elog(ERROR, "flags 0x00 should be valid");
	if (!ExtendedFlagsAreValid(0x01))
		elog(ERROR, "flags 0x01 should be valid");
	if (!ExtendedFlagsAreValid(0x02))
		elog(ERROR, "flags 0x02 should be valid");
	if (!ExtendedFlagsAreValid(0x03))
		elog(ERROR, "flags 0x03 should be valid");

	/* Invalid flags should fail */
	if (ExtendedFlagsAreValid(0x04))
		elog(ERROR, "flags 0x04 should be invalid");
	if (ExtendedFlagsAreValid(0x08))
		elog(ERROR, "flags 0x08 should be invalid");
	if (ExtendedFlagsAreValid(0xFF))
		elog(ERROR, "flags 0xFF should be invalid");

	/* Compression methods 0-255 are valid */
	if (!ExtendedCompressionMethodIsValid(0))
		elog(ERROR, "compression method 0 should be valid");
	if (!ExtendedCompressionMethodIsValid(255))
		elog(ERROR, "compression method 255 should be valid");

	/* Verify method ID constants */
	if (TOAST_PGLZ_EXT_METHOD != 0)
		elog(ERROR, "TOAST_PGLZ_EXT_METHOD is %d, expected 0", TOAST_PGLZ_EXT_METHOD);
	if (TOAST_LZ4_EXT_METHOD != 1)
		elog(ERROR, "TOAST_LZ4_EXT_METHOD is %d, expected 1", TOAST_LZ4_EXT_METHOD);
	if (TOAST_ZSTD_EXT_METHOD != 2)
		elog(ERROR, "TOAST_ZSTD_EXT_METHOD is %d, expected 2", TOAST_ZSTD_EXT_METHOD);
	if (TOAST_UNCOMPRESSED_EXT_METHOD != 3)
		elog(ERROR, "TOAST_UNCOMPRESSED_EXT_METHOD is %d, expected 3", TOAST_UNCOMPRESSED_EXT_METHOD);

	PG_RETURN_VOID();
}

/*
 * Verify compression ID constants are consistent.
 */
Datum
test_toast_compression_ids(PG_FUNCTION_ARGS)
{
	/* Standard compression IDs */
	if (TOAST_PGLZ_COMPRESSION_ID != 0)
		elog(ERROR, "TOAST_PGLZ_COMPRESSION_ID is %d, expected 0", TOAST_PGLZ_COMPRESSION_ID);
	if (TOAST_LZ4_COMPRESSION_ID != 1)
		elog(ERROR, "TOAST_LZ4_COMPRESSION_ID is %d, expected 1", TOAST_LZ4_COMPRESSION_ID);
	if (TOAST_INVALID_COMPRESSION_ID != 2)
		elog(ERROR, "TOAST_INVALID_COMPRESSION_ID is %d, expected 2", TOAST_INVALID_COMPRESSION_ID);
	if (TOAST_EXTENDED_COMPRESSION_ID != 3)
		elog(ERROR, "TOAST_EXTENDED_COMPRESSION_ID is %d, expected 3", TOAST_EXTENDED_COMPRESSION_ID);

	/* Extended IDs should match standard where applicable */
	if (TOAST_PGLZ_EXT_METHOD != TOAST_PGLZ_COMPRESSION_ID)
		elog(ERROR, "PGLZ IDs mismatch: standard=%d, extended=%d",
			 TOAST_PGLZ_COMPRESSION_ID, TOAST_PGLZ_EXT_METHOD);
	if (TOAST_LZ4_EXT_METHOD != TOAST_LZ4_COMPRESSION_ID)
		elog(ERROR, "LZ4 IDs mismatch: standard=%d, extended=%d",
			 TOAST_LZ4_COMPRESSION_ID, TOAST_LZ4_EXT_METHOD);

	PG_RETURN_VOID();
}
