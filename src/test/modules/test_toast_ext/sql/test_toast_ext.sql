--
-- Tests for extended TOAST header structures and zstd compression
--

CREATE EXTENSION test_toast_ext;

-- Use dedicated schema for test isolation
CREATE SCHEMA test_toast_ext_schema;
SET search_path TO test_toast_ext_schema, public;

-- Compile-time validation tests (always run)
-- These error out on failure, so completing without error = pass
SELECT test_toast_structure_sizes();
SELECT test_toast_flag_validation();
SELECT test_toast_compression_ids();

--
-- Functional tests for zstd TOAST compression
-- Skip if not built with USE_ZSTD
--

SELECT NOT(enumvals @> '{zstd}') AS skip_test FROM pg_settings WHERE
  name = 'default_toast_compression' \gset
\if :skip_test
   \echo '*** skipping TOAST tests with zstd (not supported) ***'
   \quit
\endif

-- Test basic zstd compression
CREATE TABLE test_zstd_basic (id serial, data text COMPRESSION zstd);
INSERT INTO test_zstd_basic (data)
    VALUES (repeat('PostgreSQL zstd TOAST compression test. ', 3000));

SELECT id,
       pg_column_compression(data) AS compression,
       length(data) AS data_length,
       left(data, 42) AS data_prefix
FROM test_zstd_basic;

-- Test slice access
SELECT id, substr(data, 100, 42) AS slice FROM test_zstd_basic;

-- Test UPDATE
UPDATE test_zstd_basic SET data = repeat('Updated zstd data for TOAST test. ', 3000);
SELECT id,
       pg_column_compression(data) AS compression,
       length(data) AS data_length,
       left(data, 35) AS data_prefix
FROM test_zstd_basic;

-- Test extended header with pglz
SET use_extended_toast_header = on;

CREATE TABLE test_pglz_extended (data text COMPRESSION pglz);
INSERT INTO test_pglz_extended (data)
    VALUES (repeat('PGLZ with extended header format. ', 3000));

SELECT pg_column_compression(data) AS compression,
       length(data) AS data_length
FROM test_pglz_extended;

SELECT substr(data, 50, 34) AS slice FROM test_pglz_extended;

-- Test data integrity
CREATE TABLE test_integrity (
    method text,
    original_data text,
    compressed_data text
);

INSERT INTO test_integrity VALUES
    ('pglz', repeat('Integrity test data pattern. ', 2000), NULL),
    ('zstd', repeat('Integrity test data pattern. ', 2000), NULL);

CREATE TABLE test_pglz_integrity (data text COMPRESSION pglz);
CREATE TABLE test_zstd_integrity (data text COMPRESSION zstd);

INSERT INTO test_pglz_integrity SELECT original_data FROM test_integrity WHERE method = 'pglz';
INSERT INTO test_zstd_integrity SELECT original_data FROM test_integrity WHERE method = 'zstd';

SELECT 'pglz' AS method,
       md5((SELECT original_data FROM test_integrity WHERE method = 'pglz')) =
       md5((SELECT data FROM test_pglz_integrity)) AS checksum_match;

SELECT 'zstd' AS method,
       md5((SELECT original_data FROM test_integrity WHERE method = 'zstd')) =
       md5((SELECT data FROM test_zstd_integrity)) AS checksum_match;

-- Test CLUSTER and VACUUM FULL
CREATE TABLE test_cluster_zstd (id serial PRIMARY KEY, data text COMPRESSION zstd);
INSERT INTO test_cluster_zstd (data)
    VALUES (repeat('Data for CLUSTER test with zstd compression. ', 2500));

SELECT 'before_cluster' AS stage, md5(data) AS hash FROM test_cluster_zstd;

CLUSTER test_cluster_zstd USING test_cluster_zstd_pkey;

SELECT 'after_cluster' AS stage,
       pg_column_compression(data) AS compression,
       md5(data) AS hash
FROM test_cluster_zstd;

VACUUM FULL test_cluster_zstd;

SELECT 'after_vacuum_full' AS stage,
       pg_column_compression(data) AS compression,
       md5(data) AS hash
FROM test_cluster_zstd;

-- Test GUC toggling (mixed formats in same table)
SET use_extended_toast_header = on;
CREATE TABLE test_guc_toggle (id serial, data text COMPRESSION pglz);
INSERT INTO test_guc_toggle (data)
    VALUES (repeat('Data created with extended header on. ', 3000));

SELECT 'with_ext_on' AS stage,
       pg_column_compression(data) AS compression,
       length(data) AS data_length
FROM test_guc_toggle;

SET use_extended_toast_header = off;
INSERT INTO test_guc_toggle (data)
    VALUES (repeat('Data created with extended header off. ', 3000));

SELECT id,
       pg_column_compression(data) AS compression,
       length(data) AS data_length,
       left(data, 39) AS data_prefix
FROM test_guc_toggle ORDER BY id;

SET use_extended_toast_header = on;
SELECT id, length(data) AS data_length FROM test_guc_toggle ORDER BY id;

-- Cleanup
DROP SCHEMA test_toast_ext_schema CASCADE;
DROP EXTENSION test_toast_ext;
