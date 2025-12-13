# Copyright (c) 2022-2025, PostgreSQL Global Development Group

test_toast_ext_sources = files(
  'test_toast_ext.c',
)

if host_system == 'windows'
  test_toast_ext_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'test_toast_ext',
    '--FILEDESC', 'test_toast_ext - test code for extended TOAST headers',])
endif

test_toast_ext = shared_module('test_toast_ext',
  test_toast_ext_sources,
  kwargs: pg_test_mod_args,
)
test_install_libs += test_toast_ext

test_install_data += files(
  'test_toast_ext.control',
  'test_toast_ext--1.0.sql',
)

tests += {
  'name': 'test_toast_ext',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'test_toast_ext',
    ],
  },
}
