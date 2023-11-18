MODULE_big = mtree_am2
OBJS = \
	$(WIN32RES) \
	src/mtree.o src/mtreebuild.o src/mtreeinsert.o src/mtreescan.o src/mtreeutils.o src/mtreevaccum.o \
	src/vector.o
HEADERS = src/vector.h

EXTENSION = mtree_am2
DATA = mtree_am2--1.0.sql
PGFILEDESC = "mtree_am - template table AM eating all data"

# HEADERS = util_mtree/utils_tup.h

REGRESS = mtree_am

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/mtree_am2
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
