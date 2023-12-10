MODULE_big = m3v
OBJS = \
	$(WIN32RES) \
	src/m3v.o src/m3vbuild.o src/m3vinsert.o src/m3vscan.o src/m3vutils.o src/m3vvaccum.o \
	src/vector.o
HEADERS = src/vector.h

EXTENSION = m3v
DATA = m3v--1.0.sql
PGFILEDESC = "m3v - template table AM eating all data"
# INCLUDEDIR       = $(shell $(PG_CONFIG) --includedir-server)
# PG_CXXFLAGS += -fno-exceptions
# PG_CPPFLAGS += -fno-exceptions
PG_CXXFLAGS      += -std=gnu++11 -lstdc++ -I$(INCLUDEDIR) -fno-exceptions
# PG_CPPFLAGS      += -std=gnu++11 -lstdc++ -I$(INCLUDEDIR) -fno-exceptions
# HEADERS = util_m3v/utils_tup.h

CXX = g++

REGRESS = m3v

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/m3v
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
