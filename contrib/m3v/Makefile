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

## 在完成了普通的m3v-tree的c系列代码的构造之后,我们继续将其他功能的cpp包封装进来
## 外包的几个重要功能:
## 1. 在构建m3v-tree的过程当中维护每一个entry的额外的扩展数据
## 2. buffer-record-cache的缓存都在这里使用
## 3. SPrefsh的https://spdk.io调用(服务2)
## 4. 索引数据的存储直接放到这外面来,而真实数据则存放在列存引擎,列存引擎的需要
## 需要负责本身数据的分类使用(LSH).