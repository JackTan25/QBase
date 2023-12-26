https://github.com/ReverseControl/MuchPIR/blob/main/Makefile
// build cpp extension
https://stackoverflow.com/questions/48892087/using-c-functions-in-postresql
https://www.postgresql.org/docs/current/xfunc-c.html#EXTEND-CPP
https://www.postgresql.org/docs/14/ecpg-cpp.html#ECPG-CPP-AND-C
https://web.archive.org/web/20170703160924/https://www.openscg.com/2017/03/c14-and-postgres/
#MODULES = pir_select
https://github.com/carlospolop/hacktricks/blob/master/pentesting-web/sql-injection/postgresql-injection/rce-with-postgresql-extensions.md
https://stackoverflow.com/questions/77633935/how-to-write-a-c-extension-for-postgres
EXTENSION   = pir_select
DATA        = $(wildcard *--*.sql)              # script files to install
TESTS       = $(wildcard test/sql/*.sql)      # use test/sql/*.sql as testfiles

PGXS             = $(shell $(PG_CONFIG) --pgxs)
INCLUDEDIR       = $(shell $(PG_CONFIG) --includedir-server)
INCLUDE_SEAL     = /usr/local/include/SEAL-3.5/
INCLUDE_SEAL_LIB = /usr/local/lib/
INCLUDE_CPPCODEC = /usr/local/include/cppcodec
PG_CXXFLAGS      = -std=gnu++17 -fPIC -Wall  -g -O0 -pthread   -L$(INCLUDE_SEAL_LIB) -l:libseal.so.3.5.8 -I$(INCLUDEDIR)  -I$(INCLUDE_SEAL) -I$(INCLUDE_CPPCODEC) -I/usr/include/postgresql/12/server/
PG_CPPFLAGS      = -std=gnu++17 -fPIC -Wall  -g -O0 -pthread   -Wno-register                             -I$(INCLUDEDIR)  -I$(INCLUDE_SEAL) -I$(INCLUDE_CPPCODEC) -I/usr/include/postgresql/12/server/
PG_LDFLAGS       = -I$(INCLUDE_SEAL) -L$(INCLUDE_SEAL_LIB) -l:libseal.so.3.5.8 -pthread

REGRESS_OPTS  = --inputdir=test         \
                --load-extension=pir_select \
                --load-language=plpgsql
REGRESS       = $(patsubst test/sql/%.sql,%,$(TESTS))
OBJS          = $(patsubst %.cpp,%.o,$(wildcard src/*.cpp)) # object files

# final shared library to be build from multiple source files (OBJS)
MODULE_big    = $(EXTENSION)

# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
