MODULE_big = s

FILES += $(shell find . -name '*.c' -type f)
OBJS += $(foreach FILE, $(FILES), $(subst .c,.o, $(FILE)))

PG_CPPFLAGS += -std=c11
PG_CPPFLAGS += -I$(libpq_srcdir)
PG_CPPFLAGS += -I./include
# PG force declaration-after-statement but we don't like it
PG_CPPFLAGS += -Wno-declaration-after-statement
PG_CFLAGS += $(PG_CPPFLAGS)

PG_CFLAGS += $(shell pkg-config --cflags openssl)
SHLIB_LINK += $(shell pkg-config --libs openssl)

TAP_TESTS = yes

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
