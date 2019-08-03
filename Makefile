# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# compile flags for linux / osx
ifeq ($(uname_S),Linux)
	LDFLAGS ?= -shared -Bsymbolic
else
	LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

CCOPT = -O3 -std=gnu99 -Wall -pedantic -fomit-frame-pointer -DNDEBUG
CC = gcc
MPD_FLAGS = -lmpdec

all: bignumber.so

bignumber.so: bignumber.c
	$(CC) $(CCOPT) -fPIC $(LDFLAGS) $^ -o $@ $(MPD_FLAGS)

clean:
	rm -rf *.so *.o
