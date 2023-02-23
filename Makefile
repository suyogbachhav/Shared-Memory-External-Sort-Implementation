CC=gcc
CFLAGS=-Wall

all: mysort

mysort: mysort.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -rf mysort
