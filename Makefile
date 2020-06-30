#!/usr/bin/make

.PHONY: build develop hiredis clean

ext_name = hiredis
ext_relname = fastredis/$(ext_name)

build:
	python setup.py build -v

develop:
	python setup.py develop -v

hiredis: $(ext_relname).i
	swig -python $(ext_relname).i
	gcc -c -fpic $(ext_relname)_wrap.c -I/usr/include/python3.7 -I/usr/include/hiredis
	gcc -shared $(ext_name)_wrap.o -L/usr/lib/x86_64-linux-gnu -lhiredis -o fastredis/_$(ext_name).so

clean:
	python setup.py clean
	rm -rf build
	rm -rf *.egg-info
	rm -f *.so
	rm -f *.o
	rm -f fastredis/*.so
	rm -f fastredis/$(ext_name).py
	rm -f fastredis/*_wrap.c
