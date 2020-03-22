CC=g++
# change to c++14 if you are using an older version
CPPFLAGS=-std=c++17 -g

all:
	cd src;\
	$(CC) $(CPPFLAGS) *.cpp exceptions/*.cpp -I. -Wall -o badgerdb_main

clean:
	cd src;\
	rm -f badgerdb_main test.?

doc:
	doxygen Doxyfile