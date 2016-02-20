all: mergesort

mergesort: mergesort.o
	mpicc mergesort.o -o mergesort

mergesort.o: mergesort.c
	mpicc --std=c99 -c mergesort.c -o mergesort.o

clean:
	rm -f *.o mergesort