#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

#include "hash_table.h"

// This code is designed to stress test your hash table implementation. You do
// not need to significantly change it, but you may want to vary the value of
// num_tests to control the amount of time and memory that benchmarking takes
// up. Compile and run it in the command line by typing:
// make benchmark; ./benchmark



int main(void) {

  hashtable* ht=NULL;
  int num_tests = 50000000;
  int failure = allocate(&ht, num_tests);
  assert(!failure);

  int seed = 2;
  srand(seed);
  printf("Performing stress test. Inserting 50 million keys.\n");

  struct timeval stop, start;
  gettimeofday(&start, NULL);

  for (int i = 0; i < num_tests; i += 1) {
    int key = rand();
    int val = rand();
    failure = put(ht, key, val);
    assert(!failure);
  }

  gettimeofday(&stop, NULL);
  double secs = (double)(stop.tv_usec - start.tv_usec) / 1000000 + (double)(stop.tv_sec - start.tv_sec); 
  printf("50 million insertions took %f seconds\n", secs);

  failure = deallocate(ht);
  assert(failure);

  return 0;
}
