/* Author: Darin Nikolow */ 
/* Generate or check blocks with random data and its md5 included */
/* Compile: gcc -O gbs.c -lcrypto -o gbs */
/*      or: gcc -O gbs.c -Wl,-Bstatic -lcrypto -Wl,-Bdynamic -o gbs */
/* Usage: ./gbs <block_size> <seed> <num_blocks> [w|r] */

#include <stdio.h>
#include <stdlib.h>
#include <openssl/md5.h>
#include <unistd.h>


int gen_block(int block_size, long long *buf) {
  unsigned char *h;
  int bs2, ret;
  bs2 = block_size/sizeof(*buf);
  for (int i=0; i<bs2-16/sizeof(*buf); i++) {
    buf[i] = rand();
  }
  h = MD5((unsigned char*)buf, block_size-16, (unsigned char *)&buf[bs2-16/sizeof(*buf)]);
  ret = write(1, buf, block_size);
  return 0;
}

int check_block(int block_size, unsigned char *buf) {
  int nr=0, rc; // number of bytes read so far
  unsigned char h[16];
  unsigned char *hr;
  int flag=0;
  while ((rc = read(0, buf+nr, block_size-nr)) != block_size-nr) {
    nr += rc;
    if (rc <= 0) {
      fprintf(stderr, "Read error\n");
    }
  }
  hr = MD5((unsigned char*)buf, block_size-16, h);
  for (int i=0; i<16; i++) {
    if (h[i] != buf[block_size-16+i]) {
      flag = -1;
      break;
    }
  }
  return flag;
}

int main(int argc, char *argv[]) {
  int bs, seed, n;
  long long *a;
  bs = atoi(argv[1]);
  seed = atoi(argv[2]);
  n = atoi(argv[3]);
  if (bs % sizeof(*a) != 0) {
    fprintf(stderr, "Block size is not aligned to %ld\n", sizeof(*a));
    exit(-1);
  }
  a = malloc(bs);
  if (*argv[4] == 'w') {
    srand(seed);
    for (int i=0; i<n; i++) {
      gen_block(bs, a);
    }
  }
  else {
    for (int i=0; i<n; i++) {
      if (check_block(bs, (unsigned char *)a) < 0) {
	fprintf(stderr, "Block nr %d is corrupted\n", i);
	exit(-1);
      }
    }
  }
  return 0;
}

