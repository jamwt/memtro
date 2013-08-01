gcc -fno-strict-aliasing `pkg-config --cflags nitro` -O2 -Wall -Werror -g -std=gnu99 -o $3 memtro_bench.c memtro.pb-c.c `pkg-config --libs nitro` -lprotobuf-c
