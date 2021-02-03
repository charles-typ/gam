// Test program to allocate new memory
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/mman.h>
#include <pthread.h>
#include <fstream>
#include <cassert>
#include <map>

#include <thread>
#include <atomic>
#include <iostream>
#include <cstring>
#include <mutex>


#define TEST_ALLOC_FLAG MAP_PRIVATE|MAP_ANONYMOUS    // default: 0xef
#define TEST_INIT_ALLOC_SIZE (unsigned long)9 * 1024 * 1024 * 1024 // default: 16 GB
#define TEST_METADATA_SIZE 16

#define LOG_NUM_ONCE (unsigned long)1000
#define LOG_NUM_TOTAL (unsigned long)50000000
#define MMAP_ADDR_MASK 0xffffffffffff
#define MAX_NUM_THREAD 4
#define SLEEP_THRES_NANOS 10
#define TIMEWINDOW_US 10000000
#define DEBUG_LEVEL LOG_WARNING
//#define SYNC_KEY 204800
#define PASS_KEY 40960000
#define SYNC_KEY (unsigned long)10 * 1024 * 1024 * 1024 // default: 10 GB
//#define num_comp_nodes 4
//#define NUM_MEM_NODES 2

int addr_size = sizeof(GAddr);

// Test configuration
//#define single_thread_test
//#define meta_data_testg

using namespace std;

struct log_header_5B {
  char op;
  unsigned int usec;
}__attribute__((__packed__));

struct RWlog {
  char op;
  union {
    struct {
      char pad[6];
      unsigned long usec;
    }__attribute__((__packed__));
    unsigned long addr;
  }__attribute__((__packed__));
}__attribute__((__packed__));

struct Mlog {
  struct log_header_5B hdr;
  union {
    unsigned long start;
    struct {
      char pad[6];
      unsigned len;
    }__attribute__((__packed__));
  }__attribute__((__packed__));
}__attribute__((__packed__));

struct Blog {
  char op;
  union {
    struct {
      char pad[6];
      unsigned long usec;
    }__attribute__((__packed__));
    unsigned long addr;
  }__attribute__((__packed__));
}__attribute__((__packed__));

struct Ulog {
  struct log_header_5B hdr;
  union {
    unsigned long start;
    struct {
      char pad[6];
      unsigned len;
    }__attribute__((__packed__));
  }__attribute__((__packed__));
}__attribute__((__packed__));

struct trace_t {
  char *logs;
  unsigned long len;
  int node_idx;
  int num_nodes;
  int num_comp_nodes;
  int master_thread;
  int tid;
  long time;
  long read_time;
  long read_ops;
  long write_time;
  long write_ops;
  unsigned long benchmark_size;
  double remote_ratio;
  bool is_master;
  bool is_compute;
  int num_threads;
  int pass;
};

struct memory_config_t {
  int num_comp_nodes;
};

struct trace_t args[MAX_NUM_THREAD];
struct memory_config_t memory_args;

struct metadata_t {
  unsigned int node_mask;
  unsigned int fini_node_pass[8];
};

// int first;
int num_nodes;
int num_comp_nodes;
int node_id = -1;
int num_threads;

int resize_ratio = 1;


static int calc_mask_sum(unsigned int mask) {
  int sum = 0;
  while (mask > 0) {
    if (mask & 0x1)
      sum++;
    mask >>= 1;
  }
  return sum;
}

int pin_to_core(int core_id) {
  int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
  if (core_id < 0 || core_id >= num_cores)
    return -1;

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  pthread_t current_thread = pthread_self();
  return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

inline void interval_between_access(long delta_time_usec) {
  if (delta_time_usec <= 0)
    return;
  else {
    struct timespec ts;
    ts.tv_nsec = (delta_time_usec << 1) / 3;
    printf("Nano seconds to sleep: %ld\n", ts.tv_nsec);
    if (ts.tv_nsec > SLEEP_THRES_NANOS) {
      ts.tv_sec = 0;
      nanosleep(&ts, NULL);
    }
  }
}

void do_log(void *arg) {
  //printf("Show the start of do_log\n");
  struct trace_t *trace = (struct trace_t *) arg;


  int ret;

  unsigned len = trace->len;
  unsigned long old_ts = 0;
  unsigned long i = 0;

  long pass_start = get_time();
  long total_interval = 0;
  char *cur;

  if (trace->is_compute) {
    //printf("This is a compute node, run everything here!\n");
    for (i = 0; i < trace->len; ++i) {
      volatile char op = trace->logs[i * sizeof(RWlog)];
      cur = &(trace->logs[i * sizeof(RWlog)]);
      if (op == 'R') {
        struct RWlog *log = (struct RWlog *) cur;
        if(log->usec - old_ts <= 999999999) {
          total_interval += log->usec - old_ts;
        }
        interval_between_access(log->usec - old_ts);
        char buf;
        unsigned long addr = log->addr & MMAP_ADDR_MASK;
	//printf("Address is: %lu\n", addr);
	//fflush(stdout);
        size_t cache_line_block = (addr) / (BLOCK_SIZE * resize_ratio);
        size_t cache_line_offset = (addr) % (BLOCK_SIZE * resize_ratio);
        old_ts = log->usec;

      } else if (op == 'W') {
        struct RWlog *log = (struct RWlog *) cur;
        if(log->usec - old_ts <= 999999999) {
          total_interval += log->usec - old_ts;
        }
        interval_between_access(log->usec - old_ts);
        char buf = '0';
        unsigned long addr = log->addr & MMAP_ADDR_MASK;
	//printf("Address is: %lu\n", addr);
	//fflush(stdout);
        size_t cache_line_block = (addr) / (BLOCK_SIZE * resize_ratio);
        size_t cache_line_offset = (addr) % (BLOCK_SIZE * resize_ratio);
        old_ts = log->usec;

      } else if (op == 'M') {
        struct Mlog *log = (struct Mlog *) cur;
        if(log->hdr.usec <= 999999999) {
          total_interval += log->hdr.usec;
        }
        interval_between_access(log->hdr.usec);
        unsigned int len = log->len;
        old_ts += log->hdr.usec;
      } else if (op == 'B') {
        struct Blog *log = (struct Blog *) cur;
        interval_between_access(log->usec - old_ts);
        if(log->usec - old_ts <= 999999999) {
          total_interval += log->usec - old_ts;
        }
        old_ts = log->usec;
      } else if (op == 'U') {
        struct Ulog *log = (struct Ulog *) cur;
        interval_between_access(log->hdr.usec);
        if(log->hdr.usec <= 999999999) {
          total_interval += log->hdr.usec;
        }
        old_ts += log->hdr.usec;
      } else {
        printf("unexpected log: %c at line: %lu\n", op, i);
      }
    }

    printf("sleep time is %ld\n", total_interval);
    fflush(stdout);
  }
}

void standalone(void *arg) {
  //printf("Show the start of standalone\n");
  struct memory_config_t *trace = (struct memory_config_t *) arg;
  //GAlloc *alloc = GAllocFactory::CreateAllocator();
  //for (int i = 1; i <= trace->num_comp_nodes; i++) {
    //printf("Getting %lld\n", SYNC_KEY + i + 10);
    //int id;
    //alloc->Get(SYNC_KEY + i + 10, &id);
    //printf("Get done \n");
    //epicAssert(id == i);
  //}
  while(1){}
}

int load_trace(int fd, struct trace_t *arg, unsigned long ts_limit) {
  //printf("ts_limit: %lu\n", ts_limit);
  assert(sizeof(RWlog) == sizeof(Mlog));
  assert(sizeof(RWlog) == sizeof(Blog));
  assert(sizeof(RWlog) == sizeof(Ulog));
/*
	char *chunk = (char *)malloc(LOG_NUM_TOTAL * sizeof(RWlog));
	char *buf;
	if (!chunk) {
		printf("fail to alloc buf to hold logs\n");
		return -1;
	} else {
		arg->logs = chunk;
	}
	int fd = open(trace_name, O_RDONLY);
	if (fd < 0) {
		printf("fail to open log file\n");
		return fd;
	}
*/
  char *chunk = arg->logs;
  memset(chunk, 0, LOG_NUM_TOTAL * sizeof(RWlog));
  size_t size = 0;
  for (char *buf = chunk; true; buf += LOG_NUM_ONCE * sizeof(RWlog)) {
    size_t dsize = read(fd, buf, LOG_NUM_ONCE * sizeof(RWlog));
    if (dsize == 0)
      break;
    if (dsize % sizeof(RWlog) != 0)
      printf("dsize is :%lu\n", dsize);
    size += dsize;

    char *tail = buf + dsize - sizeof(RWlog);
    unsigned long last_ts = 0;
    while (tail - buf >= 0) {
      if (*tail == 'R' || *tail == 'W' || *tail == 'B')
        last_ts = ((struct RWlog *) tail)->usec;
      else if (*tail == 'M' || *tail == 'U') {
        tail -= sizeof(RWlog);
        continue;
      } else
        printf("unexpected op %c\n", *tail);
      break;
    }
    if (last_ts >= ts_limit)
      break;
  }
  assert(size <= LOG_NUM_TOTAL * sizeof(RWlog));
  //assert(size % sizeof(RWlog) == 0);
  arg->len = size / (sizeof(RWlog));
  printf("finish loading %lu logs\n", arg->len);

  return 0;
}

enum {
  arg_node_cnt = 1,
  arg_num_threads = 2,
  arg_ip_master = 3,
  arg_ip_worker = 4,
  arg_port_master = 5,
  arg_port_worker = 6,
  arg_is_master = 7,
  arg_is_compute = 8,
  arg_remote_ratio = 9,
  arg_benchmark_size = 10,
  arg_num_comp_nodes = 11,
  arg_log1 = 12,
};

int main(int argc, char **argv) {
  int ret;
  char *buf_test = NULL;
  if (argc < arg_log1) {
    fprintf(stderr, "Incomplete args\n");
    return 1;
  }

  num_comp_nodes = atoi(argv[arg_num_comp_nodes]);
  num_nodes = atoi(argv[arg_node_cnt]);
  num_threads = atoi(argv[arg_num_threads]);
#ifdef single_thread_test
    num_threads = 1;
    num_comp_nodes = 1;
#endif
  string ip_master = string(argv[arg_ip_master]);
  string ip_worker = string(argv[arg_ip_worker]);
  int port_master = atoi(argv[arg_port_master]);
  int port_worker = atoi(argv[arg_port_worker]);
  //FIXME check this is failed
  bool is_master = atoi(argv[arg_is_master]);
  bool is_compute = atoi(argv[arg_is_compute]);
  double remote_ratio = atof(argv[arg_remote_ratio]);
  long long benchmark_size = atol(argv[arg_benchmark_size]);
  printf("%ld %d %f %d %d\n", benchmark_size, num_comp_nodes, remote_ratio, is_master, is_compute);
  printf("Num Nodes: %d, Num Threads: %d\n", num_nodes, num_threads);
#ifndef single_thread_test
  if (argc != arg_log1 + num_threads) {
    fprintf(stderr, "thread number and log files provided not match\n");
    return 1;
  }
#endif

  //open files
  if(is_compute) {
    int *fd = new int[num_threads];
    for (int i = 0; i < num_threads; ++i) {
      fd[i] = open(argv[arg_log1 + i], O_RDONLY);
      if (fd < 0) {
        printf("fail to open log file\n");
        return 1;
      }
    }

    //get start ts
    struct RWlog first_log;
    unsigned long start_ts = -1;
    for (int i = 0; i < num_threads; ++i) {
      int size = read(fd[i], &first_log, sizeof(RWlog));
      start_ts = min(start_ts, first_log.usec);
    }

    for (int i = 0; i < num_threads; ++i) {
      args[i].num_threads = num_threads;
      args[i].node_idx = node_id;
      args[i].num_nodes = num_nodes;
      args[i].num_comp_nodes = num_comp_nodes;
      args[i].master_thread = (i == 0);
      args[i].is_master = is_master;
      args[i].is_compute = is_compute;
      args[i].tid = i;
      args[i].logs = (char *) malloc(LOG_NUM_TOTAL * sizeof(RWlog)); // This should be allocated locally
      args[i].benchmark_size = benchmark_size;
      if (!args[i].logs)
        printf("fail to alloc buf to hold logs\n");
    }

    //start load and run logs in time window
    unsigned long pass = 0;
    unsigned long ts_limit = start_ts;
    //for (int i = 0; i < num_threads; ++i) {
    //  printf("Thread[%d]: loading log...\n", i);
    //  ret = load_trace(fd[i], &args[i], ts_limit);
    //  if (ret) {
    //    printf("fail to load trace\n");
    //  }
    //}
    while (1) {
      ts_limit += TIMEWINDOW_US;

      printf("Pass[%lu] Node[%d]: loading log...\n", pass, node_id);
      for (int i = 0; i < num_threads; ++i) {
        printf("Thread[%d]: loading log...\n", i);
        ret = load_trace(fd[i], &args[i], ts_limit);
        if (ret) {
          printf("fail to load trace\n");
        }
      }

      pthread_t thread[MAX_NUM_THREAD];
      printf("running trace...\n");

      for (int i = 0; i < num_threads; ++i) {
        args[i].pass = pass;
        printf("args has len: %d\n", args[i].len);
        if (args[i].len) {
          if (pthread_create(&thread[i], NULL, (void *(*)(void *)) do_log, &args[i])) {
            printf("Error creating thread %d\n", i);
            return 1;
          }
        }
      }

      for (int i = 0; i < num_threads; ++i) {
        if (args[i].len) {
          if (pthread_join(thread[i], NULL)) {
            printf("Error joining thread %d\n", i);
            return 2;
          }
        }
      }

      //sync on the end of the time window
      ++pass;

      bool all_done = true;
      for (int i = 0; i < num_threads; ++i) {
        if (args[i].len != 0) {
          all_done = false;
        }
      }
      if (all_done) {
        printf("All done here\n");
        break;
      }
    }

    for (int i = 0; i < num_threads; ++i) {
      close(fd[i]);
    }
    delete[] fd;
  } else {
    pthread_t memory_thread;
    memory_args.num_comp_nodes = num_comp_nodes;
    if (pthread_create(&memory_thread, NULL, (void *(*)(void *)) standalone, &memory_args)) {
      printf("Error creating memory thread \n");
      return 1;
    }
    if (pthread_join(memory_thread, NULL)) {
      printf("Error joining memory thread\n");
      return 2;
    }

  }

  return 0;
}

