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

#define TEST_ALLOC_FLAG MAP_PRIVATE|MAP_ANONYMOUS	// default: 0xef
#define TEST_INIT_ALLOC_SIZE (unsigned long)9 * 1024 * 1024 * 1024 // default: 16 GB
#define TEST_METADATA_SIZE 16

#define LOG_NUM_ONCE (unsigned long)1000
#define LOG_NUM_TOTAL (unsigned long)50000000
#define MMAP_ADDR_MASK 0xffffffffffff
#define MAX_NUM_THREAD 4
#define SLEEP_THRES_NANOS 10
#define TIMEWINDOW_US 10000000

// Test configuration
// #define single_thread_test
//#define meta_data_test

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
	/*
	char *access_type;
	unsigned long *addr;
	unsigned long *ts;
	*/
	char *logs;
	unsigned long len;
	char *meta_buf;
	char *data_buf;
	int node_idx;
	int num_nodes;
	int master_thread;
	int tid;
	unsigned long time;
};
struct trace_t args[MAX_NUM_THREAD];

struct metadata_t {
	unsigned int node_mask;
	unsigned int fini_node_pass[8];
};

// int first;
int num_nodes;
int node_id = -1;
int num_threads;

static int calc_mask_sum(unsigned int mask)
{
	int sum = 0;
	while (mask > 0)
	{
		if (mask & 0x1)
			sum++;
		mask >>= 1;
	}
	return sum;
}

int pin_to_core(int core_id)
{
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
		if (ts.tv_nsec > SLEEP_THRES_NANOS) {
			ts.tv_sec = 0;
			nanosleep(&ts, NULL);
		}
	}
}


void do_log(void *arg)
{
	struct trace_t *trace = (struct trace_t*) arg;

	//pin to core first
	pin_to_core(trace->tid);

	unsigned len = trace->len;

#ifndef meta_data_test
	multimap<unsigned int, void *> len2addr;
	unsigned long old_ts = 0;
	unsigned long i = 0;

	struct timeval ts;
	gettimeofday(&ts, NULL);
	char *cur;
	for (i = 0; i < trace->len ; ++i) {
		volatile char op = trace->logs[i * sizeof(RWlog)];
		cur = &(trace->logs[i * sizeof(RWlog)]);
		if (op == 'R') {
			struct RWlog *log = (struct RWlog *)cur;
			interval_between_access(log->usec - old_ts);
			//assert((log->addr & MMAP_ADDR_MASK) < TEST_INIT_ALLOC_SIZE);
			//char val = trace->data_buf[log->addr & MMAP_ADDR_MASK];
			char *data_buf = trace->data_buf;
			unsigned long addr = log->addr & MMAP_ADDR_MASK;
			char val = data_buf[addr];

			old_ts = log->usec;
		} else if (op == 'W') {
			struct RWlog *log = (struct RWlog *)cur;
			interval_between_access(log->usec - old_ts);
			//assert((log->addr & MMAP_ADDR_MASK) < TEST_INIT_ALLOC_SIZE);
			//trace->data_buf[log->addr & MMAP_ADDR_MASK] = 0;
			char *data_buf = trace->data_buf;
			unsigned long addr = log->addr & MMAP_ADDR_MASK;
			data_buf[addr] = 0;

			old_ts = log->usec;
		} else if (op == 'M') {
			struct Mlog *log = (struct Mlog *)cur;
			interval_between_access(log->hdr.usec);
			void *ret_addr = mmap((void *)(log->start & MMAP_ADDR_MASK), log->len, PROT_READ|PROT_WRITE, TEST_ALLOC_FLAG, -1, 0);
			unsigned int len = log->len;
			len2addr.insert(pair<unsigned int, void *>(len, ret_addr));
			old_ts += log->hdr.usec;
		} else if (op == 'B') {
			struct Blog *log = (struct Blog *)cur;
			interval_between_access(log->usec - old_ts);
			brk((void *)(log->addr & MMAP_ADDR_MASK));
			old_ts = log->usec;
		} else if (op == 'U') {
			struct Ulog *log = (struct Ulog *)cur;
			interval_between_access(log->hdr.usec);
			auto itr = len2addr.find(log->len);
			if (itr == len2addr.end())
//				printf("no mapping to unmap\n");
				;
			else {
				munmap(itr->second, log->len);
				len2addr.erase(itr);
			}
			old_ts += log->hdr.usec;
		} else {
			printf("unexpected log: %c at line: %lu\n", op, i);
		}
		//if (i % 1000000 == 0)
		//	printf("%lu\n", i);
	}

/*
	volatile unsigned j;
	for (j = 0; j < len; ++j);
*/
	unsigned long old_t = ts.tv_sec * 1000000 + ts.tv_usec;
	gettimeofday(&ts, NULL);
	unsigned long dt = ts.tv_sec * 1000000 + ts.tv_usec - old_t;
	
#endif
	printf("done in %lu us\n", dt);
	trace->time += dt;
	printf("total run time is %lu us\n", trace->time);
}

int load_trace(int fd, struct trace_t *arg, unsigned long ts_limit) {
	printf("ts_limit: %lu\n", ts_limit);
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
				last_ts = ((struct RWlog *)tail)->usec;
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
/*
		for (buf = chunk; buf != chunk + size; buf += sizeof(RWlog)) {
			char op = buf[0];
                	if (op == 'R' || op == 'W') {
                		struct RWlog *log = (struct RWlog *)buf;
                		//printf("%c %lu %lx\n", log->op, log->usec, log->addr & MMAP_ADDR_MASK);
                	} else if (op == 'M') {
                		struct Mlog *log = (struct Mlog *)buf;
				//printf("M %u %lx %u\n", log->hdr.usec, log->start & MMAP_ADDR_MASK, log->len);
			} else if (op == 'B') {
                		struct Blog *log = (struct Blog *)buf;
				//printf("B %lu %lx\n", log->usec, log->addr & MMAP_ADDR_MASK);
			} else if (op == 'U') {
                		struct Ulog *log = (struct Ulog *)buf;
				//printf("U %u %lx %u\n", log->hdr.usec, log->start & MMAP_ADDR_MASK, log->len);
			} else {
                        	printf("unexpected op: %c\n", op);
			}
		}
*/

//	close(fd);
	return 0;
}
/*
void print_res(char *trace_name, struct trace_t *trace) {
	FILE *fp;
	fp = fopen(trace_name, "w");
	if (!fp) {
		printf("fail to open res file\n");
		return;
	}
	//printf("%d\n", size);
	for (int i = 0; i < trace->len; ++i) {
	//	printf("%d %hhu\n", i, buf[i]);
		//fprintf(fp, "%c %lu %hhu\n", trace->access_type[i], trace->addr[i], trace->val[i]);
	}
	fclose(fp);
}
*/
enum
{
	arg_node_cnt = 1,
	arg_node_id = 2,
	arg_num_threads = 3,
	arg_log1 = 4,
};

int main(int argc, char **argv)
{
	// const int ALLOC_SIZE = 9999 * 4096;
	//starts from few pages, dense access
   	int ret;
	char *buf_test = NULL;
	if (argc < arg_log1)
	{
		fprintf(stderr, "Incomplete args\n");
		return 1;
	}
	num_nodes = atoi(argv[arg_node_cnt]);
	node_id = atoi(argv[arg_node_id]);
	num_threads = atoi(argv[arg_num_threads]);
	printf("Num Nodes: %d, Num Threads: %d\n", num_nodes, num_threads);
	if (argc != arg_log1 + num_threads) {
		fprintf(stderr, "thread number and log files provided not match\n");
                return 1;
	}

	//open files
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
	printf("start ts is: %lu\n", start_ts);

	// init traces
	// ====== NOTE =====
        // If we make access to the memory ranges other than this size with TEST_ALLOC_FLAG flag,
        // it will generate segmentation fault
        // =================
        char *meta_buf = (char *)mmap(NULL, TEST_INIT_ALLOC_SIZE, PROT_READ | PROT_WRITE, TEST_ALLOC_FLAG, -1, 0);
        if (!meta_buf) {
                printf("can not allocate test buff\n");
                //return 1;
        } else {
                printf("meta buf: %p\n", meta_buf);
        }
//	for(int i = 0; i < 10; ++i) {
//		printf("sleeping %d, please run the same executable on other machines now...\n", i);
//		sleep(1);
//	}


	/* put this back when run with switch */
	/*
	buf_test = (char *)mmap(NULL, TEST_INIT_ALLOC_SIZE, PROT_READ | PROT_WRITE, TEST_ALLOC_FLAG, -1, 0);
	// arg1.buf = arg2.buf = (char *)mmap(NULL, ALLOC_SIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
	//memset(arg.buf, -1, ALLOC_SIZE);
	printf("protocol testing buf addr is: %p\n", arg1.meta_buf);
	if (buf_test != arg1.meta_buf)
	{
		fprintf(stderr, "Protocol testing buf verification failed: %p <-> %p\n", arg1.meta_buf, buf_test);
		return 1;
	}
	*/
        for (int i = 0; i < num_threads; ++i) {
                args[i].node_idx = node_id;
                args[i].meta_buf = meta_buf;
                args[i].data_buf = meta_buf + TEST_METADATA_SIZE;
                args[i].num_nodes = num_nodes;
                args[i].master_thread = (i == 0);
                args[i].tid = i;
		args[i].logs = (char *)malloc(LOG_NUM_TOTAL * sizeof(RWlog));
        	if (!args[i].logs)
                	printf("fail to alloc buf to hold logs\n");
        }

	//start load and run logs in time window
	unsigned long pass = 0;
	unsigned long ts_limit = start_ts;
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
		//printf("running trace...\n");

#ifdef single_thread_test
		num_threads = 1;
#endif
		for (int i = 0; i < num_threads; ++i) {
			if (args[i].len) {
				if (pthread_create(&thread[i], NULL, (void *(*)(void *))do_log, &args[i]))
        			{
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
		fini((metadata_t *)meta_buf, num_nodes, node_id, pass);

		bool all_done = true;
		for (int i = 0; i < num_threads; ++i)
			if (args[i].len)
				all_done = false;
		if (all_done)
			break;
	}

	for (int i = 0; i < num_threads; ++i) {
		close(fd[i]);
	}
	delete[] fd;

	while(1)
		sleep(30);

	return 0;
}

