#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>

#include <uv.h>
#include <mysql.h>

typedef struct {
	int seq;
	char sql[1024];
	MYSQL mysql;
	MYSQL_RES *res;
} c_data_t;

int gettid() {
	return syscall(SYS_gettid);
}

int safe_snprintf(char *str, size_t size, const char *format, ...) {
	int ret = 0;
	va_list arg;

	va_start(arg, format);
	ret = vsnprintf(str, size, format, arg);
	if (ret < 0 || (size_t)ret > size) {
		str[0] = '\0';
		ret = -1;
	}
	else {
		str[ret] = '\0';
	}
	va_end(arg);
	return ret;
}

void work_cb(uv_work_t *req) {
	c_data_t *d = (c_data_t *)req->data;
	int t = 0;
	char *sql = d->sql;
	MYSQL *ms = &(d->mysql);
	printf("%s(%d): %d: sql: %s\n", __FUNCTION__, __LINE__, gettid(), sql);
	t = mysql_real_query(ms, sql, (unsigned int)strlen(sql));
	if (t) {
		printf("%s(%d): mysql_real_query error: %s\n", __FUNCTION__, __LINE__, mysql_error(ms));
		return;
	}
	d->res = mysql_store_result(ms);
	return;
}

void after_work_cb(uv_work_t *req, int status) {
	MYSQL_ROW row;
	int t;
	c_data_t *d = (c_data_t *)req->data;
	while ((row = mysql_fetch_row(d->res))) {
		printf("%d: %d ", gettid(), d->seq);
		for (t=0; t<mysql_num_fields(d->res); ++t) {
			printf("%s ", row[t]);
		}
		printf("\n");
	}
	mysql_free_result(d->res);
	return;
}

// int uv_queue_work(uv_loop_t* loop, uv_work_t* req, uv_work_cb work_cb, uv_after_work_cb after_work_cb)

int main(int argc, char* argv[]) {
	int i;
	uv_loop_t *loop = malloc(sizeof(uv_loop_t));
	uv_loop_init(loop);

	c_data_t data[10];

	uv_work_t worker[10];
	for (i=0; i<10; ++i) {
		data[i].seq = i;
		mysql_init(&(data[i].mysql));
		if (!mysql_real_connect(&(data[i].mysql), "localhost", "test2", "test123", "mi_train", 0, NULL, 0)) {
			printf("%s(%d): mysql_real_connect error: %s\n", __FUNCTION__, __LINE__, mysql_error(&(data[i].mysql)));
			return -1;
		} 
		printf("%s(%d): %d: mysql_real_connect ok.\n", __FUNCTION__, __LINE__, gettid());

		safe_snprintf(data[i].sql, sizeof(data[i].sql)-1, "select * from %s", "t_course");
		worker[i].data = (void *)&data[i];
		uv_queue_work(loop, &worker[i], work_cb, after_work_cb);
	}
	uv_run(loop, UV_RUN_DEFAULT);

	printf("Now, quitting.\n");
	uv_loop_close(loop);
	free(loop);
	for (i=0; i<10; ++i) {
		printf("close mysql %d\n", data[i].seq);
		mysql_close(&(data[i].mysql));
	}
	return 0;
}
