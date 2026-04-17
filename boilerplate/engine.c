#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    int nice_value;
    int stop_requested;
    int monitor_registered;
    int producer_started;
    int producer_active;
    char reason[32];
    char log_path[PATH_MAX];
    pthread_t producer_thread;
    pthread_cond_t state_changed;
    void *child_stack;
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    int exit_code;
    uint32_t payload_len;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    pthread_t signal_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

typedef struct {
    supervisor_ctx_t *ctx;
    container_record_t *record;
    int read_fd;
} producer_args_t;

typedef struct {
    supervisor_ctx_t *ctx;
    int client_fd;
} client_handler_args_t;

typedef struct {
    supervisor_ctx_t *ctx;
    sigset_t signal_set;
} signal_thread_args_t;

static volatile sig_atomic_t g_run_interrupted;
static char g_run_container_id[CONTAINER_ID_LEN];

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static void run_client_signal_handler(int signo)
{
    (void)signo;
    g_run_interrupted = 1;
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *cursor = buf;
    size_t written = 0;

    while (written < len) {
        ssize_t rc = write(fd, cursor + written, len - written);
        if (rc < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        written += (size_t)rc;
    }

    return 0;
}

static int read_full_internal(int fd,
                              void *buf,
                              size_t len,
                              const char *run_container_id)
{
    char *cursor = buf;
    size_t read_bytes = 0;

    while (read_bytes < len) {
        ssize_t rc = read(fd, cursor + read_bytes, len - read_bytes);
        if (rc == 0)
            return -1;
        if (rc < 0) {
            if (errno == EINTR) {
                if (run_container_id != NULL && g_run_interrupted) {
                    control_request_t stop_req;
                    struct sockaddr_un addr;
                    int stop_fd;

                    memset(&stop_req, 0, sizeof(stop_req));
                    stop_req.kind = CMD_STOP;
                    strncpy(stop_req.container_id,
                            run_container_id,
                            sizeof(stop_req.container_id) - 1);

                    stop_fd = socket(AF_UNIX, SOCK_STREAM, 0);
                    if (stop_fd >= 0) {
                        memset(&addr, 0, sizeof(addr));
                        addr.sun_family = AF_UNIX;
                        strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);
                        if (connect(stop_fd,
                                    (struct sockaddr *)&addr,
                                    sizeof(addr)) == 0) {
                            control_response_t ignored;

                            (void)write_full(stop_fd, &stop_req, sizeof(stop_req));
                            (void)read_full_internal(stop_fd, &ignored, sizeof(ignored), NULL);
                            if (ignored.payload_len > 0) {
                                char sink[256];
                                size_t remaining = ignored.payload_len;
                                while (remaining > 0) {
                                    size_t chunk = remaining < sizeof(sink) ? remaining : sizeof(sink);
                                    if (read_full_internal(stop_fd, sink, chunk, NULL) != 0)
                                        break;
                                    remaining -= chunk;
                                }
                            }
                        }
                        close(stop_fd);
                    }
                    g_run_interrupted = 0;
                }
                continue;
            }
            return -1;
        }
        read_bytes += (size_t)rc;
    }

    return 0;
}

static int mkdir_if_missing(const char *path, mode_t mode)
{
    if (mkdir(path, mode) == 0)
        return 0;
    if (errno == EEXIST)
        return 0;
    return -1;
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

static int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

static int send_response(int fd,
                         int status,
                         int exit_code,
                         const char *message,
                         const char *payload)
{
    control_response_t resp;
    const char *safe_message = message != NULL ? message : "";
    size_t payload_len = payload != NULL ? strlen(payload) : 0;

    memset(&resp, 0, sizeof(resp));
    resp.status = status;
    resp.exit_code = exit_code;
    resp.payload_len = (uint32_t)payload_len;
    strncpy(resp.message, safe_message, sizeof(resp.message) - 1);

    if (write_full(fd, &resp, sizeof(resp)) != 0)
        return -1;
    if (payload_len > 0 && write_full(fd, payload, payload_len) != 0)
        return -1;
    return 0;
}

static container_record_t *find_container_by_id_locked(supervisor_ctx_t *ctx,
                                                       const char *id)
{
    container_record_t *cursor;

    for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
        if (strcmp(cursor->id, id) == 0)
            return cursor;
    }

    return NULL;
}

static container_record_t *find_container_by_pid_locked(supervisor_ctx_t *ctx,
                                                        pid_t pid)
{
    container_record_t *cursor;

    for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
        if (cursor->host_pid == pid)
            return cursor;
    }

    return NULL;
}

static int container_is_live(const container_record_t *record)
{
    return record->state == CONTAINER_STARTING || record->state == CONTAINER_RUNNING;
}

static int any_live_containers_locked(supervisor_ctx_t *ctx)
{
    container_record_t *cursor;

    for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
        if (container_is_live(cursor))
            return 1;
    }

    return 0;
}

static int validate_request(supervisor_ctx_t *ctx,
                            const control_request_t *req,
                            char *error,
                            size_t error_len)
{
    container_record_t *cursor;
    struct stat st;

    if (strlen(req->container_id) == 0 || strlen(req->container_id) >= CONTAINER_ID_LEN) {
        snprintf(error, error_len, "container id must be 1..%d characters", CONTAINER_ID_LEN - 1);
        return -1;
    }

    if (strlen(req->rootfs) == 0 || strlen(req->command) == 0) {
        snprintf(error, error_len, "rootfs and command are required");
        return -1;
    }

    if (stat(req->rootfs, &st) != 0 || !S_ISDIR(st.st_mode)) {
        snprintf(error, error_len, "rootfs path does not exist: %s", req->rootfs);
        return -1;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    if (find_container_by_id_locked(ctx, req->container_id) != NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        snprintf(error, error_len, "container id already exists: %s", req->container_id);
        return -1;
    }

    for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
        if (container_is_live(cursor) && strcmp(cursor->rootfs, req->rootfs) == 0) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(error,
                     error_len,
                     "rootfs already in use by running container: %s",
                     cursor->id);
            return -1;
        }
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return 0;
}

static void classify_exit_locked(container_record_t *record, int status)
{
    if (WIFEXITED(status)) {
        record->exit_code = WEXITSTATUS(status);
        record->exit_signal = 0;
        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            strncpy(record->reason, "stopped", sizeof(record->reason) - 1);
        } else {
            record->state = CONTAINER_EXITED;
            strncpy(record->reason, "exited", sizeof(record->reason) - 1);
        }
        return;
    }

    if (WIFSIGNALED(status)) {
        record->exit_code = 128 + WTERMSIG(status);
        record->exit_signal = WTERMSIG(status);
        if (record->stop_requested) {
            record->state = CONTAINER_STOPPED;
            strncpy(record->reason, "stopped", sizeof(record->reason) - 1);
        } else if (WTERMSIG(status) == SIGKILL) {
            record->state = CONTAINER_KILLED;
            strncpy(record->reason, "hard_limit_killed", sizeof(record->reason) - 1);
        } else {
            record->state = CONTAINER_KILLED;
            strncpy(record->reason, "signaled", sizeof(record->reason) - 1);
        }
        return;
    }

    record->state = CONTAINER_EXITED;
    strncpy(record->reason, "unknown", sizeof(record->reason) - 1);
}

static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        {
            container_record_t *record = find_container_by_pid_locked(ctx, pid);
            if (record != NULL) {
                classify_exit_locked(record, status);
                if (ctx->monitor_fd >= 0 && record->monitor_registered) {
                    (void)unregister_from_monitor(ctx->monitor_fd, record->id, record->host_pid);
                    record->monitor_registered = 0;
                }
                pthread_cond_broadcast(&record->state_changed);
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

static void request_supervisor_stop(supervisor_ctx_t *ctx)
{
    if (!ctx->should_stop)
        ctx->should_stop = 1;
    if (ctx->server_fd >= 0)
        shutdown(ctx->server_fd, SHUT_RDWR);
}

static void stop_all_containers(supervisor_ctx_t *ctx, int sig)
{
    container_record_t *cursor;

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
        if (!container_is_live(cursor))
            continue;
        cursor->stop_requested = 1;
        kill(cursor->host_pid, sig);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);
}

static void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = arg;
    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (fd < 0)
            continue;
        (void)write_full(fd, item.data, item.length);
        close(fd);
    }

    return NULL;
}

static void *producer_thread_main(void *arg)
{
    producer_args_t *producer = arg;
    supervisor_ctx_t *ctx = producer->ctx;
    container_record_t *record = producer->record;
    int read_fd = producer->read_fd;

    free(producer);

    for (;;) {
        char buf[LOG_CHUNK_SIZE];
        ssize_t bytes;
        log_item_t item;

        bytes = read(read_fd, buf, sizeof(buf));
        if (bytes == 0)
            break;
        if (bytes < 0) {
            if (errno == EINTR)
                continue;
            break;
        }

        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, record->id, sizeof(item.container_id) - 1);
        item.length = (size_t)bytes;
        memcpy(item.data, buf, item.length);

        if (bounded_buffer_push(&ctx->log_buffer, &item) != 0)
            break;
    }

    close(read_fd);

    pthread_mutex_lock(&ctx->metadata_lock);
    record->producer_active = 0;
    pthread_cond_broadcast(&record->state_changed);
    pthread_mutex_unlock(&ctx->metadata_lock);

    return NULL;
}

static int child_fn(void *arg)
{
    child_config_t *cfg = arg;

    errno = 0;
    if (cfg->nice_value != 0 && nice(cfg->nice_value) < 0 && errno != 0) {
        perror("nice");
        return 1;
    }

    if (sethostname(cfg->id, strlen(cfg->id)) != 0) {
        perror("sethostname");
        return 1;
    }

    if (mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL) != 0) {
        perror("mount-private");
        return 1;
    }

    if (chdir(cfg->rootfs) != 0) {
        perror("chdir-rootfs");
        return 1;
    }

    if (chroot(".") != 0) {
        perror("chroot");
        return 1;
    }

    if (chdir("/") != 0) {
        perror("chdir-slash");
        return 1;
    }

    if (mkdir("/proc", 0555) != 0 && errno != EEXIST) {
        perror("mkdir-proc");
        return 1;
    }

    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        perror("mount-proc");
        return 1;
    }

    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }

    close(cfg->log_write_fd);

    execl("/bin/sh", "sh", "-c", cfg->command, (char *)NULL);
    perror("execl");
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

static int start_container(supervisor_ctx_t *ctx,
                           const control_request_t *req,
                           int wait_for_exit,
                           int client_fd)
{
    container_record_t *record = NULL;
    producer_args_t *producer_args = NULL;
    child_config_t *cfg = NULL;
    int pipe_fds[2] = {-1, -1};
    int rc = 1;
    char error[CONTROL_MESSAGE_LEN];
    pid_t child_pid = -1;
    int record_inserted = 0;

    if (validate_request(ctx, req, error, sizeof(error)) != 0)
        return send_response(client_fd, 1, 1, error, NULL);

    record = calloc(1, sizeof(*record));
    if (record == NULL)
        return send_response(client_fd, 1, 1, "out of memory", NULL);

    strncpy(record->id, req->container_id, sizeof(record->id) - 1);
    strncpy(record->rootfs, req->rootfs, sizeof(record->rootfs) - 1);
    strncpy(record->command, req->command, sizeof(record->command) - 1);
    record->soft_limit_bytes = req->soft_limit_bytes;
    record->hard_limit_bytes = req->hard_limit_bytes;
    record->nice_value = req->nice_value;
    record->state = CONTAINER_STARTING;
    record->exit_code = -1;
    record->started_at = time(NULL);
    snprintf(record->log_path, sizeof(record->log_path), "%s/%s.log", LOG_DIR, record->id);
    strncpy(record->reason, "starting", sizeof(record->reason) - 1);

    if (pthread_cond_init(&record->state_changed, NULL) != 0) {
        free(record);
        return send_response(client_fd, 1, 1, "failed to initialize record", NULL);
    }

    if (pipe(pipe_fds) != 0) {
        pthread_cond_destroy(&record->state_changed);
        free(record);
        return send_response(client_fd, 1, 1, "failed to create log pipe", NULL);
    }

    record->child_stack = malloc(STACK_SIZE);
    cfg = calloc(1, sizeof(*cfg));
    producer_args = calloc(1, sizeof(*producer_args));
    if (record->child_stack == NULL || cfg == NULL || producer_args == NULL) {
        send_response(client_fd, 1, 1, "out of memory", NULL);
        goto cleanup;
    }

    cfg->log_write_fd = pipe_fds[1];
    cfg->nice_value = req->nice_value;
    strncpy(cfg->id, req->container_id, sizeof(cfg->id) - 1);
    strncpy(cfg->rootfs, req->rootfs, sizeof(cfg->rootfs) - 1);
    strncpy(cfg->command, req->command, sizeof(cfg->command) - 1);

    {
        int log_fd = open(record->log_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (log_fd < 0) {
            send_response(client_fd, 1, 1, "failed to create log file", NULL);
            goto cleanup;
        }
        close(log_fd);
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    record->next = ctx->containers;
    ctx->containers = record;
    pthread_mutex_unlock(&ctx->metadata_lock);
    record_inserted = 1;

    child_pid = clone(child_fn,
                      (char *)record->child_stack + STACK_SIZE,
                      CLONE_NEWUTS | CLONE_NEWPID | CLONE_NEWNS | SIGCHLD,
                      cfg);
    if (child_pid < 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        if (ctx->containers == record) {
            ctx->containers = record->next;
        } else {
            container_record_t *cursor;
            for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
                if (cursor->next == record) {
                    cursor->next = record->next;
                    break;
                }
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        record_inserted = 0;
        send_response(client_fd, 1, 1, "clone failed", NULL);
        goto cleanup;
    }

    close(pipe_fds[1]);
    pipe_fds[1] = -1;
    free(cfg);
    cfg = NULL;

    producer_args->ctx = ctx;
    producer_args->record = record;
    producer_args->read_fd = pipe_fds[0];
    pipe_fds[0] = -1;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->host_pid = child_pid;
    record->state = CONTAINER_RUNNING;
    strncpy(record->reason, "running", sizeof(record->reason) - 1);
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pthread_create(&record->producer_thread, NULL, producer_thread_main, producer_args) != 0) {
        producer_args = NULL;
        kill(child_pid, SIGKILL);
        send_response(client_fd, 1, 1, "failed to create log producer thread", NULL);
        goto cleanup;
    }
    producer_args = NULL;

    pthread_mutex_lock(&ctx->metadata_lock);
    record->producer_started = 1;
    record->producer_active = 1;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (ctx->monitor_fd >= 0 &&
        register_with_monitor(ctx->monitor_fd,
                              record->id,
                              record->host_pid,
                              record->soft_limit_bytes,
                              record->hard_limit_bytes) == 0) {
        pthread_mutex_lock(&ctx->metadata_lock);
        record->monitor_registered = 1;
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    if (!wait_for_exit) {
        rc = send_response(client_fd, 0, 0, "container accepted by supervisor", NULL);
        return rc;
    }

    pthread_mutex_lock(&ctx->metadata_lock);
    while (container_is_live(record))
        pthread_cond_wait(&record->state_changed, &ctx->metadata_lock);
    rc = send_response(client_fd, 0, record->exit_code, record->reason, NULL);
    pthread_mutex_unlock(&ctx->metadata_lock);
    return rc;

cleanup:
    if (pipe_fds[0] >= 0)
        close(pipe_fds[0]);
    if (pipe_fds[1] >= 0)
        close(pipe_fds[1]);
    if (producer_args != NULL)
        free(producer_args);
    if (cfg != NULL)
        free(cfg);
    if (record != NULL && !record_inserted) {
        if (record->child_stack != NULL)
            free(record->child_stack);
        pthread_cond_destroy(&record->state_changed);
        free(record);
    }
    return 1;
}

static char *render_ps(supervisor_ctx_t *ctx)
{
    size_t capacity = 8192;
    size_t used = 0;
    char *out = malloc(capacity);
    container_record_t *cursor;

    if (out == NULL)
        return NULL;

    used += (size_t)snprintf(out + used,
                             capacity - used,
                             "ID\tPID\tSTATE\tREASON\tSOFT_MIB\tHARD_MIB\tNICE\tSTARTED\tEXIT\tLOG\n");

    pthread_mutex_lock(&ctx->metadata_lock);
    for (cursor = ctx->containers; cursor != NULL; cursor = cursor->next) {
        char ts[64];
        struct tm tm_buf;
        localtime_r(&cursor->started_at, &tm_buf);
        strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

        if (used + 512 >= capacity) {
            char *grown;
            capacity *= 2;
            grown = realloc(out, capacity);
            if (grown == NULL) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                free(out);
                return NULL;
            }
            out = grown;
        }

        used += (size_t)snprintf(out + used,
                                 capacity - used,
                                 "%s\t%d\t%s\t%s\t%lu\t%lu\t%d\t%s\t%d\t%s\n",
                                 cursor->id,
                                 cursor->host_pid,
                                 state_to_string(cursor->state),
                                 cursor->reason,
                                 cursor->soft_limit_bytes >> 20,
                                 cursor->hard_limit_bytes >> 20,
                                 cursor->nice_value,
                                 ts,
                                 cursor->exit_code,
                                 cursor->log_path);
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    return out;
}

static char *slurp_file(const char *path)
{
    FILE *fp;
    char *buf;
    long len;

    fp = fopen(path, "rb");
    if (fp == NULL)
        return NULL;

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }

    len = ftell(fp);
    if (len < 0) {
        fclose(fp);
        return NULL;
    }

    if (fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }

    buf = calloc(1, (size_t)len + 1);
    if (buf == NULL) {
        fclose(fp);
        return NULL;
    }

    if (len > 0 && fread(buf, 1, (size_t)len, fp) != (size_t)len) {
        fclose(fp);
        free(buf);
        return NULL;
    }

    fclose(fp);
    return buf;
}

static int handle_logs(supervisor_ctx_t *ctx,
                       const control_request_t *req,
                       int client_fd)
{
    char log_path[PATH_MAX];
    char *payload;
    int found = 0;
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, req->container_id);
    if (record != NULL) {
        strncpy(log_path, record->log_path, sizeof(log_path) - 1);
        log_path[sizeof(log_path) - 1] = '\0';
        found = 1;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!found)
        return send_response(client_fd, 1, 1, "unknown container id", NULL);

    payload = slurp_file(log_path);
    if (payload == NULL)
        return send_response(client_fd, 1, 1, "failed to read log file", NULL);

    {
        int rc = send_response(client_fd, 0, 0, "ok", payload);
        free(payload);
        return rc;
    }
}

static int handle_stop(supervisor_ctx_t *ctx,
                       const control_request_t *req,
                       int client_fd)
{
    container_record_t *record;

    pthread_mutex_lock(&ctx->metadata_lock);
    record = find_container_by_id_locked(ctx, req->container_id);
    if (record == NULL) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return send_response(client_fd, 1, 1, "unknown container id", NULL);
    }

    if (!container_is_live(record)) {
        pthread_mutex_unlock(&ctx->metadata_lock);
        return send_response(client_fd, 1, 1, "container is not running", NULL);
    }

    record->stop_requested = 1;
    kill(record->host_pid, SIGTERM);
    pthread_mutex_unlock(&ctx->metadata_lock);

    return send_response(client_fd, 0, 0, "stop signal sent", NULL);
}

static void *client_handler_thread(void *arg)
{
    client_handler_args_t *handler = arg;
    supervisor_ctx_t *ctx = handler->ctx;
    int client_fd = handler->client_fd;
    control_request_t req;

    free(handler);

    if (read_full_internal(client_fd, &req, sizeof(req), NULL) != 0) {
        close(client_fd);
        return NULL;
    }

    switch (req.kind) {
    case CMD_START:
        (void)start_container(ctx, &req, 0, client_fd);
        break;
    case CMD_RUN:
        (void)start_container(ctx, &req, 1, client_fd);
        break;
    case CMD_PS:
    {
        char *payload = render_ps(ctx);
        if (payload == NULL)
            (void)send_response(client_fd, 1, 1, "failed to render container list", NULL);
        else {
            (void)send_response(client_fd, 0, 0, "ok", payload);
            free(payload);
        }
        break;
    }
    case CMD_LOGS:
        (void)handle_logs(ctx, &req, client_fd);
        break;
    case CMD_STOP:
        (void)handle_stop(ctx, &req, client_fd);
        break;
    default:
        (void)send_response(client_fd, 1, 1, "unsupported command", NULL);
        break;
    }

    close(client_fd);
    return NULL;
}

static void *signal_thread_main(void *arg)
{
    signal_thread_args_t *state = arg;
    supervisor_ctx_t *ctx = state->ctx;

    for (;;) {
        int sig;

        if (sigwait(&state->signal_set, &sig) != 0)
            continue;

        if (sig == SIGCHLD) {
            reap_children(ctx);
        } else if (sig == SIGINT || sig == SIGTERM) {
            request_supervisor_stop(ctx);
            stop_all_containers(ctx, SIGTERM);
            reap_children(ctx);
        }

        if (ctx->should_stop) {
            pthread_mutex_lock(&ctx->metadata_lock);
            if (!any_live_containers_locked(ctx)) {
                pthread_mutex_unlock(&ctx->metadata_lock);
                break;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);
        }
    }

    free(state);
    return NULL;
}

static void cleanup_records(supervisor_ctx_t *ctx)
{
    container_record_t *cursor = ctx->containers;

    while (cursor != NULL) {
        container_record_t *next = cursor->next;

        if (cursor->producer_started)
            pthread_join(cursor->producer_thread, NULL);
        if (cursor->monitor_registered && ctx->monitor_fd >= 0)
            (void)unregister_from_monitor(ctx->monitor_fd, cursor->id, cursor->host_pid);
        pthread_cond_destroy(&cursor->state_changed);
        free(cursor->child_stack);
        free(cursor);
        cursor = next;
    }

    ctx->containers = NULL;
}

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;
    int accepted_fd;
    struct sockaddr_un addr;
    sigset_t signal_set;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    if (mkdir_if_missing(LOG_DIR, 0755) != 0)
        perror("mkdir logs");

    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        perror("open /dev/container_monitor");

    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGCHLD);
    sigaddset(&signal_set, SIGINT);
    sigaddset(&signal_set, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signal_set, NULL) != 0) {
        perror("pthread_sigmask");
        goto fail;
    }

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto fail;
    }

    unlink(CONTROL_PATH);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        goto fail;
    }

    if (listen(ctx.server_fd, 16) != 0) {
        perror("listen");
        goto fail;
    }

    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger");
        goto fail;
    }

    {
        signal_thread_args_t *signal_args = calloc(1, sizeof(*signal_args));
        if (signal_args == NULL) {
            perror("calloc signal_args");
            goto fail_after_logger;
        }
        signal_args->ctx = &ctx;
        signal_args->signal_set = signal_set;
        if (pthread_create(&ctx.signal_thread, NULL, signal_thread_main, signal_args) != 0) {
            perror("pthread_create signal");
            free(signal_args);
            goto fail_after_logger;
        }
    }

    fprintf(stderr, "Supervisor listening on %s with base-rootfs template %s\n",
            CONTROL_PATH,
            rootfs);

    while (!ctx.should_stop) {
        client_handler_args_t *handler_args;
        pthread_t tid;

        accepted_fd = accept(ctx.server_fd, NULL, NULL);
        if (accepted_fd < 0) {
            if (errno == EINTR)
                continue;
            if (ctx.should_stop)
                break;
            perror("accept");
            request_supervisor_stop(&ctx);
            break;
        }

        handler_args = calloc(1, sizeof(*handler_args));
        if (handler_args == NULL) {
            close(accepted_fd);
            continue;
        }

        handler_args->ctx = &ctx;
        handler_args->client_fd = accepted_fd;
        if (pthread_create(&tid, NULL, client_handler_thread, handler_args) != 0) {
            close(accepted_fd);
            free(handler_args);
            continue;
        }
        pthread_detach(tid);
    }

    request_supervisor_stop(&ctx);
    stop_all_containers(&ctx, SIGTERM);
    kill(getpid(), SIGTERM);
    pthread_join(ctx.signal_thread, NULL);
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    cleanup_records(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;

fail_after_logger:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
fail:
    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    unlink(CONTROL_PATH);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    cleanup_records(&ctx);
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 1;
}

static int send_control_request(const control_request_t *req)
{
    struct sockaddr_un addr;
    control_response_t resp;
    int fd;
    char *payload = NULL;
    int rc = 1;
    struct sigaction old_int;
    struct sigaction old_term;
    struct sigaction sa;
    const char *run_id = NULL;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    if (req->kind == CMD_RUN) {
        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = run_client_signal_handler;
        sigemptyset(&sa.sa_mask);
        sigaction(SIGINT, &sa, &old_int);
        sigaction(SIGTERM, &sa, &old_term);
        strncpy(g_run_container_id, req->container_id, sizeof(g_run_container_id) - 1);
        g_run_container_id[sizeof(g_run_container_id) - 1] = '\0';
        g_run_interrupted = 0;
        run_id = g_run_container_id;
    }

    if (write_full(fd, req, sizeof(*req)) != 0) {
        perror("write");
        goto done;
    }

    if (read_full_internal(fd, &resp, sizeof(resp), run_id) != 0) {
        perror("read");
        goto done;
    }

    if (resp.payload_len > 0) {
        payload = calloc(1, (size_t)resp.payload_len + 1);
        if (payload == NULL) {
            fprintf(stderr, "out of memory\n");
            goto done;
        }
        if (read_full_internal(fd, payload, resp.payload_len, run_id) != 0) {
            perror("read payload");
            goto done;
        }
    }

    if (resp.status != 0) {
        fprintf(stderr, "%s\n", resp.message);
        rc = 1;
        goto done;
    }

    if (payload != NULL)
        fwrite(payload, 1, strlen(payload), stdout);
    else if (strlen(resp.message) > 0)
        printf("%s\n", resp.message);

    rc = (req->kind == CMD_RUN) ? resp.exit_code : 0;

done:
    if (req->kind == CMD_RUN) {
        sigaction(SIGINT, &old_int, NULL);
        sigaction(SIGTERM, &old_term, NULL);
        memset(g_run_container_id, 0, sizeof(g_run_container_id));
        g_run_interrupted = 0;
    }
    free(payload);
    close(fd);
    return rc;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
