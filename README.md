# Multi-Container Runtime

Lightweight Linux container runtime in C with:

- a long-running supervisor
- a bounded-buffer logging pipeline
- a kernel-space memory monitor
- helper workloads for memory and scheduler experiments

The implementation lives in [`boilerplate/`](./boilerplate), which this submission keeps as the main source directory. The project guide remains in [`project-guide.md`](./project-guide.md).

## 1. Team Information

Fill this in before submission.

| Name | SRN |
| --- | --- |
| Student 1 | `TODO` |
| Student 2 | `TODO` |

## 2. Repository Layout

| Path | Purpose |
| --- | --- |
| `boilerplate/engine.c` | User-space supervisor, CLI, container launch, logging pipeline |
| `boilerplate/monitor.c` | Kernel module for soft/hard memory enforcement |
| `boilerplate/monitor_ioctl.h` | Shared `ioctl` interface |
| `boilerplate/cpu_hog.c` | CPU-bound scheduler workload |
| `boilerplate/io_pulse.c` | I/O-oriented scheduler workload |
| `boilerplate/memory_hog.c` | Memory pressure workload |
| `boilerplate/Makefile` | Build targets for user-space and kernel-space pieces |

## 3. Build, Load, and Run

This project is meant to be built and demonstrated on an Ubuntu 22.04 or 24.04 VM with Secure Boot disabled. Do not use WSL for the final demo.

Install dependencies:

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r)
```

Run the environment check:

```bash
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
```

Prepare the Alpine rootfs template:

```bash
cd ..
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

Build the project:

```bash
cd boilerplate
make
```

Load the kernel module:

```bash
sudo insmod monitor.ko
ls -l /dev/container_monitor
```

Start the supervisor:

```bash
sudo ./engine supervisor ../rootfs-base
```

Create per-container writable root filesystems in another terminal:

```bash
cd ..
cp -a ./rootfs-base ./rootfs-alpha
cp -a ./rootfs-base ./rootfs-beta
```

Copy helper binaries into a container rootfs before launch when needed:

```bash
cp ./boilerplate/cpu_hog ./rootfs-alpha/
cp ./boilerplate/io_pulse ./rootfs-beta/
cp ./boilerplate/memory_hog ./rootfs-alpha/
```

Start background containers:

```bash
cd boilerplate
sudo ./engine start alpha ../rootfs-alpha /bin/sh --soft-mib 48 --hard-mib 80
sudo ./engine start beta ../rootfs-beta /bin/sh --soft-mib 64 --hard-mib 96
```

Inspect metadata and logs:

```bash
sudo ./engine ps
sudo ./engine logs alpha
```

Run a foreground container and wait for exit:

```bash
sudo ./engine run gamma ../rootfs-alpha "/memory_hog 8 500" --soft-mib 32 --hard-mib 64
echo $?
```

Stop containers:

```bash
sudo ./engine stop alpha
sudo ./engine stop beta
```

Inspect kernel events and unload the module:

```bash
dmesg | tail -n 50
sudo rmmod monitor
```

## 4. CLI Contract

The runtime exposes the required commands:

```bash
engine supervisor <base-rootfs>
engine start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]
engine ps
engine logs <id>
engine stop <id>
```

Implementation notes:

- `start` launches the container under the long-running supervisor and returns after the request is accepted.
- `run` uses the same supervisor path but waits for the container to finish and returns that container's exit status.
- `ps` prints tracked metadata including state, reason, limits, priority, start time, and log path.
- `logs` returns the persistent per-container log file contents managed by the logging pipeline.
- `stop` marks `stop_requested` and sends `SIGTERM`, which lets the supervisor classify manual stop separately from hard-limit kills.

## 5. Design Summary

### User-space runtime

- Control-plane IPC uses a UNIX domain socket at `/tmp/mini_runtime.sock`.
- Each CLI invocation is a short-lived client that sends one structured request to the supervisor.
- The supervisor stores container metadata in a linked list protected by `metadata_lock`.
- Containers are launched with `clone()` using new PID, UTS, and mount namespaces.
- The child sets its hostname, makes mounts private, `chroot`s into its assigned rootfs, mounts `/proc`, redirects output into a pipe, and executes `/bin/sh -c <command>`.

### Logging pipeline

- Each container writes stdout and stderr into a pipe owned by the supervisor.
- A producer thread per container reads from that pipe and pushes log chunks into a bounded shared buffer.
- A dedicated consumer thread drains the buffer and appends the chunks to `logs/<id>.log`.

Why the synchronization matters:

- The bounded buffer uses one mutex plus `not_empty` and `not_full` condition variables.
- Without the mutex, producers and the consumer could race on head/tail/count and corrupt the ring buffer.
- Without `not_full`, a full buffer would force busy-waiting or data loss.
- Without `not_empty`, the consumer would need to spin when no logs are available.
- Shutdown broadcasts on both conditions so producers and the consumer can exit cleanly without deadlock.

### Kernel monitor

- The supervisor registers each container's host PID with `/dev/container_monitor`.
- The kernel module stores monitored entries in a linked list protected by a spinlock because the timer path must not sleep while touching shared list state.
- The timer checks RSS periodically, emits one warning on first soft-limit breach, kills on hard-limit breach, and removes stale entries when a process has already exited.

## 6. Engineering Analysis

### Isolation mechanisms

Namespaces let the container see a different process tree, hostname, and mount table from the host. `CLONE_NEWPID` creates a fresh PID namespace, `CLONE_NEWUTS` isolates the hostname, and `CLONE_NEWNS` isolates mount changes. `chroot()` changes the process-visible filesystem root so the container only sees its assigned rootfs tree. The host kernel is still shared, so all containers use the same scheduler, memory manager, and kernel code paths.

### Supervisor and process lifecycle

A long-running supervisor is useful because it centralizes metadata, logging, signal handling, and reaping across multiple containers. The supervisor owns the control socket, creates children with `clone()`, tracks the host PID for each container, and handles `SIGCHLD` so dead children are reaped promptly. That prevents zombies and keeps lifecycle state available to `ps`, `logs`, and `stop`.

### IPC, threads, and synchronization

This project uses two separate IPC paths. Path A is the log pipe from each container back to the supervisor. Path B is the UNIX socket used by CLI clients to send control requests. Shared metadata uses a separate mutex because metadata operations and log-buffer operations have different critical sections and different correctness concerns. The ring buffer uses condition variables so producers block when full and the consumer blocks when empty, which prevents busy-waiting and lost updates.

### Memory management and enforcement

RSS measures resident physical memory currently mapped into a process address space. It does not directly tell you total virtual memory usage, swapped-out pages, or broader cgroup-style accounting. A soft limit is a warning threshold, useful for observability and tuning. A hard limit is an enforcement threshold, useful when the runtime must protect the system from runaway memory growth. Enforcement belongs in kernel space because the kernel has authoritative access to process memory accounting and can reliably kill the task even when user-space is delayed or the target process is misbehaving.

### Scheduling behavior

This runtime provides a platform for controlled scheduling experiments because it can launch concurrent workloads with different `nice` values while preserving per-container logs and metadata. A CPU-bound pair with different priorities should show different completion rates or progress rates. A CPU-bound workload alongside an I/O-oriented workload should demonstrate Linux's balance between throughput and responsiveness. The exact numbers depend on the VM's core count, load, and timing, so the final report should include the measured outputs from your own runs.

## 7. Design Decisions and Tradeoffs

| Subsystem | Design choice | Tradeoff | Why it was reasonable |
| --- | --- | --- | --- |
| Namespace isolation | `clone()` with PID/UTS/mount namespaces plus `chroot()` | Simpler than `pivot_root()`, but less thorough against path-based escape tricks | Easier to implement and explain within the course scope |
| Supervisor control plane | UNIX domain socket | Requires a persistent socket path and binary request/response format | Straightforward request-response IPC for a daemon plus many short-lived clients |
| Logging | Per-container producer threads plus one consumer thread | More thread management than direct file writes | Clearly demonstrates bounded-buffer synchronization and keeps file I/O out of container children |
| Kernel monitor | Timer-driven periodic RSS checks | Sampling can miss very short spikes between checks | Simple, inspectable enforcement model that is easy to demo with `dmesg` |
| Scheduling experiments | `nice`-based comparison with helper workloads | Results depend on VM conditions and are less controlled than a lab-grade benchmark | Matches the course goal of observing Linux scheduling behavior rather than reimplementing a scheduler |

## 8. Scheduler Experiment Results

Replace this section with your actual VM measurements.

Suggested experiment A:

```bash
sudo ./engine start cpu-low  ../rootfs-alpha "/cpu_hog 15" --nice 10
sudo ./engine start cpu-high ../rootfs-beta  "/cpu_hog 15" --nice 0
sudo ./engine logs cpu-low
sudo ./engine logs cpu-high
```

Suggested experiment B:

```bash
sudo ./engine start cpu ../rootfs-alpha "/cpu_hog 15" --nice 0
sudo ./engine start io  ../rootfs-beta  "/io_pulse 20 200" --nice 0
sudo ./engine logs cpu
sudo ./engine logs io
```

Recommended table format:

| Experiment | Workload(s) | Priority setup | Observation | Interpretation |
| --- | --- | --- | --- | --- |
| A | `cpu_hog` vs `cpu_hog` | `nice 0` vs `nice 10` | `TODO` | `TODO` |
| B | `cpu_hog` vs `io_pulse` | same `nice` | `TODO` | `TODO` |

## 9. Demo Screenshots

Add annotated screenshots with short captions for:

1. Multi-container supervision
2. Metadata tracking with `engine ps`
3. Bounded-buffer logging and captured log files
4. CLI request plus supervisor response over the control IPC path
5. Soft-limit warning in `dmesg`
6. Hard-limit enforcement in `dmesg` and supervisor metadata
7. Scheduling experiment output
8. Clean teardown with no zombies

## 10. CI-safe Build Path

The inherited GitHub Actions smoke check expects:

```bash
make -C boilerplate ci
```

That target only builds the user-space binaries and avoids kernel-module loading or privileged runtime setup.

## 11. What Still Requires VM Validation

This repository can be compiled in CI for the user-space path, but the full deliverable still needs real Linux VM validation for:

- namespace creation and container launch under `sudo`
- `/proc` mounting inside the container
- module insertion and `/dev/container_monitor` creation
- `dmesg` evidence for soft and hard memory events
- scheduling experiment measurements
- screenshot capture for the final report
