# Multi-Container Runtime

## 1. Introduction

This project is a lightweight container runtime implemented in C as part of an Operating Systems course. The aim of this project is to understand how container systems work internally by building a simplified version from scratch.

The runtime demonstrates core OS concepts such as:

* Process isolation using namespaces
* Memory monitoring and enforcement in kernel space
* Inter-process communication (IPC)
* Scheduling behavior using priority control
* Synchronization using producer–consumer patterns

---

## 2. Student Information

**Name:** Rudra Dhadhal
**Roll Number:** PES1UG24CS146

**Name:** Dhruv Swatantramath
**Roll Number:** PES1UG24CS155

---

## 3. Features

* Run multiple containers under a single supervisor
* Isolate containers using Linux namespaces
* Enforce memory limits using a custom kernel module
* Capture logs using a bounded-buffer logging system
* Perform scheduling experiments using `nice` values

---

## 4. System Requirements

* Ubuntu 22.04 / 24.04 (VM recommended)
* Root access (required for namespaces and kernel module)
* Secure Boot disabled

---

## 5. Build and Setup

### Install Dependencies

```bash
sudo apt update
sudo apt install -y build-essential linux-headers-$(uname -r) wget
```

### Build Project

```bash
make -C boilerplate ci
make -C boilerplate
```

### Optional Environment Check

```bash
cd boilerplate
chmod +x environment-check.sh
sudo ./environment-check.sh
cd ..
```

---

## 6. Preparing Root Filesystem

```bash
mkdir rootfs-base
wget https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/x86_64/alpine-minirootfs-3.20.3-x86_64.tar.gz
tar -xzf alpine-minirootfs-3.20.3-x86_64.tar.gz -C rootfs-base
```

Copy helper workloads:

```bash
cp boilerplate/cpu_hog rootfs-base/
cp boilerplate/io_pulse rootfs-base/
cp boilerplate/memory_hog rootfs-base/
```

Create container copies:

```bash
cp -a rootfs-base rootfs-alpha
cp -a rootfs-base rootfs-beta
```

---

## 7. Running the Runtime

### Load Kernel Module

```bash
sudo insmod boilerplate/monitor.ko
ls -l /dev/container_monitor
```

### Start Supervisor

```bash
sudo ./boilerplate/engine supervisor ./rootfs-base
```

The supervisor runs continuously and handles all container requests.

---

## 8. Container Commands

### Start Containers

```bash
sudo ./boilerplate/engine start alpha ./rootfs-alpha "/cpu_hog 120" --soft-mib 48 --hard-mib 128 --nice 0
sudo ./boilerplate/engine start beta ./rootfs-beta "/io_pulse 300 200" --soft-mib 48 --hard-mib 128 --nice 0
```

### View Containers

```bash
sudo ./boilerplate/engine ps
```

### View Logs

```bash
sudo ./boilerplate/engine logs alpha
```

### Stop Container

```bash
sudo ./boilerplate/engine stop alpha
```

---

## 9. How the System Works

### Container Isolation

Each container is created using Linux namespaces (PID, UTS, Mount). This ensures that containers have:

* Separate process trees
* Independent hostnames
* Isolated filesystem views using `chroot()`

### Supervisor Design

A single long-running supervisor process manages all containers. It:

* Receives CLI requests
* Tracks container metadata
* Handles process cleanup
* Prevents zombie processes

### IPC Mechanisms

* UNIX domain socket → communication between CLI and supervisor
* Pipes → container output sent to supervisor

### Logging System

Each container sends output to the supervisor via pipes. Logs are processed using:

* Producer threads (per container)
* Shared bounded buffer
* Consumer thread writing logs to files

### Memory Monitoring

A kernel module tracks memory usage (RSS):

* Soft limit → warning logged in `dmesg`
* Hard limit → container is terminated

### Scheduling

Containers can be assigned priorities using `nice`, allowing observation of CPU scheduling behavior.

---

## 10. Scheduling Experiment

Two CPU-bound containers were run with different priorities:

| Container | Nice Value | Time Taken |
| --------- | ---------- | ---------- |
| hi5       | 0          | 19.718 sec |
| lo5       | 10         | 19.761 sec |

### Observation

The higher-priority container (nice 0) completed slightly faster than the lower-priority one. The difference is small due to short execution time and identical workloads.

---

## 11. Design Decisions

* Used `clone()` with namespaces for simplicity
* Used `chroot()` instead of `pivot_root()` to reduce complexity
* Implemented supervisor architecture for centralized control
* Used bounded-buffer logging to demonstrate synchronization
* Performed memory enforcement in kernel space for reliability

---

## 12. Limitations

* `chroot()` provides limited filesystem isolation
* Memory monitoring is periodic (not real-time)
* Scheduling results depend on system conditions

---

## 13. Cleanup

```bash
sudo rmmod monitor
```

Verify no leftover processes:

```bash
ps -ef | grep "[d]efunct" || echo "no defunct processes"
```

---

## 14. Conclusion

This project demonstrates how fundamental Operating Systems concepts can be combined to build a basic container runtime. It provides a deeper understanding of process isolation, memory control, scheduling, and system design.

The focus of this implementation is on clarity and learning rather than production-level completeness.

---

## 15. Reflection (Optional but Recommended)

During this project, key challenges included understanding namespace behavior, debugging kernel module interactions, and implementing correct synchronization in the logging system. Overcoming these helped build a strong understanding of both user-space and kernel-space programming.
