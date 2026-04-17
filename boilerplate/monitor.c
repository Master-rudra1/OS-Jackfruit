#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

struct monitored_entry {
    pid_t pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    bool soft_limit_logged;
    char container_id[MONITOR_NAME_LEN];
    struct list_head list;
};

static LIST_HEAD(monitored_processes);
static DEFINE_SPINLOCK(monitored_lock);

static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry;
    struct monitored_entry *tmp;
    LIST_HEAD(to_free);
    unsigned long flags;

    (void)t;

    spin_lock_irqsave(&monitored_lock, flags);
    list_for_each_entry_safe(entry, tmp, &monitored_processes, list) {
        long rss_bytes = get_rss_bytes(entry->pid);

        if (rss_bytes < 0) {
            list_del_init(&entry->list);
            list_add_tail(&entry->list, &to_free);
            continue;
        }

        if (!entry->soft_limit_logged && rss_bytes > (long)entry->soft_limit_bytes) {
            entry->soft_limit_logged = true;
            log_soft_limit_event(entry->container_id,
                                 entry->pid,
                                 entry->soft_limit_bytes,
                                 rss_bytes);
        }

        if (rss_bytes > (long)entry->hard_limit_bytes) {
            kill_process(entry->container_id,
                         entry->pid,
                         entry->hard_limit_bytes,
                         rss_bytes);
            list_del_init(&entry->list);
            list_add_tail(&entry->list, &to_free);
        }
    }
    spin_unlock_irqrestore(&monitored_lock, flags);

    list_for_each_entry_safe(entry, tmp, &to_free, list) {
        list_del_init(&entry->list);
        kfree(entry);
    }

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        struct monitored_entry *entry;
        unsigned long flags;

        if (req.pid <= 0 || req.soft_limit_bytes == 0 || req.hard_limit_bytes == 0)
            return -EINVAL;
        if (req.soft_limit_bytes > req.hard_limit_bytes)
            return -EINVAL;

        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_limit_logged = false;
        strscpy(entry->container_id, req.container_id, sizeof(entry->container_id));
        INIT_LIST_HEAD(&entry->list);

        spin_lock_irqsave(&monitored_lock, flags);
        {
            struct monitored_entry *cursor;
            list_for_each_entry(cursor, &monitored_processes, list) {
                if (cursor->pid == req.pid ||
                    strncmp(cursor->container_id,
                            req.container_id,
                            sizeof(cursor->container_id)) == 0) {
                    spin_unlock_irqrestore(&monitored_lock, flags);
                    kfree(entry);
                    return -EEXIST;
                }
            }
            list_add_tail(&entry->list, &monitored_processes);
        }
        spin_unlock_irqrestore(&monitored_lock, flags);

        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);
        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    {
        struct monitored_entry *entry;
        struct monitored_entry *tmp;
        unsigned long flags;

        spin_lock_irqsave(&monitored_lock, flags);
        list_for_each_entry_safe(entry, tmp, &monitored_processes, list) {
            if (entry->pid == req.pid ||
                strncmp(entry->container_id,
                        req.container_id,
                        sizeof(entry->container_id)) == 0) {
                list_del_init(&entry->list);
                spin_unlock_irqrestore(&monitored_lock, flags);
                kfree(entry);
                return 0;
            }
        }
        spin_unlock_irqrestore(&monitored_lock, flags);
    }

    return -ENOENT;
}

static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

static void __exit monitor_exit(void)
{
    struct monitored_entry *entry;
    struct monitored_entry *tmp;
    LIST_HEAD(to_free);
    unsigned long flags;

    del_timer_sync(&monitor_timer);

    spin_lock_irqsave(&monitored_lock, flags);
    list_for_each_entry_safe(entry, tmp, &monitored_processes, list) {
        list_del_init(&entry->list);
        list_add_tail(&entry->list, &to_free);
    }
    spin_unlock_irqrestore(&monitored_lock, flags);

    list_for_each_entry_safe(entry, tmp, &to_free, list) {
        list_del_init(&entry->list);
        kfree(entry);
    }

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
