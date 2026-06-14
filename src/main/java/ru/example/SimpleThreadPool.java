package ru.example;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleThreadPool implements CustomExecutor {
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveMillis;
    private final int queueSize;
    private final int minSpareThreads;
    private final List<Worker> workers = new ArrayList<>();
    private final AtomicInteger rr = new AtomicInteger();
    private final ThreadFactory threadFactory = new MyThreadFactory();
    private volatile boolean shutdown;

    public SimpleThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveMillis = timeUnit.toMillis(keepAliveTime);
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;

        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException();
        }
        if (shutdown) {
            reject(command);
            return;
        }

        maybeAddWorker();

        Worker worker = chooseWorker();
        boolean ok = worker.queue.offer(command);

        if (ok) {
            System.out.println("[Pool] Task accepted into queue #" + worker.id + ": " + command);
            return;
        }

        synchronized (workers) {
            if (workers.size() < maxPoolSize) {
                worker = addWorker();
                ok = worker.queue.offer(command);
            }
        }

        if (ok) {
            System.out.println("[Pool] Task accepted into queue #" + worker.id + ": " + command);
        } else {
            reject(command);
        }
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }

    @Override
    public void shutdown() {
        System.out.println("[Pool] shutdown called");
        shutdown = true;
    }

    @Override
    public void shutdownNow() {
        System.out.println("[Pool] shutdownNow called");
        shutdown = true;
        synchronized (workers) {
            for (Worker worker : workers) {
                worker.thread.interrupt();
                worker.queue.clear();
            }
        }
    }

    private Worker chooseWorker() {
        synchronized (workers) {
            int index = Math.abs(rr.getAndIncrement()) % workers.size();
            return workers.get(index);
        }
    }

    private void maybeAddWorker() {
        synchronized (workers) {
            int free = 0;
            for (Worker worker : workers) {
                if (worker.queue.isEmpty() && !worker.busy) {
                    free++;
                }
            }
            if (free < minSpareThreads && workers.size() < maxPoolSize) {
                addWorker();
            }
        }
    }

    private Worker addWorker() {
        int id = workers.size() + 1;
        Worker worker = new Worker(id);
        Thread thread = threadFactory.newThread(worker);
        worker.thread = thread;
        workers.add(worker);
        thread.start();
        return worker;
    }

    private void reject(Runnable command) {
        System.out.println("[Rejected] Task " + command + " was rejected due to overload!");
        throw new RejectedExecutionException("Too many tasks");
    }

    private class Worker implements Runnable {
        private final int id;
        private final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(queueSize);
        private Thread thread;
        private volatile boolean busy;

        Worker(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    if (shutdown && queue.isEmpty()) {
                        break;
                    }

                    Runnable task = queue.poll(keepAliveMillis, TimeUnit.MILLISECONDS);

                    if (task == null) {
                        synchronized (workers) {
                            if (workers.size() > corePoolSize) {
                                System.out.println("[Worker] " + thread.getName() + " idle timeout, stopping.");
                                workers.remove(this);
                                break;
                            }
                        }
                        continue;
                    }

                    if (shutdown) {
                        break;
                    }

                    busy = true;
                    System.out.println("[Worker] " + thread.getName() + " executes " + task);
                    try {
                        task.run();
                    } catch (Exception e) {
                        System.out.println("[Worker] task error: " + e.getMessage());
                    } finally {
                        busy = false;
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("[Worker] " + thread.getName() + " interrupted.");
            }

            System.out.println("[Worker] " + thread.getName() + " terminated.");
        }
    }

    private static class MyThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            String name = "MyPool-worker-" + counter.getAndIncrement();
            System.out.println("[ThreadFactory] Creating new thread: " + name);
            return new Thread(r, name);
        }
    }
}
