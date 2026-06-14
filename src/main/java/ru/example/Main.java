package ru.example;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        CustomExecutor pool = new SimpleThreadPool(2, 4, 3, TimeUnit.SECONDS, 2, 1);

        for (int i = 1; i <= 10; i++) {
            try {
                pool.execute(new DemoTask(i, 1500));
            } catch (Exception e) {
                System.out.println("Main caught reject for task " + i);
            }
        }

        Future<String> result = pool.submit(() -> {
            Thread.sleep(1000);
            return "result from callable";
        });

        System.out.println("Callable result: " + result.get());

        Thread.sleep(7000);

        pool.shutdown();

        try {
            pool.execute(new DemoTask(999, 1000));
        } catch (Exception e) {
            System.out.println("Task after shutdown rejected");
        }
    }

    static class DemoTask implements Runnable {
        private final int id;
        private final long sleep;

        DemoTask(int id, long sleep) {
            this.id = id;
            this.sleep = sleep;
        }

        @Override
        public void run() {
            System.out.println("Task " + id + " started");
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                System.out.println("Task " + id + " interrupted");
            }
            System.out.println("Task " + id + " finished");
        }

        @Override
        public String toString() {
            return "DemoTask-" + id;
        }
    }
}
