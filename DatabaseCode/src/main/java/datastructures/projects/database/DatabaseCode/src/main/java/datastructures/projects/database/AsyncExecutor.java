package datastructures.projects.database;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class AsyncExecutor extends ThreadPoolExecutor implements Executor
{
    public static Executor create()
    {
        return new AsyncExecutor();
    }

    public AsyncExecutor()
    {
        super(5, 10, 10000, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(12));
    }

    @Override
    public void execute(Runnable process) {
        System.out.println("New Process");
        System.out.println(this.getQueue());

        Thread newProcess = new Thread(process);
        newProcess.setDaemon(false);

        newProcess.start();

        System.out.println("Thread created");
    }
}