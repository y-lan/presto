/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.util;

import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Guarantees that no more than maxPermits of tasks will be run concurrently.
 * The class will rely on the ListenableFuture returned by the submitter function to determine
 * when a task has been completed. The submitter function NEEDS to be thread-safe and is recommended
 * to do the bulk of its work asynchronously.
 */
@ThreadSafe
public class AsyncSemaphore<T>
{
    private final Queue<QueuedTask<T>> queuedTasks = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, Queue<QueuedTask<T>>> queuedKeyTasks = new ConcurrentHashMap<>();

    private final AtomicInteger counter = new AtomicInteger();
    public final ConcurrentMap<String, AtomicInteger> keyCounter = new ConcurrentHashMap<>();

    private final int maxPermits;
    private final int maxPermitsPerKey;
    private final Executor submitExecutor;
    private final Function<T, ListenableFuture<?>> submitter;

    public AsyncSemaphore(int maxPermits, int maxPermitsPerKey, Executor submitExecutor, Function<T, ListenableFuture<?>> submitter)
    {
        checkArgument(maxPermits > 0, "must have at least one permit");
        checkArgument(maxPermitsPerKey > 0, "must have at least one permit for each key");
        this.maxPermits = maxPermits;
        this.maxPermitsPerKey = maxPermitsPerKey;
        this.submitExecutor = checkNotNull(submitExecutor, "submitExecutor is null");
        this.submitter = checkNotNull(submitter, "submitter is null");
    }

    public ListenableFuture<?> submit(T task, String key)
    {
        QueuedTask<T> queuedTask = new QueuedTask<>(task, key);
        queuedTasks.add(queuedTask);
        acquirePermit();
        return queuedTask.getCompletionFuture();
    }

    private void acquirePermit()
    {
        if (counter.incrementAndGet() <= maxPermits) {
            promoteOneToKeyTasks();
        }
    }

    private void promoteOneToKeyTasks()
    {
        QueuedTask<T> task = queuedTasks.poll();
        String key = task.getKey();
        ConcurrentLinkedQueue<QueuedTask<T>> queue = new ConcurrentLinkedQueue<>();
        queue.add(task);
        Queue<QueuedTask<T>> oldQueue = queuedKeyTasks.putIfAbsent(key, queue);
        if (oldQueue != null) {
            oldQueue.add(task);
        }
        acquireKeyPermit(key);
    }

    private void acquireKeyPermit(String key)
    {
        AtomicInteger newCount = new AtomicInteger(1);
        AtomicInteger oldCount = keyCounter.putIfAbsent(key, newCount);
        if (!((oldCount != null) && oldCount.incrementAndGet() > maxPermitsPerKey)) {
            submitExecutor.execute(new RunNextTask(key));
        }
    }

    private void releasePermit(String key)
    {
        if (keyCounter.get(key).getAndDecrement() > maxPermitsPerKey) {
            // Now that a key-slot is empty, can kick off another task for that key
            submitExecutor.execute(new RunNextTask(key));
        }
        if (counter.getAndDecrement() > maxPermits) {
            // Now that a task has finished, we can let another task in
            promoteOneToKeyTasks();
        }
    }

    private void runNext(String key)
    {
        final QueuedTask<T> queuedTask = queuedKeyTasks.get(key).poll();
        final String releaseKey = key;
        ListenableFuture<?> future = submitTask(queuedTask.getTask());
        Futures.addCallback(future, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(Object result)
            {
                queuedTask.markCompleted();
                releasePermit(releaseKey);
            }

            @Override
            public void onFailure(Throwable t)
            {
                queuedTask.markFailure(t);
                releasePermit(releaseKey);
            }
        });
    }

    private ListenableFuture<?> submitTask(T task)
    {
        try {
            ListenableFuture<?> future = submitter.apply(task);
            if (future == null) {
                return Futures.immediateFailedFuture(new NullPointerException("Submitter returned a null future for task: " + task));
            }
            return future;
        }
        catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
    }

    private class RunNextTask implements Runnable
    {
        private String key;

        RunNextTask(String key)
        {
            this.key = key;
        }

        @Override
        public void run()
        {
            runNext(key);
        }
    }

    private static class QueuedTask<T>
    {
        private final T task;
        private final String key;
        private final SettableFuture<?> settableFuture = SettableFuture.create();

        private QueuedTask(T task, String key)
        {
            this.task = checkNotNull(task, "task is null");
            this.key = checkNotNull(key, "key is null");
        }

        public T getTask()
        {
            return task;
        }

        public String getKey()
        {
            return key;
        }

        public void markFailure(Throwable throwable)
        {
            settableFuture.setException(throwable);
        }

        public void markCompleted()
        {
            settableFuture.set(null);
        }

        public ListenableFuture<?> getCompletionFuture()
        {
            return settableFuture;
        }
    }
}
