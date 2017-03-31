/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gradle.internal.resources;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradle.api.Transformer;
import org.gradle.api.specs.Spec;
import org.gradle.internal.Pair;
import org.gradle.internal.UncheckedException;
import org.gradle.util.CollectionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultResourceLockCoordinationService implements ResourceLockCoordinationService {
    private final Object lock = new Object();
    private final ThreadLocal<List<ResourceLockState>> currentState = new ThreadLocal<List<ResourceLockState>>() {
        @Override
        protected List<ResourceLockState> initialValue() {
            return Lists.newArrayList();
        }
    };
    private final Map<Thread, DefaultResourceLockState> waiting = Maps.newHashMap();
    private final Set<ResourceLockRegistry> lockRegistries = Sets.newConcurrentHashSet();

    @Override
    public boolean withStateLock(Transformer<ResourceLockState.Disposition, ResourceLockState> resourceLockAction) {
        while (true) {
            DefaultResourceLockState resourceLockState = new DefaultResourceLockState();
            ResourceLockState.Disposition disposition;
            synchronized (lock) {
                try {
                    currentState.get().add(resourceLockState);
                    disposition = resourceLockAction.transform(resourceLockState);

                    switch (disposition) {
                        case RETRY:
                            releaseLocks(resourceLockState);
                            checkForDeadlocks(resourceLockState);
                            waitForLockStateChange(resourceLockState);
                            break;
                        case FINISHED:
                            return true;
                        case FAILED:
                            releaseLocks(resourceLockState);
                            return false;
                        default:
                            throw new IllegalArgumentException("Unhandled disposition type: " + disposition.name());
                    }
                } catch (Throwable t) {
                    releaseLocks(resourceLockState);
                    throw UncheckedException.throwAsUncheckedException(t);
                } finally {
                    currentState.get().remove(resourceLockState);
                }
            }
        }
    }

    @Override
    public ResourceLockState getCurrent() {
        if (!currentState.get().isEmpty()) {
            int numStates = currentState.get().size();
            return currentState.get().get(numStates - 1);
        } else {
            return null;
        }
    }

    private void waitForLockStateChange(DefaultResourceLockState resourceLockState) {
        waiting.put(Thread.currentThread(), resourceLockState);
        try {
            lock.wait();
        } catch (InterruptedException e) {
            throw UncheckedException.throwAsUncheckedException(e);
        } finally {
            waiting.remove(Thread.currentThread());
        }
    }

    @Override
    public void notifyStateChange() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    @Override
    public void addRegistry(ResourceLockRegistry registry) {
        lockRegistries.add(registry);
    }

    private void releaseLocks(DefaultResourceLockState stateLock) {
        for (ResourceLock resourceLock : stateLock.getResourceLocks()) {
            resourceLock.unlock();
        }
    }

    private void checkForDeadlocks(DefaultResourceLockState resourceLockState) {
        Set<ResourceLock> locksIHold = getResourceLocksByThread(Thread.currentThread());
        Map<ResourceLock, Boolean> alreadyChecked = Maps.newHashMap();
        Set<Pair<Thread, Iterable<ResourceLock>>> cycle = Sets.newLinkedHashSet();
        for (ResourceLock failedLock : resourceLockState.getResourceLockFailures()) {
            if (checkForCycle(cycle, locksIHold, failedLock, alreadyChecked)) {
                cycle.add(Pair.of(Thread.currentThread(), getHolderLocks(failedLock)));
                List<Pair<Thread, Iterable<ResourceLock>>> cycleList = Lists.newArrayList(cycle);
                Collections.reverse(cycleList);
                throw new ResourceDeadlockException(cycleList);
            }
        }
    }

    private boolean checkForCycle(final Set<Pair<Thread, Iterable<ResourceLock>>> cycle, final Set<ResourceLock> locksIHold, final ResourceLock failedLock, Map<ResourceLock, Boolean> alreadyChecked) {
        if (alreadyChecked.containsKey(failedLock)) {
            return alreadyChecked.get(failedLock);
        }

        boolean cycleExists = false;
        if (failedLock instanceof LeaseHolder) {
            cycleExists = checkForLeaseHolderCycle(cycle, locksIHold, failedLock, alreadyChecked);
        } else if (waiting.containsKey(failedLock.getOwner())) {
            DefaultResourceLockState ownerLockState = waiting.get(failedLock.getOwner());
            for (ResourceLock ownerFailedLock : ownerLockState.getResourceLockFailures()) {
                cycleExists = hasCycle(cycle, locksIHold, failedLock.getOwner(), ownerFailedLock, alreadyChecked);
                if (cycleExists) {
                    break;
                }
            }
        }

        alreadyChecked.put(failedLock, cycleExists);
        return cycleExists;
    }

    private boolean checkForLeaseHolderCycle(final Set<Pair<Thread, Iterable<ResourceLock>>> cycle, final Set<ResourceLock> locksIHold, final ResourceLock failedLock, final Map<ResourceLock, Boolean> alreadyChecked) {
        final LeaseHolder root = ((LeaseHolder) failedLock).getRoot();

        // find all of the waiting threads that hold leases for the same root
        Map<Thread, DefaultResourceLockState> waitersWithLeases = getWaitersWithLeaseRoot(root, locksIHold);

        // If all of the leases are being held by waiting threads
        if (waitersWithLeases.size() >= root.getMaxLeases()) {
            // Check to see if all of the waiters that hold leases are waiting on a lock I hold
            return CollectionUtils.every(waitersWithLeases.entrySet(), new Spec<Map.Entry<Thread, DefaultResourceLockState>>() {
                @Override
                public boolean isSatisfiedBy(final Map.Entry<Thread, DefaultResourceLockState> waiter) {
                    return CollectionUtils.any(waiter.getValue().getResourceLockFailures(), new Spec<ResourceLock>() {
                        @Override
                        public boolean isSatisfiedBy(ResourceLock waiterFailedLock) {
                            return hasCycle(cycle, locksIHold, waiter.getKey(), waiterFailedLock, alreadyChecked);
                        }
                    });
                }
            });
        } else {
            return false;
        }
    }

    private boolean hasCycle(final Set<Pair<Thread, Iterable<ResourceLock>>> cycle, final Set<ResourceLock> locksIHold, Thread thread, final ResourceLock failedLock, Map<ResourceLock, Boolean> alreadyChecked) {
        boolean leaseHolderCycle = failedLock instanceof LeaseHolder && containsLeaseRoot(locksIHold, ((LeaseHolder)failedLock).getRoot());

        if (leaseHolderCycle || locksIHold.contains(failedLock) || checkForCycle(cycle, locksIHold, failedLock, alreadyChecked)) {
            cycle.add(Pair.of(thread, getHolderLocks(failedLock)));
            return true;
        } else {
            return false;
        }
    }

    private Map<Thread, DefaultResourceLockState> getWaitersWithLeaseRoot(final LeaseHolder root, Set<ResourceLock> locksIHold) {
        Map<Thread, DefaultResourceLockState> waitersWithLeases = CollectionUtils.filter(waiting, new Spec<Map.Entry<Thread, DefaultResourceLockState>>() {
            @Override
            public boolean isSatisfiedBy(final Map.Entry<Thread, DefaultResourceLockState> waiter) {
                Set<ResourceLock> waiterLocks = getResourceLocksByThread(waiter.getKey());
                return containsLeaseRoot(waiterLocks, root);
            }
        });

        if (containsLeaseRoot(locksIHold, root)) {
            waitersWithLeases.put(Thread.currentThread(), (DefaultResourceLockState)getCurrent());
        }

        return waitersWithLeases;
    }

    private boolean containsLeaseRoot(Set<ResourceLock> locks, final LeaseHolder root) {
        return CollectionUtils.any(locks, new Spec<ResourceLock>() {
            @Override
            public boolean isSatisfiedBy(ResourceLock resourceLock) {
                return resourceLock instanceof LeaseHolder && ((LeaseHolder)resourceLock).getRoot() == root;
            }
        });
    }

    private Iterable<ResourceLock> getHolderLocks(ResourceLock resourceLock) {
        if (resourceLock instanceof LeaseHolder) {
            Set<ResourceLock> holderLocks = Sets.newHashSet();
            final LeaseHolder root = ((LeaseHolder) resourceLock).getRoot();
            Set<Thread> threads = Sets.newHashSet(waiting.keySet());
            threads.add(Thread.currentThread());
            for (Thread waitingThread : threads) {
                for (ResourceLock lock : getResourceLocksByThread(waitingThread)) {
                    if (lock instanceof LeaseHolder && ((LeaseHolder)lock).getRoot() == root) {
                        holderLocks.add(lock);
                    }
                }
            }
            return holderLocks;
        } else {
            return Lists.newArrayList(resourceLock);
        }
    }

    private Set<ResourceLock> getResourceLocksByThread(Thread thread) {
        Set<ResourceLock> resourceLocks = Sets.newHashSet();
        for (ResourceLockRegistry registry : lockRegistries) {
            resourceLocks.addAll(registry.getResourceLocksByThread(thread));
        }
        return resourceLocks;
    }

    private static class DefaultResourceLockState implements ResourceLockState {
        private final Set<ResourceLock> resourceLocks = Sets.newHashSet();
        private final Set<ResourceLock> resourceLockFailures = Sets.newHashSet();

        @Override
        public void registerLocked(ResourceLock resourceLock) {
            resourceLocks.add(resourceLock);
        }

        @Override
        public void registerFailed(ResourceLock resourceLock) {
            resourceLockFailures.add(resourceLock);
        }

        Set<ResourceLock> getResourceLocks() {
            return resourceLocks;
        }

        Set<ResourceLock> getResourceLockFailures() {
            return resourceLockFailures;
        }
    }

    /**
     * Attempts an atomic, blocking lock on the provided resource locks.
     */
    public static Transformer<ResourceLockState.Disposition, ResourceLockState> lock(Collection<? extends ResourceLock> resourceLocks) {
        return new AcquireLocks(resourceLocks, true);
    }

    /**
     * Attempts an atomic, blocking lock on the provided resource locks.
     */
    public static Transformer<ResourceLockState.Disposition, ResourceLockState> lock(ResourceLock... resourceLocks) {
        return lock(Arrays.asList(resourceLocks));
    }

    /**
     * Attempts an atomic, non-blocking lock on the provided resource locks.
     */
    public static Transformer<ResourceLockState.Disposition, ResourceLockState> tryLock(Collection<? extends ResourceLock> resourceLocks) {
        return new AcquireLocks(resourceLocks, false);
    }

    /**
     * Attempts an atomic, non-blocking lock on the provided resource locks.
     */
    public static Transformer<ResourceLockState.Disposition, ResourceLockState> tryLock(ResourceLock... resourceLocks) {
        return tryLock(Arrays.asList(resourceLocks));
    }

    /**
     * Unlocks the provided resource locks.
     */
    public static Transformer<ResourceLockState.Disposition, ResourceLockState> unlock(Collection<? extends ResourceLock> resourceLocks) {
        return new ReleaseLocks(resourceLocks);
    }

    /**
     * Unlocks the provided resource locks.
     */
    public static Transformer<ResourceLockState.Disposition, ResourceLockState> unlock(ResourceLock... resourceLocks) {
        return unlock(Arrays.asList(resourceLocks));
    }

    private static class AcquireLocks implements Transformer<ResourceLockState.Disposition, ResourceLockState> {
        private final Iterable<? extends ResourceLock> resourceLocks;
        private final boolean blocking;

        AcquireLocks(Iterable<? extends ResourceLock> resourceLocks, boolean blocking) {
            this.resourceLocks = resourceLocks;
            this.blocking = blocking;
        }

        @Override
        public ResourceLockState.Disposition transform(ResourceLockState resourceLockState) {
            for (ResourceLock resourceLock : resourceLocks) {
                if (!resourceLock.tryLock()) {
                    return blocking ? ResourceLockState.Disposition.RETRY : ResourceLockState.Disposition.FAILED;
                }
            }
            return ResourceLockState.Disposition.FINISHED;
        }
    }

    private static class ReleaseLocks implements Transformer<ResourceLockState.Disposition, ResourceLockState> {
        private final Iterable<? extends ResourceLock> resourceLocks;

        ReleaseLocks(Iterable<? extends ResourceLock> resourceLocks) {
            this.resourceLocks = resourceLocks;
        }

        @Override
        public ResourceLockState.Disposition transform(ResourceLockState resourceLockState) {
            for (ResourceLock resourceLock : resourceLocks) {
                resourceLock.unlock();
            }
            return ResourceLockState.Disposition.FINISHED;
        }
    }
}
