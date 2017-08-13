package concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * from The Art of Multiprocessor Programming 13.4
 */
public class RefinableCuckooHashSet<T> extends PhasedCuckooHashSet<T> {
    private          AtomicMarkableReference<Thread> owner;
    private volatile ReentrantLock[][]               locks;

    public RefinableCuckooHashSet(int capacity) {
        super(capacity);
        locks = new ReentrantLock[NUM_ARRAY][capacity];
        for (int i = 0; i < NUM_ARRAY; i++) {
            for (int j = 0; j < capacity; j++) {
                locks[i][j] = new ReentrantLock();
            }
        }
        owner = new AtomicMarkableReference<>(null, false);
    }

    @Override protected void acquire(T x) {
        boolean[] mark = {true};
        Thread    me   = Thread.currentThread();
        Thread    who;

        while (true) {
            do {
                who = owner.get(mark);
            } while (mark[0] && who != me);
            ReentrantLock[][] oldLocks = locks;
            ReentrantLock     oldLock0 = oldLocks[0][hash0(x) % oldLocks[0].length];
            ReentrantLock     oldLock1 = oldLocks[1][hash1(x) % oldLocks[1].length];
            oldLock0.lock();
            oldLock1.lock();
            who = owner.get(mark);
            if ((!mark[0] || who == me) && locks == oldLocks) {
                return;
            } else {
                oldLock0.unlock();
                oldLock1.unlock();
            }
        }
    }

    @Override protected void release(T x) {
        locks[0][hash0(x)].unlock();
        locks[1][hash1(x)].unlock();
    }

    @Override protected void resize() {
        int    oldCapacity = capacity;
        Thread me          = Thread.currentThread();
        if (owner.compareAndSet(null, me, false, true)) {
            try {
                if (capacity != oldCapacity) {
                    return;
                }
                quiesce();
                capacity = 2 * capacity;
                List<T>[][] oldTable = table;
                table = (List<T>[][]) new List[NUM_ARRAY][capacity];
                locks = new ReentrantLock[NUM_ARRAY][capacity];

                for (int i = 0; i < NUM_ARRAY; i++) {
                    for (int j = 0; j < capacity; j++) {
                        locks[i][j] = new ReentrantLock();
                    }
                }
                for (List<T>[] row : table) {
                    for (int i = 0; i < row.length; i++) {
                        row[i] = new ArrayList<T>(PROBE_SIZE);
                    }
                }
                for (List<T>[] row : oldTable) {
                    for (List<T> set : row) {
                        for (T z : set) {
                            add(z);
                        }
                    }
                }
            } finally {
                owner.set(null, false);
            }
        }
    }

    protected void quiesce() {
        for (ReentrantLock lock : locks[0]) {
            while (lock.isLocked()) {
            }
        }
    }
}
