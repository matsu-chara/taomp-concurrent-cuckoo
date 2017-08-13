package chara.matsu.taomp.hash.closed.cuckoo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * from The Art of Multiprocessor Programming 13.4
 *
 * lock striping implementation of PhaseCuckooHashSet
 */
public class StripedCuckooHashSet<T> extends PhasedCuckooHashSet<T> {
    final ReentrantLock[][] lock;

    public StripedCuckooHashSet(int capacity) {
        super(capacity);
        lock = new ReentrantLock[NUM_ARRAY][capacity];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < capacity; j++) {
                lock[i][j] = new ReentrantLock();
            }
        }
    }

    @Override protected void acquire(T x) {
        lock[0][hash0(x) % lock[0].length].lock();
        lock[1][hash1(x) % lock[1].length].lock();
    }

    @Override protected void release(T x) {
        lock[0][hash0(x) % lock[0].length].unlock();
        lock[1][hash1(x) % lock[1].length].unlock();
    }

    @Override protected void resize() {
        int oldCapacity = capacity;
        for (Lock aLock : lock[0]) {
            aLock.lock();
        }

        try {
            if (capacity != oldCapacity) {
                return;
            }
            List<T>[][] oldTable = table;
            capacity = 2 * capacity;

            table = (List<T>[][]) new List[NUM_ARRAY][capacity];
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
            for (Lock aLock : lock[0]) {
                aLock.unlock();
            }
        }
    }
}
