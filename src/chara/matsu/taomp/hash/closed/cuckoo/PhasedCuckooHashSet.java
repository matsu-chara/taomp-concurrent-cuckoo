package chara.matsu.taomp.hash.closed.cuckoo;

import java.util.ArrayList;
import java.util.List;

/**
 * from The Art of Multiprocessor Programming 13.4
 *
 * abstract class for chara.matsu.concurrent cuckoo
 */
public abstract class PhasedCuckooHashSet<T> {
    protected volatile int         capacity;
    protected volatile List<T>[][] table;

    /**
     * arrays (simple cuckoo hashing use 2 arrays)
     */
    static int NUM_ARRAY = 2;

    /**
     * max size of list. if exceeds, it try to resize
     */
    static int PROBE_SIZE = 4;

    /**
     * threshold to relocate
     */
    static int THRESHOLD = 2;

    /**
     * max relocate count
     */
    static int LIMIT = 100;

    public PhasedCuckooHashSet(int size) {
        capacity = size;
        table = (List<T>[][]) new ArrayList[NUM_ARRAY][capacity];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < capacity; j++) {
                table[i][j] = new ArrayList<T>(PROBE_SIZE);
            }
        }
    }

    public boolean remove(T x) {
        acquire(x);
        try {
            List<T> set0 = table[0][hash0(x) % capacity];
            if (set0.contains(x)) {
                set0.remove(x);
                return true;
            }

            List<T> set1 = table[0][hash1(x) % capacity];
            if (set1.contains(x)) {
                set1.remove(x);
                return true;
            }
            return false;
        } finally {
            release(x);
        }
    }

    public boolean add(T x) {
        T y = null;
        acquire(x);
        int     h0         = hash0(x) % capacity;
        int     h1         = hash1(x) % capacity;
        int     i          = -1;
        int     h          = -1;
        boolean mustResize = false;

        try {
            if (present(x)) return false;
            List<T> set0 = table[0][h0];
            List<T> set1 = table[1][h1];
            if (set0.size() < THRESHOLD) {
                set0.add(x);
                return true;
            } else if (set1.size() < THRESHOLD) {
                set1.add(x);
                return true;
            }

            if (set0.size() < PROBE_SIZE) {
                set0.add(x);
                i = 0;
                h = h0;
            } else if (set1.size() < PROBE_SIZE) {
                set1.add(x);
                i = 1;
                h = h1;
            } else {
                mustResize = true;
            }
        } finally {
            release(x);
        }

        if (mustResize) {
            resize();
            add(x);
        } else if (!relocate(i, h)) {
            resize();
        }
        return true;
    }

    protected boolean relocate(int i, int hi) {
        int hj = 0;
        int j  = 1 - i;

        for (int round = 0; round < LIMIT; round++) {
            List<T> iSet = table[i][hi];
            T       y    = iSet.get(0);
            switch (i) {
                case 0:
                    hj = hash1(y) % capacity;
                    break;
                case 1:
                    hj = hash0(y) % capacity;
                    break;
            }
            acquire(y);
            List<T> jSet = table[j][hj];
            try {
                if (iSet.remove(y)) {
                    if (jSet.size() < THRESHOLD) {
                        jSet.add(y);
                        return true;
                    } else if (jSet.size() < PROBE_SIZE) {
                        jSet.add(y);
                        i = 1 - i;
                        hi = hj;
                        j = 1 - j;
                    } else {
                        iSet.add(y);
                        return false;
                    }
                } else if (iSet.size() >= THRESHOLD) {
                    continue;
                } else {
                    return true;
                }
            } finally {
                release(y);
            }
        }
        return false;
    }

    public boolean present(T x) {
        acquire(x);
        try {
            List<T> set0 = table[0][hash0(x) % capacity];
            if (set0.contains(x)) {
                return true;
            }

            List<T> set1 = table[1][hash1(x) % capacity];
            if (set1.contains(x)) {
                return true;
            }
        } finally {
            release(x);
        }
        return false;
    }

    protected int hash0(T x) {
        return (x.hashCode() % 9) % 8;
    }

    protected int hash1(T x) {
        return (x.hashCode() % 11) % 8;
    }

    protected abstract void acquire(T x);

    protected abstract void release(T x);

    protected abstract void resize();

}
