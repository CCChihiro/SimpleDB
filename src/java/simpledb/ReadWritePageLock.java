package simpledb;

import java.util.HashSet;
import java.util.Set;

public class ReadWritePageLock {
    // private Permissions perm;
    private Set<TransactionId> readers;
    private TransactionId writer;

    public ReadWritePageLock() {
        // this.perm = null;
        this.readers = new HashSet<>();
        this.writer = null;
    }

    public void acquireLock(TransactionId tid, Permissions perm) throws TransactionAbortedException {
        int numTried = 0;
        for (; numTried < 5; ++numTried) {
            if (this.doAcquire(tid, perm))
                return;

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        throw new TransactionAbortedException();
    }

    private synchronized boolean doAcquire(TransactionId tid, Permissions perm) {
        /**
         * -  R      W
         * R true   false
         * W false  false
         */

        switch (perm) {
            case READ_ONLY:
                if (this.writer != null && this.writer != tid) {
                    return false;
                }
                this.readers.add(tid);
                return true;

            case READ_WRITE:
                if (this.writer != null && this.writer != tid) {
                    return false;
                }
                if (this.readers.isEmpty()) {
                    this.readers.add(tid);
                    this.writer = tid;
                    return true;
                } else if (this.readers.size() == 1 && this.readers.contains(tid)) {
                    this.writer = tid;
                    return true;
                } else {
                    return false;
                }

        }
        return false;
    }

    public void release(TransactionId tid) {
        synchronized (this) {
            this.readers.remove(tid);
            if (this.writer == tid) {
                this.writer = null;
            }
        }
    }

    public boolean holdsLock(TransactionId tid) {
        return this.readers.contains(tid);
    }

    public Set<TransactionId> getTransactions() {
        return this.readers;
    }

}
