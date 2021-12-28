package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {


    private Map<PageId, List<LockState>> lockStatesByPgId;

    private Map<TransactionId, PageId> waitingInfo;//事务在某页加载失败，加载事务是阻塞的，因此一个tid最多映射一个pid

    public static final long SLEEP_MILLIS = 500;

    public LockManager() {
        lockStatesByPgId = new ConcurrentHashMap<>();
        waitingInfo = new ConcurrentHashMap<>();
    }


    /**
     * grant share-lock on the Page to the Transaction
     *
     * @param tid the Transaction that asks for s-lock.
     * @param pid the Page that need to be locked.
     * @return <p>true</p> if (lock and) grant, or <p>false</p> if wait and deny.
     */
    public synchronized boolean grantSLock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStatesByPgId.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {
                // there exists only 1 lock:
                LockState ls = list.iterator().next();
                if (ls.getTid().equals(tid)) {
                    // if there exists a lock of this tid:
                    // grant (what exists is  a s-lock) or lock and grant
                    return ls.getPerm() == Permissions.READ_ONLY
                            || lock(pid, tid, Permissions.READ_ONLY);
                } else {
                    // What exists is not belong to this tid:
                    // lock and grant (what exists is a s-lock) or wait and deny
                    return ls.getPerm() == Permissions.READ_ONLY
                            ? lock(pid, tid, Permissions.READ_ONLY)
                            : wait(tid, pid);
                }
            } else {
                // there exists more than one lock:
                // there are four situations:
                // 1. one s-lock and one x-lock, either of them belongs to tid => grant
                // 2. one s-lock and one x-lock, neither of them belongs to tid => wait and deny
                // 3. more than one s-lock and among them there is one belongs to tid => grant
                // 4. more than one s-lock and among them there is no one belongs to tid => lock and grant
                for (LockState ls : list) {
                    if (ls.getPerm() == Permissions.READ_WRITE) {
                        // situation 1 or 2
                        return ls.getTid().equals(tid) || wait(tid, pid);
                    } else if (ls.getTid().equals(tid)) {
                        // situation 1 or 3
                        return true;
                    }
                }
                // situation 4
                return lock(pid, tid, Permissions.READ_ONLY);
            }
        } else {
            // no lock before:
            return lock(pid, tid, Permissions.READ_ONLY);
        }
    }

    /**
     * grant exclusive-lock on the Page to the Transaction
     *
     * @param tid the Transaction that asks for x-lock.
     * @param pid the Page that need to be locked.
     * @return <p>true</p> if (lock and) grant, or <p>false</p> if wait and deny.
     */
    public synchronized boolean grantXLock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStatesByPgId.get(pid);
        if (list != null && list.size() != 0) {
            if (list.size() == 1) {
                // there exists only 1 lock:
                LockState ls = list.iterator().next();
                return ls.getTid().equals(tid)
                        ? ls.getPerm() == Permissions.READ_WRITE || lock(pid, tid, Permissions.READ_WRITE)
                        : wait(tid, pid);
            } else {
                // there exists more than one lock:
                // there are four situations:
                // 1. one s-lock and one x-lock, either of them belongs to tid => grant
                // 2. one s-lock and one x-lock, either of them belongs to tid => wait and deny
                // 3. more than one s-lock => wait and deny
                if (list.size() == 2) {
                    for (LockState ls : list) {
                        if (ls.getTid().equals(tid) && ls.getPerm() == Permissions.READ_WRITE) {
                            // situation 1
                            return true;
                        }
                    }
                }
                return wait(tid, pid);
            }
        } else {//pid上没有锁，可以加写锁
            return lock(pid, tid, Permissions.READ_WRITE);
        }
    }


    /**
     * grant lock of Page to Transaction
     * @param pid the Page to be locked
     * @param tid the Transaction to hold the lock
     * @param perm the perm of the lock
     * @return <p>true</p>
     */
    private synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm) {
        LockState nls = new LockState(tid, perm);
        ArrayList<LockState> list = (ArrayList<LockState>) lockStatesByPgId.get(pid);
        if (list == null) {
            list = new ArrayList<>();
        }
        list.add(nls);
        lockStatesByPgId.put(pid, list);
        waitingInfo.remove(tid);
        return true;
    }

    /**
     * To demonstrate that the Transaction is waiting for pid.
     * @param pid the Page waited to be locked
     * @param tid the Transaction waiting the lock
     * @return <p>false</p>
     */
    private synchronized boolean wait(TransactionId tid, PageId pid) {
        waitingInfo.put(tid, pid);
        return false;
    }


    /**
     * remove the lock on the Page held by the Transaction
     * @param tid the lock's holder
     * @param pid the locked Page
     * @return <p>false</p> if no lock of Transaction is on the Page, or <p>true</p> if remove successfully
     */
    public synchronized boolean unlock(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStatesByPgId.get(pid);
        if (list == null || list.size() == 0) return false;
        LockState ls = getLockState(tid, pid);
        if (ls == null) return false;
        list.remove(ls);
        lockStatesByPgId.put(pid, list);
        return true;
    }

    /**
     * remove all locks held by the Transaction
     * @param tid the lock's holder
     */
    public synchronized void releaseTransactionLocks(TransactionId tid) {
        List<PageId> toRelease = getAllLocksByTid(tid);
        for (PageId pid : toRelease) {
            unlock(tid, pid);
        }
    }

    /**
     * check whether deadlock occurred when the Transaction is waiting for the Page.
     * @param tid the Transaction trapped into waiting.
     * @param pid the Page that being waiting for.
     * @return <p>true</p> if deadlock occurred <p>false</p> or not
     */
    public synchronized boolean deadlockOccurred(TransactionId tid, PageId pid) {
        List<LockState> lockStates = lockStatesByPgId.get(pid);
        if (lockStates == null || lockStates.size() == 0) {
            return false;
        }
        List<PageId> pids = getAllLocksByTid(tid); // find all pages tid has locked.
        // traverse the holders of pid to find
        // whether one of them is waiting for some page
        // that is being held by tid
        for (LockState ls : lockStates) {
            TransactionId holder = ls.getTid(); // the transaction that hold the pid.
            if (!holder.equals(tid)) {
                boolean isWaiting = isWaitingResources(holder, pids, tid);
                if (isWaiting) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Recursively judge whether Tranaction tid is waiting for some page that held by tid2
     *
     * @param tid the Transaction that may hold some page needed by tid2.
     * @param pids the Pages being held by tid2.
     * @param tid2 the Transaction that may hold some Page needed by tid.
     * @return
     */
    private synchronized boolean isWaitingResources(TransactionId tid, List<PageId> pids, TransactionId tid2) {
        PageId waitingPage = waitingInfo.get(tid);
        if (waitingPage == null) {
            return false;
        }
        for (PageId pid : pids) {
            if (pid.equals(waitingPage)) {
                return true;
            }
        }

        List<LockState> lockStates = lockStatesByPgId.get(waitingPage);
        if (lockStates == null || lockStates.size() == 0)
            return false;
        for (LockState ls : lockStates) {
            TransactionId holder = ls.getTid();
            if (!holder.equals(tid2)) {
                boolean isWaiting = isWaitingResources(holder, pids, tid2);
                if (isWaiting) return true;
            }
        }
        return false;
    }


//==========================查询与修改两个map信息的相关方法 begin=========================

    /**
     * get the lock on the specified page held by the specified transaction
     * @param tid the specified transaction
     * @param pid the specified page
     * @return the lock or null
     */
    public synchronized LockState getLockState(TransactionId tid, PageId pid) {
        ArrayList<LockState> list = (ArrayList<LockState>) lockStatesByPgId.get(pid);
        if (list == null || list.size() == 0) {
            return null;
        }
        for (LockState ls : list) {
            if (ls.getTid().equals(tid)) {
                return ls;
            }
        }
        return null;
    }

    /**
     * 得到tid所拥有的所有锁，以锁所在的资源pid的形式返回
     *
     * @param tid
     * @return
     */
    private synchronized List<PageId> getAllLocksByTid(TransactionId tid) {
        ArrayList<PageId> pids = new ArrayList<>();
        for (Map.Entry<PageId, List<LockState>> entry : lockStatesByPgId.entrySet()) {
            for (LockState ls : entry.getValue()) {
                if (ls.getTid().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }

//==========================查询与修改两个map信息的相关方法 end=========================

}