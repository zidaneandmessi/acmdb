package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages;argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numPages;
    private int pageCnt;
    private ConcurrentHashMap<PageId, Page> pageMap;

    public class LinkList {

        public class Node {

            public PageId pageId;
            public Node next;

            public Node(PageId pageId) {
                this.pageId = pageId;
            }
        }
        public Node head;
        public Node tail;

        public LinkList() {
            head = tail = null;
        }

        public synchronized void add(PageId pageId) {
            if (head == null) {
                head = new Node(pageId);
                tail = head;
            }
            else {
                tail.next = new Node(pageId);
                tail = tail.next;
            }
        }

        public synchronized void del(PageId pageId) {
            if (head == null) return;
            if (head.pageId.equals(pageId)) {
                head = head.next;
                if (head == null)
                    tail = null;
                else if (head.next == null)
                    tail = head;
                return;
            }
            for (Node n = head; n.next != null; n = n.next)
                if (n.next.pageId.equals(pageId)) {
                    n.next = n.next.next;
                    if (n.next == null)
                        tail = n;
                    return;
                }
        }
        public synchronized void clear() {
            head = tail = null;
        }
    }

    public LinkList LRU;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pageCnt = 0;
        this.pageMap = new ConcurrentHashMap<PageId, Page>();
        this.LRU = new LinkList();
        this.locker = new Locker();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    public enum LockType {
        SHARED_LOCK, EXCLUSIVE_LOCK;
    }

    public class Locker {

        public class Lock {

            public LockType lockType;
            public Vector<TransactionId> tids;

            public Lock() {
                this.lockType = null;
                this.tids = new Vector<TransactionId>();
            }
            public Lock(LockType lockType) {
                this.lockType = lockType;
                this.tids = new Vector<TransactionId>();
            }
        }

        private ConcurrentHashMap<PageId, Lock> pageLockTable; // Locks on a page
        private ConcurrentHashMap<TransactionId, Vector<PageId>> dirtyPagesTable; // Pages dirtied by tid
        private ConcurrentHashMap<Integer, AtomicInteger> pidObjectTable;
        private ConcurrentHashMap<TransactionId, Vector<TransactionId>> tidDepGraph;

        public Locker() {
            pageLockTable = new ConcurrentHashMap<PageId, Lock>();
            dirtyPagesTable = new ConcurrentHashMap<TransactionId, Vector<PageId>>();
            pidObjectTable = new ConcurrentHashMap<Integer, AtomicInteger>();
            tidDepGraph = new ConcurrentHashMap<TransactionId, Vector<TransactionId>>();
        }

        public AtomicInteger getPidObject(int x) {
            Integer X = new Integer(x);
            if (pidObjectTable.containsKey(X))
                return pidObjectTable.get(X);
            AtomicInteger atoInt = new AtomicInteger(X);
            pidObjectTable.put(X, atoInt);
            return atoInt;
        }

        public void acquireSharedLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
            if (!dirtyPagesTable.containsKey(tid)) {
                Vector<PageId> pidSet = new Vector<PageId>();
                pidSet.add(pid);
                dirtyPagesTable.put(tid, pidSet);
            }
            else {
                Vector<PageId> pidSet = dirtyPagesTable.get(tid);
                pidSet.add(pid);
                dirtyPagesTable.put(tid, pidSet);
            }
            Vector<TransactionId> tidSet;
            synchronized(getPidObject(pid.hashCode())) {
                while(true) {
                    if (!pageLockTable.containsKey(pid)) {
                        Lock lock = new Lock(LockType.SHARED_LOCK);
                        lock.tids.add(tid);
                        pageLockTable.put(pid, lock);
                        break;
                    }
                    Lock lock = pageLockTable.get(pid);
                    if (lock.lockType == LockType.SHARED_LOCK) {
                        if (!lock.tids.contains(tid)) {
                            lock.tids.add(tid);
                            pageLockTable.put(pid, lock);
                        }
                        break;
                    }
                    else if (lock.lockType == LockType.EXCLUSIVE_LOCK && lock.tids.contains(tid))
                        break;
                    Iterator<TransactionId> it = lock.tids.iterator();
                    TransactionId depTid = it.next();
                    if (!tid.equals(depTid))
                        addEdge(tid, depTid);
                    if (hasDeadLock())
                        throw new TransactionAbortedException();
                    try {
                        getPidObject(pid.hashCode()).wait();
                    } catch (InterruptedException e) {}
                }
            }
        }

        public void acquireExclusiveLock(TransactionId tid, PageId pid) 
            throws TransactionAbortedException {
            if (!dirtyPagesTable.containsKey(tid)) {
                Vector<PageId> pidSet = new Vector<PageId>();
                pidSet.add(pid);
                dirtyPagesTable.put(tid, pidSet);
            }
            else {
                Vector<PageId> pidSet = dirtyPagesTable.get(tid);
                pidSet.add(pid);
                dirtyPagesTable.put(tid, pidSet);
            }
            Vector<TransactionId> tidSet;
            synchronized(getPidObject(pid.hashCode())) {
                while(true) {
                    if (!pageLockTable.containsKey(pid)) {
                        Lock lock = new Lock(LockType.EXCLUSIVE_LOCK);
                        lock.tids.add(tid);
                        pageLockTable.put(pid, lock);
                        break;
                    }
                    Lock lock = pageLockTable.get(pid);
                    if (lock.lockType == LockType.SHARED_LOCK && lock.tids.size() == 1 && lock.tids.contains(tid)) {
                        lock.lockType = LockType.EXCLUSIVE_LOCK;
                        pageLockTable.put(pid, lock);
                        break;
                    }
                    else if (lock.lockType == LockType.EXCLUSIVE_LOCK && lock.tids.contains(tid))
                        break;
                    if (lock.lockType == LockType.SHARED_LOCK) {
                        Iterator<TransactionId> it = lock.tids.iterator();
                        while (it.hasNext()) {
                            TransactionId depTid = it.next();
                            if (!tid.equals(depTid))
                                addEdge(tid, depTid);
                        }
                        if (hasDeadLock())
                            throw new TransactionAbortedException();
                    }
                    else {
                        Iterator<TransactionId> it = lock.tids.iterator();
                        TransactionId depTid = it.next();
                        if (!tid.equals(depTid))
                            addEdge(tid, depTid);
                        if (hasDeadLock())
                            throw new TransactionAbortedException();
                    }
                    try {
                        getPidObject(pid.hashCode()).wait();
                    } catch (InterruptedException e) {}
                }
            }
        }

        private ConcurrentHashMap<TransactionId, Integer> dfn;
        private ConcurrentHashMap<TransactionId, Integer> low;
        private ConcurrentHashMap<TransactionId, Integer> fresh;
        private ConcurrentHashMap<TransactionId, Boolean> inStack;
        private int tot;
        private Stack<TransactionId> stack;
        private boolean deadLockFound;

        private void tarjan(TransactionId tid) {
            if (deadLockFound || !tidDepGraph.containsKey(tid))
                return;
            tot++;
            dfn.put(tid, tot);
            low.put(tid, tot);
            stack.push(tid);
            inStack.put(tid, true);
            Vector<TransactionId> tidSet = tidDepGraph.get(tid);
            Iterator<TransactionId> it = tidSet.iterator();
            while (it.hasNext()) {
                TransactionId nextTid = it.next();
                if (!dfn.containsKey(nextTid) || dfn.get(nextTid) == 0) {
                    tarjan(nextTid);
                    if (deadLockFound) return;
                    if (!dfn.containsKey(nextTid)) continue;
                    int tidLow = low.get(tid);
                    int nxtLow = low.get(nextTid);
                    if (nxtLow < tidLow)
                        low.put(tid, nxtLow);
                }
                else if (inStack.containsKey(nextTid) && inStack.get(nextTid) == true) {
                    int tidLow = low.get(tid);
                    int nxtDfn = dfn.get(nextTid);
                    if (nxtDfn < tidLow)
                        low.put(tid, nxtDfn);
                }
            }
            if (dfn.get(tid) == low.get(tid)) {
                if (stack.pop() != tid) {
                    deadLockFound = true;
                    return;
                }
                inStack.put(tid, false);
            }
        }

        public synchronized boolean hasDeadLock() {
            dfn = new ConcurrentHashMap<TransactionId, Integer>();
            low = new ConcurrentHashMap<TransactionId, Integer>();
            fresh = new ConcurrentHashMap<TransactionId, Integer>();
            inStack = new ConcurrentHashMap<TransactionId, Boolean>();
            tot = 0;
            stack = new Stack<TransactionId>();
            deadLockFound = false;
            for (TransactionId tid : tidDepGraph.keySet())
                if (!dfn.containsKey(tid)) {
                    tarjan(tid);
                    if (deadLockFound)
                        return true;
                }
            return false;
        }

        public synchronized void addEdge(TransactionId tid1, TransactionId tid2) {
            Vector<TransactionId> tidSet;
            if (!tidDepGraph.containsKey(tid1)) 
                tidSet = new Vector<TransactionId>();
            else
                tidSet = tidDepGraph.get(tid1);
            if (!tidSet.contains(tid2))
                tidSet.add(tid2);
            tidDepGraph.put(tid1, tidSet);
        }

        public synchronized void removeEdge(TransactionId tid) {
            tidDepGraph.remove(tid);
            for (Vector<TransactionId> tidSet : tidDepGraph.values())
                tidSet.remove(tid);
        }

        public void releaseLock(TransactionId tid) {
            if (!dirtyPagesTable.containsKey(tid))
                return;
            Vector<PageId> dirtyPages = dirtyPagesTable.get(tid);
            Iterator<PageId> it = dirtyPages.iterator();
            while (it.hasNext()) {
                PageId pid = it.next();
                synchronized(getPidObject(pid.hashCode())) {
                    if (pageLockTable.containsKey(pid)) {
                        Lock lock = pageLockTable.get(pid);
                            if (lock.tids.contains(tid))
                                lock.tids.remove(tid);
                            if (lock.tids.size() == 0) {
                                pageLockTable.remove(pid);
                            }
                    }
                    getPidObject(pid.hashCode()).notify();
                }
            }
            removeEdge(tid);
            dirtyPagesTable.remove(tid);
        }

        public boolean holdsLock(TransactionId tid, PageId pid) {
            if (!pageLockTable.containsKey(pid))
                return false;
            return pageLockTable.get(pid).tids.contains(tid);
        }
    }

    private Locker locker;

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (perm == Permissions.READ_ONLY)
            locker.acquireSharedLock(tid, pid);
        else if (perm == Permissions.READ_WRITE)
            locker.acquireExclusiveLock(tid, pid);
        Page p = null;
        if (pageMap.containsKey(pid))
            p = pageMap.get(pid);
        else {
            p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            while (pageCnt >= numPages)
                evictPage();
            pageMap.put(pid, p);
            pageCnt++;
        }
        LRU.del(pid);
        LRU.add(pid);
        return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        locker.releaseLock(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return locker.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit)
            flushPages(tid);
        else { // abort
            for (ConcurrentHashMap.Entry<PageId, Page> entry : pageMap.entrySet()) {
                PageId pid = entry.getKey();
                Page p = entry.getValue();
                if (p.isDirty() == tid) {
                    DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    p = f.readPage(pid);
                    if (pageMap.containsKey(pid)) {
                        pageMap.put(pid, p);
                        LRU.del(pid);
                        LRU.add(pid);
                    }
                }
            }
        }
        locker.releaseLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *66
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = f.insertTuple(tid, t);
        for (Page p : pages) {
            PageId pid = p.getId();
            p.markDirty(true, tid);
            LRU.del(pid);
            LRU.add(pid);
            if (pageMap.containsKey(pid)) {
                pageMap.remove(pid);
                pageCnt--;
            }
            while (pageMap.size() >= numPages)
                evictPage();
            pageMap.put(p.getId(), p);
            pageCnt++;
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> pages = f.deleteTuple(tid, t);
        for (Page p : pages) {
            PageId pid = p.getId();
            p.markDirty(true, tid);
            LRU.del(pid);
            LRU.add(pid);
            if (pageMap.containsKey(pid)) {
                pageMap.remove(pid);
                pageCnt--;
            }
            while (pageMap.size() >= numPages)
                evictPage();
            pageMap.put(p.getId(), p);
            pageCnt++;
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pageMap.keySet())
            flushPage(pid);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        LRU.del(pid);
        if (pageMap.containsKey(pid)) {
            try {
                flushPage(pid);
            } catch(IOException e) {
                e.printStackTrace();
            }
            pageMap.remove(pid);
            pageCnt--;
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile f = Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page p = pageMap.get(pid);
        if (p == null || p.isDirty() == null)
            return;
        try {
            f.writePage(p);
            p.markDirty(false, null);
        } catch(IOException e) {
            e.printStackTrace();
        }
        LRU.del(pid);
        LRU.add(pid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (ConcurrentHashMap.Entry<PageId, Page> entry : pageMap.entrySet()) {
            PageId pid = entry.getKey();
            Page p = entry.getValue();
            if (p.isDirty() == tid)
                flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        LinkList.Node node = LRU.head;
        if (node == null)
            throw new DbException("All dirty!");
        Page p = pageMap.get(node.pageId);
        while (node != null && (p.isDirty() != null || !pageMap.containsKey(node.pageId))) {
            node = node.next;
            if (node != null)
                p = pageMap.get(node.pageId);
        }
        if (node == null)
            throw new DbException("All dirty!");
        discardPage(node.pageId);
    }

}
