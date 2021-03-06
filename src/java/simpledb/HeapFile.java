package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	private File file;
	private TupleDesc td;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
        // some code goes here
        byte[] data = HeapPage.createEmptyPageData();
        try {
            DataInputStream dis = new DataInputStream(new FileInputStream(file));
            dis.skipBytes(pid.pageNumber() * BufferPool.getPageSize());
            dis.read(data, 0, BufferPool.getPageSize());
            dis.close();
            return new HeapPage((HeapPageId)pid, data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek(page.getId().pageNumber() * BufferPool.getPageSize());
        f.write(page.getPageData(), 0, BufferPool.getPageSize());
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.floor(1.0 * file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (p.getNumEmptySlots() > 0) {
                p.insertTuple(t);
                p.markDirty(true, tid);
                dirtyPagesArr.add(p);
                return dirtyPagesArr;
            }
        }
        HeapPage p = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        p.insertTuple(t);
        writePage(p);
        dirtyPagesArr.add(p);
        return dirtyPagesArr;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> dirtyPagesArr = new ArrayList<Page>();
        HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        p.deleteTuple(t);
        p.markDirty(true, tid);
        dirtyPagesArr.add(p);
        return dirtyPagesArr;
    }

    public class HeapFileIterator implements DbFileIterator {

        public TransactionId tid;
        public HeapPage page;
        private int pageCnt;
        private Iterator<Tuple> it;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
        }
        
        public void open() throws DbException {
            this.pageCnt = 0;
            try {
                page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageCnt++), Permissions.READ_ONLY);
            } catch(TransactionAbortedException e) {
                e.printStackTrace();
            }
            it = page.iterator();
        }

        public boolean hasNext() throws DbException {
            if (page == null || it == null)
                return false;
            if (!it.hasNext()) {
                try {
                    for (int i = pageCnt; i < numPages(); i++)
                        if (((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY)).iterator().hasNext())
                            return true;
                } catch(TransactionAbortedException e) {
                    e.printStackTrace();
                }
                return false;
            }
            return true;
        }

        public Tuple next() throws DbException {
            if (!hasNext())
                throw new NoSuchElementException();
            while (hasNext() && !it.hasNext()) {
                try {
                    page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageCnt++), Permissions.READ_ONLY);
                } catch(TransactionAbortedException e) {
                    e.printStackTrace();
                } catch(DbException e) {
                    e.printStackTrace();
                }
                it = page.iterator();
            }
            return it.next();
        }

        public void rewind() {
            this.pageCnt = 0;
            try {
                page = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageCnt++), Permissions.READ_ONLY);
            } catch(TransactionAbortedException e) {
                e.printStackTrace();
            } catch(DbException e) {
                e.printStackTrace();
            }
            it = page.iterator();
        }

        public void close() {
            this.pageCnt = 0;
            this.page = null;
            this.it = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

