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

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
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
        return this.file;
        // return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note: you
     * will need to generate this tableid somewhere to ensure that each HeapFile has
     * a "unique id," and that you always return the same value for a particular
     * HeapFile. We suggest hashing the absolute file name of the file underlying
     * the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsolutePath().hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        // throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getTableId() != this.getId()) {
            throw new IllegalArgumentException();
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file, "r");
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] pageBuf = HeapPage.createEmptyPageData();
            raf.read(pageBuf, 0, BufferPool.getPageSize());
            raf.close();
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), pageBuf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (this.file.length() / BufferPool.getPageSize());
        // return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page;
        int i;
        for (i = 0; i < this.numPages(); ++i) {
            PageId pid = new HeapPageId(this.getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() == 0) {
                continue;
            }
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            page.insertTuple(t);
            return new ArrayList<>(Collections.singletonList(page));
        }
        page = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());
        page.insertTuple(t);
        this.writePage(page);
        return new ArrayList<>(Collections.singletonList(page));
        // return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // return null;
        RecordId rid = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, rid.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
        // return null;
    }

}
