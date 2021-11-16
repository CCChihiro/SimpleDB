package simpledb;

import java.util.Iterator;

public class HeapFileIterator extends AbstractDbFileIterator {
    private final HeapFile heapFile;
    private final TransactionId tid;
    private Iterator<Tuple> it = null;
    private Integer currentPageNo = null;

    HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
    }

    private Iterator<Tuple> getCurrentPageIterator(int pageNo) throws TransactionAbortedException, DbException {
        HeapPageId pid = new HeapPageId(this.heapFile.getId(), pageNo);
        return ((HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY)).iterator();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.currentPageNo = 0;
        this.it = this.getCurrentPageIterator(this.currentPageNo);
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        // not opened
        if (this.it == null)
            return null;

        // current page
        if (this.it.hasNext()) {
            return this.it.next();
        }

        // next page
        while (!this.it.hasNext() && ++this.currentPageNo < this.heapFile.numPages()) {
            this.it = this.getCurrentPageIterator(this.currentPageNo);
        }
        return this.it.hasNext() ? this.it.next() : null;

        // return null;
    }

    @Override
    public void close() {
        super.close();
        this.it = null;
        this.currentPageNo = null;
    }

}
