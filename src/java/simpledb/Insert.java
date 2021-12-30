package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private boolean returned;
    private OpIterator child;
    private final int tableId;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we
     *                     are to insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.returned = false;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("tuple desc mismatch when construct Insert Operator");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] { Type.INT_TYPE });

        // return null;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        this.child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the constructor.
     * It returns a one field tuple containing the number of inserted records.
     * Inserts should be passed through BufferPool. An instances of BufferPool is
     * available via Database.getBufferPool(). Note that insert DOES NOT need check
     * to see if a particular tuple is a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or null if
     *         called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (returned) {
            return null;
        }
        int cnt = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, child.next());
            } catch (IOException e) {
                throw new TransactionAbortedException();
                // e.printStackTrace();
            }
            ++cnt;
        }
        Tuple tuple = new Tuple(this.getTupleDesc());
        tuple.setField(0, new IntField(cnt));
        returned = true;
        return tuple;
        // return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
        // return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        assert children.length == 1;
        this.child = children[0];
    }
}
