package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;

    /** 是否fetchnext已经被调用 */
    private boolean called;

    /** 插入个数 */
    private Integer count;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        TupleDesc td1=Database.getCatalog().getTupleDesc(tableId);
        TupleDesc td2=child.getTupleDesc();
        if(!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())){
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
        this.td=new TupleDesc(new Type[]{Type.INT_TYPE});
        this.transactionId=t;
        this.child=child;
        this.tableId=tableId;
        this.called=false;
        this.count=null;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.called=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(this.called){
            return null;
        }else{
            this.called=true;
        }
        if(this.count==null){
            int tempCount=0;
            while(child.hasNext()){
                Tuple inserted=child.next();
                try {
                    Database.getBufferPool().insertTuple(this.transactionId,this.tableId,inserted);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                tempCount++;
            }
            this.count=tempCount;
        }
        Tuple result=new Tuple(this.td);
        result.setField(0,new IntField(this.count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child=children[0];
    }
}
