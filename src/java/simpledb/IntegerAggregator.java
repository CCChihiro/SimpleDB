package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByFieldId;
    private final Type groupByFieldType;
    private final int aggFieldId;
    private final Op aggOp;

    private final Map<Field, Integer> aggedFields = new HashMap<>();
    private final Map<Field, Integer> aggedCounts = new HashMap<>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldId = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggFieldId = afield;
        this.aggOp = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = this.groupByFieldId == NO_GROUPING ? IntegerAggregator.noGroupingField
                : tup.getField(this.groupByFieldId);

        this.aggedCounts.merge(field, 1, Integer::sum);
        int curValue = ((IntField) tup.getField(this.aggFieldId)).getValue();

        switch (this.aggOp) {
            case MIN:
                this.aggedFields.merge(field, curValue, Math::min);
                break;
            case MAX:
                this.aggedFields.merge(field, curValue, Math::max);
                break;
            case AVG:
            case SUM:
                this.aggedFields.merge(field, curValue, Integer::sum);
                break;
            case COUNT:
                this.aggedFields.merge(field, 1, Integer::sum);
                break;
            default:
                throw new UnsupportedOperationException("operator " + this.aggOp + " is not supported.");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggregatorIterator();
    }
    private class IntegerAggregatorIterator implements OpIterator {
        private Iterator<Map.Entry<Field, Integer>> it;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.it = aggedFields.entrySet().iterator();

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return this.it != null && this.it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Map.Entry<Field, Integer> next = it.next();
            Tuple tp = new Tuple(this.getTupleDesc());
            IntField value;
            if (aggOp == Op.AVG) {
                value = new IntField(next.getValue() / aggedCounts.get(next.getKey()));
            } else {
                value = new IntField(next.getValue());
            }

            if (groupByFieldId == NO_GROUPING) {
                tp.setField(0, value);
            } else {
                tp.setField(0, next.getKey());
                tp.setField(1, value);
            }
            return tp;
            // return null;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            Type[] typeAr;
            if (groupByFieldId == NO_GROUPING) {
                typeAr = new Type[]{Type.INT_TYPE};
            } else {
                typeAr = new Type[]{groupByFieldType, Type.INT_TYPE};
            }
            return new TupleDesc(typeAr);
            // return null;
        }

        @Override
        public void close() {
            this.it = null;

        }
    }
}
