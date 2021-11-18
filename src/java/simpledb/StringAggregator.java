package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int groupByFieldId;
    private final Type groupByFieldType;
    private final int aggFieldId;
    private final Op aggOp;

    private final Map<Field, Object> aggedFields = new HashMap<>();
    private final Map<Field, Integer> aggedCounts = new HashMap<>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldId = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggFieldId = afield;
        this.aggOp = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = this.groupByFieldId == NO_GROUPING ? noGroupingField : tup.getField(this.groupByFieldId);
        assert groupByField.getType() == this.groupByFieldType;

        this.aggedCounts.merge(groupByField, 1, Integer::sum);
        String curValue = ((StringField) tup.getField(this.aggFieldId)).getValue();

        if (aggOp == Op.COUNT) {
            this.aggedFields.merge(groupByField, 1, (old, cur) -> (int) old + 1);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator();
//        throw new UnsupportedOperationException("please implement me for lab2");
    }
    private class StringAggregatorIterator implements OpIterator {
        private Iterator<Map.Entry<Field, Object>> it;

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
            Map.Entry<Field, Object> next = it.next();
            Tuple tp = new Tuple(this.getTupleDesc());

            if (groupByFieldId == NO_GROUPING) {
                if (aggOp == Op.COUNT) {
                    tp.setField(0, new IntField(aggedCounts.get(next.getKey())));
                } else {
                    throw new UnsupportedOperationException();
                }
            } else {
                if (aggOp == Op.COUNT) {
                    tp.setField(0, next.getKey());
                    tp.setField(1, new IntField(aggedCounts.get(next.getKey())));
                } else {
                    throw new UnsupportedOperationException();
                }
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
                typeAr = new Type[] { groupByFieldType };
            } else {
                if (aggOp == Op.COUNT) {
                    typeAr = new Type[] { groupByFieldType, Type.INT_TYPE };
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            return new TupleDesc(typeAr);
        }

        @Override
        public void close() {
            this.it = null;
        }
    }
}
