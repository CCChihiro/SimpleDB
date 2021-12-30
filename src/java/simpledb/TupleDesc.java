package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType);
        }
    }

    private List<TDItem> tupleDescItems = new LinkedList<>();

    /**
     * @return An iterator which iterates over all the field TDItems that are
     *         included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.tupleDescItems.iterator();
        // return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the specified
     * types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        TDItem tdItem;
        int length = typeAr.length;
        for (int i = 0; i < length; i++) {
            tdItem = new TDItem(typeAr[i], fieldAr[i]);
            this.tupleDescItems.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with fields of
     * the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.tupleDescItems.size();
        // return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return this.tupleDescItems.get(i).fieldName;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException(e.getMessage());
        }
        // return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        try {
            return this.tupleDescItems.get(i).fieldType;
        } catch (IndexOutOfBoundsException e) {
            throw new NoSuchElementException(e.getMessage());
        }
        // return null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tupleDescItems.size(); i++) {
            if (Objects.equals(this.getFieldName(i), name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
        // return 0;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note
     *         that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return tupleDescItems.stream().map(tdItem -> tdItem.fieldType.getLen()).mapToInt(Integer::valueOf).sum();
        // return 0;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> mergedDescItems = new ArrayList<>();
        mergedDescItems.addAll(td1.tupleDescItems);
        mergedDescItems.addAll(td2.tupleDescItems);
        int num_fields = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type[num_fields];
        String[] fieldAr = new String[num_fields];
        for (int i = 0; i < num_fields; i++) {
            typeAr[i] = mergedDescItems.get(i).fieldType;
            fieldAr[i] = mergedDescItems.get(i).fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
        // return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items and if
     * the i-th type in this TupleDesc is equal to the i-th type in o for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o instanceof TupleDesc) {
            TupleDesc otd = (TupleDesc) o;
            if (otd.numFields() != numFields())
                return false;
            for (int i = 0; i < numFields(); i++) {
                if (!otd.tupleDescItems.get(i).equals(tupleDescItems.get(i)))
                    return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the
     * exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        List<String> strings = new ArrayList<>();
        for (TDItem tdItem : tupleDescItems) {
            strings.add(String.format("%s(%s)", tdItem.fieldName, tdItem.fieldType));
        }
        return String.join(", ", strings);
        // return "";
    }
}
