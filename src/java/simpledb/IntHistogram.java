package simpledb;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private final int[] buckets;
    private final int min;
    private final int max;
    private final double bucketStep;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        Arrays.fill(this.buckets, 0);
        this.min = min;
        this.max = max;
        this.bucketStep = (double) (max - min + 1) / buckets;
    }

    private int getIndexByValue(int value) {
        return (int) ((double) (value - min) * (buckets.length) / (max + 1 - min));
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = this.getIndexByValue(v);
        ++this.buckets[index];
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        Predicate.Op fakeOp = op;
        int idx = getIndexByValue(v);
        if (idx < 0) {
            idx = 0;
            if (op == Predicate.Op.GREATER_THAN) {
                fakeOp = Predicate.Op.GREATER_THAN_OR_EQ;
            } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
                fakeOp = Predicate.Op.LESS_THAN;
            } else if (op == Predicate.Op.EQUALS) {
                fakeOp = Predicate.Op.LESS_THAN;
            } else if (op == Predicate.Op.NOT_EQUALS) {
                fakeOp = Predicate.Op.GREATER_THAN_OR_EQ;
            }
        } else if (idx >= buckets.length) {
            idx = buckets.length - 1;
            if (op == Predicate.Op.LESS_THAN) {
                fakeOp = Predicate.Op.LESS_THAN_OR_EQ;
            } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
                fakeOp = Predicate.Op.GREATER_THAN;
            } else if (op == Predicate.Op.EQUALS) {
                fakeOp = Predicate.Op.GREATER_THAN;
            } else if (op == Predicate.Op.NOT_EQUALS) {
                fakeOp = Predicate.Op.LESS_THAN_OR_EQ;
            }
        }

        int sum = Arrays.stream(buckets).sum();
        int lessThan = Arrays.stream(buckets, 0, idx).sum();
        int greatThan = Arrays.stream(buckets, idx, buckets.length).sum() - buckets[idx];

        int elements;

        switch (fakeOp) {
            case EQUALS:
                elements = sum - lessThan - greatThan;
                break;
            case LESS_THAN:
                elements = lessThan;
                break;
            case LESS_THAN_OR_EQ:
                elements = sum - greatThan;
                break;
            case GREATER_THAN:
                elements = greatThan;
                break;
            case GREATER_THAN_OR_EQ:
                elements = sum - lessThan;
                break;
            case NOT_EQUALS:
                elements = lessThan + greatThan;
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return sum == 0 ? 0 : (double) elements / sum;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity(Predicate.Op op)
    {
        // some code goes here
        double avg = 0.0;
        if (max - min + 1 > 100) {
            Random r = new Random();
            for (int i = 0; i < 100; ++i)
                avg += estimateSelectivity(op, r.nextInt(max - min + 1) + min);
            return avg / 100;
        } else {
            for (int i = min; i <= max; ++i)
                avg += estimateSelectivity(op, i);
            return avg / (max - min + 1);
        }
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        List<String> stringList = Arrays.stream(buckets).mapToObj(String::valueOf).collect(Collectors.toList());
        return String.join(", ", stringList);
    }
}
