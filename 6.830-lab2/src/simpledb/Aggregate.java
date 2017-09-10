package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends Operator {
    public DbIterator child;
    public int afield;
    public int gfield;
    public Aggregator.Op aop;
    public Type computeType;
    public Aggregator ags;
    public DbIterator dbIterator;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.computeType = child.getTupleDesc().getFieldType(afield);
        Type gfieldtype = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);
        if (computeType.equals(Type.INT_TYPE)){
            this.ags = new IntegerAggregator(gfield,gfieldtype,afield,aop);
        } else {
            this.ags = new StringAggregator(gfield,gfieldtype,afield,aop);
        }
        try {
            child.open();
            while (child.hasNext()){
                this.ags.mergeTupleIntoGroup(child.next());
            }
        } catch (Exception e){
            throw  new IllegalStateException("can't read child");
        }
        this.dbIterator = ags.iterator();
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
        dbIterator.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (dbIterator.hasNext()){
            return dbIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        dbIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return dbIterator.getTupleDesc();
    }

    public void close() {
        // some code goes here
        dbIterator.close();
    }
}
