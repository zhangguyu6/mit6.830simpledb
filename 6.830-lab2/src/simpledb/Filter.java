package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    DbIterator child;
    Predicate p;
    ArrayList<Tuple> childTups = new ArrayList<>();
    TupleDesc td;
    Iterator<Tuple> it;
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.p = p;
        this.child =child;
        td = child.getTupleDesc();
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        child.open();
        while (child.hasNext()){
            Tuple nextchild = child.next();
            if (p.filter(nextchild))
                childTups.add(nextchild);
        }
        it = childTups.iterator();
    }

    public void close() {
        // some code goes here
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = childTups.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        } else return null;
    }
}
