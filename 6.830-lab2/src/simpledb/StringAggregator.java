package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    public int gbfield;
    public Type gbfieldtype;
    public int afield;
    public Op what;
    public Object ags;
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
        if(!what.COUNT.equals(what)){
            throw new IllegalArgumentException("StringAggregator only support COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == NO_GROUPING) {
            ags = new ArrayList<String>();
        } else if (gbfieldtype == Type.INT_TYPE) {
            ags = new HashMap<Integer, ArrayList<String>>();
        } else {
            ags = new HashMap<String, ArrayList<String>>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            String agsval = ((StringField) tup.getField(afield)).getValue();
            ((ArrayList<String>) ags).add(agsval);
        } else if (gbfieldtype == Type.INT_TYPE) {
            HashMap<Integer, ArrayList<String>> ags = (HashMap<Integer, ArrayList<String>>) this.ags;
            Integer agskey = ((IntField) tup.getField(gbfield)).getValue();
            String agsval = ((StringField) tup.getField(afield)).getValue();
            if (ags.containsKey(agskey)) {
                ags.get(agskey).add(agsval);
            } else {
                ArrayList<String> agsvals = new ArrayList<>();
                agsvals.add(agsval);
                ags.put(agskey, agsvals);
            }
        } else {
            HashMap<String, ArrayList<String>> ags = (HashMap<String, ArrayList<String>>) this.ags;
            String agskey = ((StringField) tup.getField(gbfield)).getValue();
            String agsval = ((StringField) tup.getField(afield)).getValue();
            if (ags.containsKey(agskey)) {
                ags.get(agskey).add(agsval);
            } else {
                ArrayList<String> agsvals = new ArrayList<>();
                agsvals.add(agsval);
                ags.put(agskey, agsvals);
            }
        }
    }
    public TupleDesc getTupleDesc(){
        if (gbfield==NO_GROUPING){
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        } else if (gbfieldtype==Type.INT_TYPE){
            return new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE});
        } else {
            return new TupleDesc(new Type[]{Type.STRING_TYPE,Type.INT_TYPE});
        }
    }
    public Field computefield(ArrayList<Integer> vals){
            return new IntField(vals.size());
    }
    /**
     * Create a DbIterator over group aggregate results.=
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator() {
            ArrayList<Tuple> res;
            Iterator<Tuple> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                res = new ArrayList<>();
                if (gbfield==NO_GROUPING){
                    Tuple t= new Tuple(getTupleDesc());
                    Field tval = computefield((ArrayList<Integer>) ags);
                    t.setField(0,tval);
                    res.add(t);
                    it = res.iterator();
                } else if (gbfieldtype == Type.INT_TYPE){
                    TupleDesc tdes = getTupleDesc();
                    HashMap<Integer, ArrayList<Integer>> map = (HashMap<Integer, ArrayList<Integer>>) ags;
                    for (Integer i :map.keySet()){
                        Tuple t = new Tuple(tdes);
                        Field tkey = new IntField(i);
                        Field tval = computefield(map.get(i));
                        t.setField(0,tkey);
                        t.setField(1,tval);
                        res.add(t);
                    }
                    it = res.iterator();
                } else {
                    TupleDesc tdes = getTupleDesc();
                    HashMap<String, ArrayList<Integer>> map = (HashMap<String, ArrayList<Integer>>) ags;
                    for (String i :map.keySet()){
                        Tuple t = new Tuple(tdes);
                        Field tkey = new StringField(i,i.length());
                        Field tval = computefield(map.get(i));
                        t.setField(0,tkey);
                        t.setField(1,tval);
                        res.add(t);
                    }
                    it = res.iterator();
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (it==null) return false;
                else  return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (it==null) throw new  IllegalStateException("didn't open");
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                it = res.iterator();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return StringAggregator.this.getTupleDesc();
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }

}
