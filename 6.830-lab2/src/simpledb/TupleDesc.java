package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] tditems;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj==null) return false;
            else if (getClass()!=obj.getClass()) return false;
            TDItem other = (TDItem) obj;
            return other.fieldType==fieldType && other.fieldName ==fieldName;
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return (Iterator<TDItem>)Arrays.asList(tditems);
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length!=fieldAr.length) throw new RuntimeException("typeAr's length doesn't equal fieldAr's length");
        this.tditems = new TDItem[typeAr.length];
        for (int i =0;i<typeAr.length;i++){
            tditems[i] = new TDItem(typeAr[i],fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.tditems = new TDItem[typeAr.length];
        for (int i =0;i<typeAr.length;i++){
            tditems[i] = new TDItem(typeAr[i],"unnamed");
        }
    }
    
    public TupleDesc(TDItem[] tditems){
        this.tditems =tditems;
    }
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tditems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return tditems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tditems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i =0;i<tditems.length;i++){
            if (tditems[i].fieldName.equals(name)) return i;
        }
        throw new NoSuchElementException("can't find such fieldname in tupledesc");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i =0;i<tditems.length;i++){
            size = size + tditems[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TDItem[] newtditems = new TDItem[td1.tditems.length+td2.tditems.length];
        for (int i = 0;i<td1.tditems.length;i++){
            newtditems[i] = td1.tditems[i];
        }
        for (int i = 0;i<td2.tditems.length;i++){
            newtditems[i+td1.tditems.length] = td2.tditems[i];
        }
        return new TupleDesc(newtditems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (o==null) return false;
        else if (getClass()!=o.getClass()) return false;
        TupleDesc other = (TupleDesc) o;
        if (tditems.length!=other.tditems.length) return false;
        else {
            for (int i = 0;i<tditems.length;i++){
                if (!tditems[i].equals(other.tditems[i])) return false;
            }
            return true;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder str = new StringBuilder();
        for (TDItem tditem : tditems){
            str.append(tditem.toString());
        }
        return String.join(",",str);
    }
}
