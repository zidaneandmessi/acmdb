package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
    }

    
    private TDItem[] tdAr;

    public class ArrayIterator<E> implements Iterator<E> {

        public E[] obj;
        public int len;
        public int index;

        public ArrayIterator(E[] a) {
            this.obj = a;
            this.len = a.length;
            this.index = 0;
        }

        public boolean hasNext() {
            return index < len && obj[index] != null;
        }

        public E next() {
            return obj[index++];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new ArrayIterator<TDItem>(tdAr);
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
    	tdAr = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++)
    		tdAr[i] = new TDItem(typeAr[i], fieldAr[i]);
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
    	tdAr = new TDItem[typeAr.length];
    	for (int i = 0; i < typeAr.length; i++)
    		tdAr[i] = new TDItem(typeAr[i], "");
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdAr.length;
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
    	if (i < 0 || i >= tdAr.length)
    		throw new NoSuchElementException();
        return tdAr[i].fieldName;
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
    	if (i < 0 || i >= tdAr.length)
    		throw new NoSuchElementException();
        return tdAr[i].fieldType;
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
    	for (int i = 0; i < tdAr.length; i++)
    		if (tdAr[i].fieldName.equals(name))
    			return i;
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int s = 0;
        for (int i = 0; i < tdAr.length; i++)
        	s += tdAr[i].fieldType.getLen();
        return s;
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
    	int n1 = td1.numFields(), n2 = td2.numFields();
    	Type[] typeAr = new Type[n1 + n2];
    	String[] fieldAr = new String[n1 + n2];
    	for (int i = 0; i < n1; i++) {
    		typeAr[i] = td1.getFieldType(i);
    		fieldAr[i] = td1.getFieldName(i);
    	}
    	for (int i = 0; i < n2; i++) {
    		typeAr[i + n1] = td2.getFieldType(i);
    		fieldAr[i + n1] = td2.getFieldName(i);
    	}
        return new TupleDesc(typeAr, fieldAr);
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
    	if (!(o instanceof TupleDesc))
    		return false;
    	TupleDesc td = (TupleDesc)o;
    	if (td.getSize() != this.getSize())
    		return false;
    	if (td.numFields() != tdAr.length)
    		return false;
    	for (int i = 0; i < tdAr.length; i++)
    		if (td.getFieldType(i) != tdAr[i].fieldType)
    			return false;
    	return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        String S = toString();
        int h = 0;
        for (int i = 0; i < S.length(); i++)
            h = h * 31 + (int)(S.charAt(i));
        return h;
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
        String S = "";
        for (int i = 0 ; i < tdAr.length - 1; i++)
        	S += tdAr[i].toString() + ",";
        if (tdAr.length != 0)
        	S += tdAr[tdAr.length - 1].toString();
        return S;
    }
}
