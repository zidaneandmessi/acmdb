package simpledb;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gfield;
    private Type gfieldtype;
    private int afield;
    private Op aop;
    private Tuple[] result;
    private int[] resultSum;
    private int[] resultCount;
    private int resultLen;

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
        this.gfield = gbfield;
        this.gfieldtype = gbfieldtype;
        this.afield = afield;
        this.aop = what;
        this.result = new Tuple[10001];
        this.resultSum = new int[10001];
        this.resultCount = new int[10001];
        this.resultLen = 0;
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
        switch (aop) {
            case MIN:
                if (gfield == Aggregator.NO_GROUPING) {
                    if (resultLen == 0) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(afield));
                        result[resultLen++] = t;
                    }
                    else {
                        if (tup.getField(afield).compare(Predicate.Op.LESS_THAN, result[0].getField(0))) {
                            TupleDesc child_td = tup.getTupleDesc();
                            Type[] typeAr = new Type[1];
                            String[] fieldAr = new String[1];
                            typeAr[0] = Type.INT_TYPE;
                            fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                            TupleDesc td = new TupleDesc(typeAr, fieldAr);
                            Tuple t = new Tuple(td);
                            t.setField(0, tup.getField(afield));
                            result[0] = t;
                        }
                    }
                }
                else {
                    int find = -1;
                    for (int i = 0; i < resultLen; i++)
                        if (tup.getField(gfield).compare(Predicate.Op.EQUALS, result[i].getField(0))) {
                            find = i;
                            break;
                        }
                    if (find == -1) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, tup.getField(afield));
                        result[resultLen++] = t;
                    }
                    else {
                        if (tup.getField(afield).compare(Predicate.Op.LESS_THAN, result[find].getField(1))) {
                            TupleDesc child_td = tup.getTupleDesc();
                            Type[] typeAr = new Type[2];
                            String[] fieldAr = new String[2];
                            typeAr[0] = gfieldtype;
                            fieldAr[0] = child_td.getFieldName(gfield);
                            typeAr[1] = Type.INT_TYPE;
                            fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                            TupleDesc td = new TupleDesc(typeAr, fieldAr);
                            Tuple t = new Tuple(td);
                            t.setField(0, tup.getField(gfield));
                            t.setField(1, tup.getField(afield));
                            result[find] = t;
                        }
                    }
                }
                break;
                case MAX:
                if (gfield == Aggregator.NO_GROUPING) {
                    if (resultLen == 0) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(afield));
                        result[resultLen++] = t;
                    }
                    else {
                        if (tup.getField(afield).compare(Predicate.Op.GREATER_THAN, result[0].getField(0))) {
                            TupleDesc child_td = tup.getTupleDesc();
                            Type[] typeAr = new Type[1];
                            String[] fieldAr = new String[1];
                            typeAr[0] = Type.INT_TYPE;
                            fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                            TupleDesc td = new TupleDesc(typeAr, fieldAr);
                            Tuple t = new Tuple(td);
                            t.setField(0, tup.getField(afield));
                            result[0] = t;
                        }
                    }
                }
                else {
                    int find = -1;
                    for (int i = 0; i < resultLen; i++)
                        if (tup.getField(gfield).compare(Predicate.Op.EQUALS, result[i].getField(0))) {
                            find = i;
                            break;
                        }
                    if (find == -1) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, tup.getField(afield));
                        result[resultLen++] = t;
                    }
                    else {
                        if (tup.getField(afield).compare(Predicate.Op.GREATER_THAN, result[find].getField(1))) {
                            TupleDesc child_td = tup.getTupleDesc();
                            Type[] typeAr = new Type[2];
                            String[] fieldAr = new String[2];
                            typeAr[0] = gfieldtype;
                            fieldAr[0] = child_td.getFieldName(gfield);
                            typeAr[1] = Type.INT_TYPE;
                            fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                            TupleDesc td = new TupleDesc(typeAr, fieldAr);
                            Tuple t = new Tuple(td);
                            t.setField(0, tup.getField(gfield));
                            t.setField(1, tup.getField(afield));
                            result[find] = t;
                        }
                    }

                }
                break;
                case SUM:
                if (gfield == Aggregator.NO_GROUPING) {
                    if (resultLen == 0) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(afield));
                        result[resultLen++] = t;
                    }
                    else {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, new IntField(((IntField)(result[0].getField(0))).getValue() + ((IntField)(tup.getField(afield))).getValue()));
                        result[0] = t;
                    }
                }
                else {
                    int find = -1;
                    for (int i = 0; i < resultLen; i++)
                        if (tup.getField(gfield).compare(Predicate.Op.EQUALS, result[i].getField(0))) {
                            find = i;
                            break;
                        }
                    if (find == -1) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, tup.getField(afield));
                        result[resultLen++] = t;
                    }
                    else {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, new IntField(((IntField)(result[find].getField(1))).getValue() + ((IntField)(tup.getField(afield))).getValue()));
                        result[find] = t;
                    }

                }
                break;
                case AVG:
                if (gfield == Aggregator.NO_GROUPING) {
                    if (resultLen == 0) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(afield));
                        resultSum[resultLen] = ((IntField)tup.getField(afield)).getValue();
                        resultCount[resultLen] = 1;
                        result[resultLen++] = t;
                    }
                    else {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        resultSum[0] += ((IntField)tup.getField(afield)).getValue();
                        resultCount[0]++;
                        t.setField(0, new IntField(resultSum[0] / resultCount[0]));
                        result[0] = t;
                    }
                }
                else {
                    int find = -1;
                    for (int i = 0; i < resultLen; i++)
                        if (tup.getField(gfield).compare(Predicate.Op.EQUALS, result[i].getField(0))) {
                            find = i;
                            break;
                        }
                    if (find == -1) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, tup.getField(afield));
                        resultSum[resultLen] = ((IntField)tup.getField(afield)).getValue();
                        resultCount[resultLen] = 1;
                        result[resultLen++] = t;
                    }
                    else {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        resultSum[find] += ((IntField)tup.getField(afield)).getValue();
                        resultCount[find]++;
                        t.setField(1, new IntField(resultSum[find] / resultCount[find]));
                        result[find] = t;
                    }
                }
                break;
                case COUNT:
                if (gfield == Aggregator.NO_GROUPING) {
                    if (resultLen == 0) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, new IntField(1));
                        result[resultLen++] = t;
                    }
                    else {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[1];
                        String[] fieldAr = new String[1];
                        typeAr[0] = Type.INT_TYPE;
                        fieldAr[0] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, new IntField(((IntField)(result[0].getField(0))).getValue() + 1));
                        result[0] = t;
                    }
                }
                else {
                    int find = -1;
                    for (int i = 0; i < resultLen; i++)
                        if (tup.getField(gfield).compare(Predicate.Op.EQUALS, result[i].getField(0))) {
                            find = i;
                            break;
                        }
                    if (find == -1) {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, new IntField(1));
                        result[resultLen++] = t;
                    }
                    else {
                        TupleDesc child_td = tup.getTupleDesc();
                        Type[] typeAr = new Type[2];
                        String[] fieldAr = new String[2];
                        typeAr[0] = gfieldtype;
                        fieldAr[0] = child_td.getFieldName(gfield);
                        typeAr[1] = Type.INT_TYPE;
                        fieldAr[1] = child_td.getFieldName(afield) + "(" + aop + ")";
                        TupleDesc td = new TupleDesc(typeAr, fieldAr);
                        Tuple t = new Tuple(td);
                        t.setField(0, tup.getField(gfield));
                        t.setField(1, new IntField(((IntField)(result[find].getField(1))).getValue() + 1));
                        result[find] = t;
                    }

                }
                break;
        }
    }

    public class ArrayIterator implements DbIterator {

        public Tuple[] obj;
        public int len;
        public int index;
        public boolean open;

        public ArrayIterator(Tuple[] a, int len) {
            this.obj = a;
            this.len = len;
            this.index = 0;
            this.open = false;
        }

        public boolean hasNext() {
            if (!open)
                return false;
            return index < len && obj[index] != null;
        }

        public void open() {
            open = true;
        }

        public Tuple next() {
            if (!open)
                return null;
            if (!hasNext())
                throw new NoSuchElementException();
            return obj[index++];
        }

        public void rewind() {
            index = 0;
        }

        public TupleDesc getTupleDesc() {
            if (!open)
                return null;
            return obj[index].getTupleDesc();
        }

        public void close() {
            open = false;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new ArrayIterator(result, resultLen);
    }

}
