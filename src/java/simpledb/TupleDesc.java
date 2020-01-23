package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A list of fields that describe a tuple
     */
    private List<TDItem> tupleFields;

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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return this.tupleFields.iterator();
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
        this.tupleFields = new ArrayList<TDItem>();
        for (int i=0; i < typeAr.length; i++) {
            TDItem tdi = new TDItem(typeAr[i], fieldAr[i]);
            this.tupleFields.add(tdi);
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
        this.tupleFields = new ArrayList<TDItem>();
        for (int i=0; i < typeAr.length; i++) {
            TDItem tdi = new TDItem(typeAr[i], null);
            this.tupleFields.add(tdi);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.tupleFields.size();
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
        if ((i < 0) || (i > this.tupleFields.size() + 1)) {
            throw new NoSuchElementException(String.format("index i: %d not valid in tuple fields list of size: %d", i, this.tupleFields.size()));
        } 

        TDItem field = this.tupleFields.get(i);
        return field.fieldName;
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
        if ((i < 0) || (i > this.tupleFields.size() + 1)) {
            throw new NoSuchElementException(String.format("index i: %d not valid in tuple fields list of size: %d", i, this.tupleFields.size()));
        }
        
        TDItem field = this.tupleFields.get(i);
        return field.fieldType;
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
        if (name == null) {
            throw new NoSuchElementException("the field searched is null, not allowed");
        }
        
        for (int i=0; i < this.tupleFields.size(); i++) {
            TDItem currField = this.tupleFields.get(i);
            if (currField.fieldName == null) {
                continue;
            }

            if (currField.fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("did not find a field in this tuple with name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int bytes = 0;
        for (int i=0; i < this.numFields(); i++) {
            // System.out.println("curr #bytes: " + bytes);
            TDItem currField = this.tupleFields.get(i);
            switch (currField.fieldType) {
                case INT_TYPE:
                    bytes += Type.INT_TYPE.getLen();
                    break;
                case STRING_TYPE:
                    bytes += Type.STRING_TYPE.getLen();
                    break;
                default:
                    System.out.println("[warning] unknown type in tupleFields");
                    break;
            }
        }
        return bytes;
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
        int mergedSize = td1.numFields() + td2.numFields();
        Type tupleTypes[] = new Type[mergedSize];
        String tupleFields[] = new String[mergedSize];
        Iterator<TDItem> tupItrOne = td1.iterator();
        TDItem tupField;
        int i = 0;
        while (tupItrOne.hasNext()) {
            tupField = tupItrOne.next();
            tupleTypes[i] = tupField.fieldType;
            tupleFields[i] = tupField.fieldName;
            i++;
        }
        Iterator<TDItem> tupItrTwo = td2.iterator();
        while (tupItrTwo.hasNext()) {
            tupField = tupItrTwo.next();
            tupleTypes[i] = tupField.fieldType;
            tupleFields[i] = tupField.fieldName;
            i++;
        }
        TupleDesc tupDesc = new TupleDesc(tupleTypes, tupleFields);
        return tupDesc;
    }   

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (!(o instanceof TupleDesc) || (o == null)) {
            return false;
        }

        // check if the sizes match up
        TupleDesc otherDesc = (TupleDesc) o;
        if (!(this.numFields() == otherDesc.numFields())) {
            return false;
        }

        // check the types for equality
        Iterator<TDItem> otherDescItr = otherDesc.iterator();
        int i = 0;
        TDItem otherDescField;
        Type thisDescFieldType;
        while (otherDescItr.hasNext()) {
            otherDescField = otherDescItr.next();
            thisDescFieldType = this.getFieldType(i);
            if (!(otherDescField.fieldType == thisDescFieldType)) {
                return false;
            }
            i++;
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int hash = 7;
        hash = 31 * hash + this.numFields();
        int fieldTypeHashSum = 0;
        int fieldNameHashSum = 0;
        for (int i=0; i < this.numFields(); i++) {
            TDItem tupField = this.tupleFields.get(i);
            fieldTypeHashSum += tupField.fieldType.hashCode();
            fieldNameHashSum += null == tupField.fieldName ? 0 : tupField.fieldName.hashCode();
        }
        hash = 31 * hash + fieldTypeHashSum;
        hash = 31 * hash + fieldNameHashSum;
        return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String tupleDescDump = "";
        for (int i=0; i < this.numFields(); i++) {
            TDItem tupField = this.tupleFields.get(i);
            switch (tupField.fieldType) {
                case INT_TYPE:
                    tupleDescDump += "INT(" + tupField.fieldName;
                    break;
                case STRING_TYPE:
                    tupleDescDump += "STRING(" + tupField.fieldName;
                    break;
                default:
                    tupleDescDump += "UNKNOWN TYPE( " + tupField.fieldName;
            }
            if (i == this.numFields() - 1) {
                tupleDescDump += ")";
            } else {
                tupleDescDump += "), ";
            }
        }
        return tupleDescDump;
    }
}
