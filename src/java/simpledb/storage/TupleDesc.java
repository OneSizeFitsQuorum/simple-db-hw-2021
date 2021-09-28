package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

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
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TDItem tdItem = (TDItem) o;
      return fieldType == tdItem.fieldType && Objects.equals(fieldName, tdItem.fieldName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldType, fieldName);
    }
  }

  /**
   * @return An iterator which iterates over all the field TDItems that are included in this
   * TupleDesc
   */
  public Iterator<TDItem> iterator() {
    return new ArrayList<>(Arrays.asList(items)).iterator();
  }

  private static final long serialVersionUID = 1L;

  private final TDItem[] items;

  /**
   * Create a new TupleDesc with typeAr.length fields with fields of the specified types, with
   * associated named fields.
   *
   * @param typeAr  array specifying the number of and types of fields in this TupleDesc. It must
   *                contain at least one entry.
   * @param fieldAr array specifying the names of the fields. Note that names may be null.
   */
  public TupleDesc(Type[] typeAr, String[] fieldAr) {
    items = new TDItem[typeAr.length];
    for (int i = 0; i < typeAr.length; i++) {
      items[i] = new TDItem(typeAr[i], fieldAr[i]);
    }
  }

  /**
   * Constructor. Create a new tuple desc with typeAr.length fields with fields of the specified
   * types, with anonymous (unnamed) fields.
   *
   * @param typeAr array specifying the number of and types of fields in this TupleDesc. It must
   *               contain at least one entry.
   */
  public TupleDesc(Type[] typeAr) {
    items = new TDItem[typeAr.length];
    for (int i = 0; i < typeAr.length; i++) {
      items[i] = new TDItem(typeAr[i], null);
    }
  }

  private TupleDesc(TDItem[] items) {
    this.items = items;
  }

  /**
   * @return the number of fields in this TupleDesc
   */
  public int numFields() {
    return items.length;
  }

  /**
   * Gets the (possibly null) field name of the ith field of this TupleDesc.
   *
   * @param i index of the field name to return. It must be a valid index.
   * @return the name of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public String getFieldName(int i) throws NoSuchElementException {
    if (i < 0 || i >= items.length) {
      throw new NoSuchElementException();
    }
    return items[i].fieldName;
  }

  /**
   * Gets the type of the ith field of this TupleDesc.
   *
   * @param i The index of the field to get the type of. It must be a valid index.
   * @return the type of the ith field
   * @throws NoSuchElementException if i is not a valid field reference.
   */
  public Type getFieldType(int i) throws NoSuchElementException {
    if (i < 0 || i >= items.length) {
      throw new NoSuchElementException();
    }
    return items[i].fieldType;
  }

  /**
   * Find the index of the field with a given name.
   *
   * @param name name of the field.
   * @return the index of the field that is first to have the given name.
   * @throws NoSuchElementException if no field with a matching name is found.
   */
  public int fieldNameToIndex(String name) throws NoSuchElementException {
    for (int i = 0; i < items.length; i++) {
      if (Objects.equals(items[i].fieldName, name)) {
        return i;
      }
    }
    throw new NoSuchElementException();
  }

  /**
   * @return The size (in bytes) of tuples corresponding to this TupleDesc. Note that tuples from a
   * given TupleDesc are of a fixed size.
   */
  public int getSize() {
    return Arrays.stream(items).mapToInt(item -> item.fieldType.getLen()).sum();
  }

  /**
   * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields, with the first
   * td1.numFields coming from td1 and the remaining from td2.
   *
   * @param td1 The TupleDesc with the first fields of the new TupleDesc
   * @param td2 The TupleDesc with the last fields of the TupleDesc
   * @return the new TupleDesc
   */
  public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    TDItem[] newItems = new TDItem[td1.numFields() + td2.numFields()];
    System.arraycopy(td1.items, 0, newItems, 0, td1.items.length);
    System.arraycopy(td2.items, 0, newItems, td1.items.length, td2.items.length);
    return new TupleDesc(newItems);
  }

  public TupleDesc deepCopyWithTableAlias(String alias) {
    TDItem[] newItems = new TDItem[items.length];
    for (int i = 0; i < items.length; i++) {
      newItems[i] = new TDItem(items[i].fieldType, alias + "." + items[i].fieldName);
    }
    return new TupleDesc(newItems);
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TupleDesc tupleDesc = (TupleDesc) o;
    return Arrays.equals(items, tupleDesc.items);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(items);
  }

  /**
   * Returns a String describing this descriptor. It should be of the form
   * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although the exact format does
   * not matter.
   *
   * @return String describing this descriptor.
   */
  public String toString() {
    return Arrays.stream(items).map(TDItem::toString).collect(Collectors.joining(","));
  }
}
