package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {

  private final int tableId;
  private final int pageNumber;

  /**
   * Constructor. Create a page id structure for a specific page of a specific table.
   *
   * @param tableId    The table that is being referenced
   * @param pageNumber The page number in that table.
   */
  public HeapPageId(int tableId, int pageNumber) {
    this.tableId = tableId;
    this.pageNumber = pageNumber;
  }

  /**
   * @return the table associated with this PageId
   */
  public int getTableId() {
    return tableId;
  }

  /**
   * @return the page number in the table getTableId() associated with this PageId
   */
  public int getPageNumber() {
    return pageNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HeapPageId that = (HeapPageId) o;
    return tableId == that.tableId && pageNumber == that.pageNumber;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, pageNumber);
  }

  /**
   * Return a representation of this object as an array of integers, for writing to disk.  Size of
   * returned array must contain number of integers that corresponds to number of args to one of the
   * constructors.
   */
  public int[] serialize() {
    int[] data = new int[2];

    data[0] = getTableId();
    data[1] = getPageNumber();

    return data;
  }

}
