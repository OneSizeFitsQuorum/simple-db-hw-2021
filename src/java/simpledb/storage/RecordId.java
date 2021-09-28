package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a specific table.
 */
public class RecordId implements Serializable {

  private static final long serialVersionUID = 1L;

  private final PageId pid;
  private final int tupleNumber;

  /**
   * Creates a new RecordId referring to the specified PageId and tuple number.
   *
   * @param pid         the pageid of the page on which the tuple resides
   * @param tupleNumber the tuple number within the page.
   */
  public RecordId(PageId pid, int tupleNumber) {
    this.pid = pid;
    this.tupleNumber = tupleNumber;
  }

  /**
   * @return the tuple number this RecordId references.
   */
  public int getTupleNumber() {
    return tupleNumber;
  }

  /**
   * @return the page id this RecordId references.
   */
  public PageId getPageId() {
    return pid;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordId recordId = (RecordId) o;
    return tupleNumber == recordId.tupleNumber && Objects.equals(pid, recordId.pid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pid, tupleNumber);
  }
}
