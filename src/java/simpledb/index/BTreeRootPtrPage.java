package simpledb.index;

import simpledb.common.DbException;
import simpledb.storage.BufferPool;
import simpledb.storage.Page;
import simpledb.transaction.TransactionId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * BTreeRootPtrPage stores the pointer to the root node used in the B+ tree and implements Page
 * Interface that is used by BufferPool
 *
 * @see BufferPool
 */
public class BTreeRootPtrPage implements Page {

  // size of this page
  public final static int PAGE_SIZE = 9;

  private boolean dirty = false;
  private TransactionId dirtier = null;

  private final BTreePageId pid;

  private int root;
  private int rootCategory;
  private int header;

  private byte[] oldData;

  /**
   * Constructor. Construct the BTreeRootPtrPage from a set of bytes of data read from disk. The
   * format of an BTreeRootPtrPage is an integer for the page number of the root node, followed by a
   * byte to encode the category of the root page (either leaf or internal), followed by an integer
   * for the page number of the first header page
   */
  public BTreeRootPtrPage(BTreePageId id, byte[] data) throws IOException {
    this.pid = id;
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

    // read in the root pointer
    root = dis.readInt();
    rootCategory = dis.readByte();

    // read in the header pointer
    header = dis.readInt();

    setBeforeImage();
  }

  public void setBeforeImage() {
    oldData = getPageData().clone();
  }

  /**
   * @return the PageId associated with this page.
   */
  public BTreePageId getId() {
    return pid;
  }

  /**
   * There is only one instance of a BTreeRootPtrPage per table. This static method is separate from
   * getId() in order to maintain the Page interface
   *
   * @param tableid - the tableid of this table
   * @return the root pointer page id for the given table
   */
  public static BTreePageId getId(int tableid) {
    return new BTreePageId(tableid, 0, BTreePageId.ROOT_PTR);
  }

  /**
   * Generates a byte array representing the contents of this root pointer page. Used to serialize
   * this root pointer page to disk. The invariant here is that it should be possible to pass the
   * byte array generated by getPageData to the BTreeRootPtrPage constructor and have it produce an
   * identical BTreeRootPtrPage object.
   *
   * @return A byte array corresponding to the bytes of this root pointer page.
   */
  public byte[] getPageData() {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(PAGE_SIZE);
    DataOutputStream dos = new DataOutputStream(baos);

    // write out the root pointer (page number of the root page)
    try {
      dos.writeInt(root);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // write out the category of the root page (leaf or internal)
    try {
      dos.writeByte((byte) rootCategory);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // write out the header pointer (page number of the first header page)
    try {
      dos.writeInt(header);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      dos.flush();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return baos.toByteArray();
  }

  /**
   * Static method to generate a byte array corresponding to an empty BTreeRootPtrPage. Used to add
   * new, empty pages to the file. Passing the results of this method to the BTreeRootPtrPage
   * constructor will create a BTreeRootPtrPage with no valid entries in it.
   *
   * @return The returned ByteArray.
   */
  public static byte[] createEmptyPageData() {
    return new byte[PAGE_SIZE]; //all 0
  }

  public void markDirty(boolean dirty, TransactionId tid) {
    this.dirty = dirty;
    if (dirty) {
      this.dirtier = tid;
    }
  }

  public TransactionId isDirty() {
    if (this.dirty) {
      return this.dirtier;
    } else {
      return null;
    }
  }

  /**
   * Return a view of this page before it was modified -- used by recovery
   */
  public BTreeRootPtrPage getBeforeImage() {
    try {
      return new BTreeRootPtrPage(pid, oldData);
    } catch (IOException e) {
      e.printStackTrace();
      //should never happen -- we parsed it OK before!
      System.exit(1);
    }
    return null;
  }

  /**
   * Get the id of the root page in this B+ tree
   *
   * @return the id of the root page
   */
  public BTreePageId getRootId() {
    if (root == 0) {
      return null;
    }
    return new BTreePageId(pid.getTableId(), root, rootCategory);
  }

  /**
   * Set the id of the root page in this B+ tree
   *
   * @param id - the id of the root page
   * @throws DbException if the id is invalid
   */
  public void setRootId(BTreePageId id) throws DbException {
    if (id == null) {
      root = 0;
    } else {
      if (id.getTableId() != pid.getTableId()) {
        throw new DbException("table id mismatch in setRootId");
      }
      if (id.pgcateg() != BTreePageId.INTERNAL && id.pgcateg() != BTreePageId.LEAF) {
        throw new DbException("root must be an internal node or leaf node");
      }
      root = id.getPageNumber();
      rootCategory = id.pgcateg();
    }
  }

  /**
   * Get the id of the first header page, or null if none exists
   *
   * @return the id of the first header page
   */
  public BTreePageId getHeaderId() {
    if (header == 0) {
      return null;
    }
    return new BTreePageId(pid.getTableId(), header, BTreePageId.HEADER);
  }

  /**
   * Set the page id of the first header page
   *
   * @param id - the id of the first header page
   * @throws DbException if the id is invalid
   */
  public void setHeaderId(BTreePageId id) throws DbException {
    if (id == null) {
      header = 0;
    } else {
      if (id.getTableId() != pid.getTableId()) {
        throw new DbException("table id mismatch in setHeaderId");
      }
      if (id.pgcateg() != BTreePageId.HEADER) {
        throw new DbException("header must be of type BTreePageId.HEADER");
      }
      header = id.getPageNumber();
    }
  }

  /**
   * Get the page size of root pointer pages
   *
   * @return the page size
   */
  public static int getPageSize() {
    return PAGE_SIZE;
  }

}

