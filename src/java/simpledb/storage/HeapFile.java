package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples in no particular
 * order. Tuples are stored on pages, each of which is a fixed size, and the file is simply a
 * collection of those pages. HeapFile works closely with HeapPage. The format of HeapPages is
 * described in the HeapPage constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

  private final File f;
  private final TupleDesc td;

  /**
   * Constructs a heap file backed by the specified file.
   *
   * @param f the file that stores the on-disk backing store for this heap file.
   */
  public HeapFile(File f, TupleDesc td) {
    this.f = f;
    this.td = td;
  }

  /**
   * Returns the File backing this HeapFile on disk.
   *
   * @return the File backing this HeapFile on disk.
   */
  public File getFile() {
    return this.f;
  }

  /**
   * Returns an ID uniquely identifying this HeapFile. Implementation note: you will need to
   * generate this tableid somewhere to ensure that each HeapFile has a "unique id," and that you
   * always return the same value for a particular HeapFile. We suggest hashing the absolute file
   * name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
   *
   * @return an ID uniquely identifying this HeapFile.
   */
  public int getId() {
    return this.f.getAbsoluteFile().hashCode();
  }

  /**
   * Returns the TupleDesc of the table stored in this DbFile.
   *
   * @return TupleDesc of this DbFile.
   */
  public TupleDesc getTupleDesc() {
    return this.td;
  }

  // see DbFile.java for javadocs
  public Page readPage(PageId pid) {
    try (RandomAccessFile file = new RandomAccessFile(f, "r")) {
      file.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
      byte[] result = new byte[BufferPool.getPageSize()];
      file.read(result);
      return new HeapPage((HeapPageId) pid, result);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  // see DbFile.java for javadocs
  public void writePage(Page page) throws IOException {
    // some code goes here
    // not necessary for lab1
  }

  /**
   * Returns the number of pages in this HeapFile.
   */
  public int numPages() {
    return (int) (f.length() / BufferPool.getPageSize());
  }

  // see DbFile.java for javadocs
  public List<Page> insertTuple(TransactionId tid, Tuple t)
      throws DbException, IOException, TransactionAbortedException {
    // some code goes here
    return null;
    // not necessary for lab1
  }

  // see DbFile.java for javadocs
  public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
      TransactionAbortedException {
    // some code goes here
    return null;
    // not necessary for lab1
  }

  // see DbFile.java for javadocs
  public DbFileIterator iterator(TransactionId tid) {
    return new HeapIterator(this, tid);
  }
}

class HeapIterator extends AbstractDbFileIterator {

  private Iterator<Tuple> it = null;
  private HeapPageId curPageId = null;

  private final HeapFile f;
  private final TransactionId tid;

  public HeapIterator(HeapFile f, TransactionId tid) {
    this.f = f;
    this.tid = tid;
  }

  public void open() throws DbException, TransactionAbortedException {
    curPageId = new HeapPageId(f.getId(), 0);
    it = ((HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY))
        .iterator();
  }

  protected Tuple readNext() throws TransactionAbortedException, DbException,
      NoSuchElementException {
    if (it != null && !it.hasNext()) {
      it = null;
    }

    while (it == null && curPageId != null) {
      if (curPageId.getPageNumber() + 1 >= f.numPages()) {
        curPageId = null;
      } else {
        curPageId = new HeapPageId(f.getId(), curPageId.getPageNumber() + 1);
        it = ((HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY))
            .iterator();
        if (!it.hasNext()) {
          it = null;
        }
      }
    }

    if (it == null) {
      return null;
    }
    return it.next();
  }

  /**
   * rewind this iterator back to the beginning of the tuples
   */
  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /**
   * close the iterator
   */
  public void close() {
    super.close();
    it = null;
    curPageId = null;
  }
}

