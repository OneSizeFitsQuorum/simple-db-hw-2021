package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

  private static final long serialVersionUID = 1L;

  private final JoinPredicate p;
  private final TupleDesc desc;
  private OpIterator child1;
  private OpIterator child2;
  private Tuple cur;

  /**
   * Constructor. Accepts two children to join and the predicate to join them on
   *
   * @param p      The predicate to use to join the children
   * @param child1 Iterator for the left(outer) relation to join
   * @param child2 Iterator for the right(inner) relation to join
   */
  public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
    this.p = p;
    this.child1 = child1;
    this.child2 = child2;
    this.desc = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
  }

  public JoinPredicate getJoinPredicate() {
    return p;
  }

  /**
   * @return the field name of join field1. Should be quantified by alias or table name.
   */
  public String getJoinField1Name() {
    return child1.getTupleDesc().getFieldName(p.getField1());
  }

  /**
   * @return the field name of join field2. Should be quantified by alias or table name.
   */
  public String getJoinField2Name() {
    return child2.getTupleDesc().getFieldName(p.getField2());
  }

  /**
   * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible implementation logic.
   */
  public TupleDesc getTupleDesc() {
    return desc;
  }

  public void open() throws DbException, NoSuchElementException,
      TransactionAbortedException {
    super.open();
    child1.open();
    child2.open();
    cur = child1.hasNext() ? child1.next() : null;
  }

  public void close() {
    super.close();
    child1.close();
    child2.close();
    cur = null;
  }

  public void rewind() throws DbException, TransactionAbortedException {
    close();
    open();
  }

  /**
   * Returns the next tuple generated by the join, or null if there are no more tuples. Logically,
   * this is the next tuple in r1 cross r2 that satisfies the join predicate. There are many
   * possible implementations; the simplest is a nested loops join.
   * <p>
   * Note that the tuples returned from this particular implementation of Join are simply the
   * concatenation of joining tuples from the left and right relation. Therefore, if an equality
   * predicate is used there will be two copies of the join attribute in the results. (Removing such
   * duplicate columns can be done with an additional projection operator if needed.)
   * <p>
   * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6}, joined on equality of the
   * first column, then this returns {1,2,3,1,5,6}.
   *
   * @return The next matching tuple.
   * @see JoinPredicate#filter
   */
  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    while (cur != null) {
      Tuple right = getRightTuple();
      if (right != null) {
        return buildResult(right);
      }
      cur = child1.hasNext() ? child1.next() : null;
      child2.rewind();
    }
    return null;
  }

  private Tuple getRightTuple() throws TransactionAbortedException, DbException {
    while (child2.hasNext()) {
      Tuple tuple = child2.next();
      if (p.filter(cur, tuple)) {
        return tuple;
      }
    }
    return null;
  }

  private Tuple buildResult(Tuple right) {
    Tuple tuple = new Tuple(getTupleDesc());
    int leftTupleSize = cur.getTupleDesc().numFields();
    for (int i = 0; i < leftTupleSize; i++) {
      tuple.setField(i, cur.getField(i));
    }
    int rightTupleSize = right.getTupleDesc().numFields();
    for (int i = 0; i < rightTupleSize; i++) {
      tuple.setField(i + leftTupleSize, right.getField(i));
    }
    return tuple;
  }

  @Override
  public OpIterator[] getChildren() {
    return new OpIterator[]{child1, child2};
  }

  @Override
  public void setChildren(OpIterator[] children) {
    this.child1 = children[0];
    this.child2 = children[1];
  }

}
