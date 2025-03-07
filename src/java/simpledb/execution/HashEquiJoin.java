package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

  private static final long serialVersionUID = 1L;
  private final JoinPredicate pred;
  private OpIterator child1, child2;
  private final TupleDesc comboTD;
  transient private Tuple t1 = null;
  transient private Tuple t2 = null;

  /**
   * Constructor. Accepts to children to join and the predicate to join them on
   *
   * @param p      The predicate to use to join the children
   * @param child1 Iterator for the left(outer) relation to join
   * @param child2 Iterator for the right(inner) relation to join
   */
  public HashEquiJoin(JoinPredicate p, OpIterator child1, OpIterator child2) {
    this.pred = p;
    this.child1 = child1;
    this.child2 = child2;
    comboTD = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
  }

  public JoinPredicate getJoinPredicate() {
    return pred;
  }

  public TupleDesc getTupleDesc() {
    return comboTD;
  }

  public String getJoinField1Name() {
    return this.child1.getTupleDesc().getFieldName(this.pred.getField1());
  }

  public String getJoinField2Name() {
    return this.child2.getTupleDesc().getFieldName(this.pred.getField2());
  }

  final Map<Object, List<Tuple>> map = new HashMap<>();
  public final static int MAP_SIZE = 20000;

  private boolean loadMap() throws DbException, TransactionAbortedException {
    int cnt = 0;
    map.clear();
    while (child1.hasNext()) {
      t1 = child1.next();
      List<Tuple> list = map.computeIfAbsent(t1.getField(pred.getField1()), k -> new ArrayList<>());
      list.add(t1);
      if (cnt++ == MAP_SIZE) {
        return true;
      }
    }
    return cnt > 0;

  }

  public void open() throws DbException, NoSuchElementException,
      TransactionAbortedException {
    child1.open();
    child2.open();
    loadMap();
    super.open();
  }

  public void close() {
    super.close();
    child2.close();
    child1.close();
    this.t1 = null;
    this.t2 = null;
    this.listIt = null;
    this.map.clear();
  }

  public void rewind() throws DbException, TransactionAbortedException {
    child1.rewind();
    child2.rewind();
  }

  transient Iterator<Tuple> listIt = null;

  /**
   * Returns the next tuple generated by the join, or null if there are no more tuples. Logically,
   * this is the next tuple in r1 cross r2 that satisfies the join predicate. There are many
   * possible implementations; the simplest is a nested loops join.
   * <p>
   * Note that the tuples returned from this particular implementation of Join are simply the
   * concatenation of joining tuples from the left and right relation. Therefore, there will be two
   * copies of the join attribute in the results. (Removing such duplicate columns can be done with
   * an additional projection operator if needed.)
   * <p>
   * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6}, joined on equality of the
   * first column, then this returns {1,2,3,1,5,6}.
   *
   * @return The next matching tuple.
   * @see JoinPredicate#filter
   */
  private Tuple processList() {
    t1 = listIt.next();

    int td1n = t1.getTupleDesc().numFields();
    int td2n = t2.getTupleDesc().numFields();

    // set fields in combined tuple
    Tuple t = new Tuple(comboTD);
    for (int i = 0; i < td1n; i++) {
      t.setField(i, t1.getField(i));
    }
    for (int i = 0; i < td2n; i++) {
      t.setField(td1n + i, t2.getField(i));
    }
    return t;

  }

  protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    if (listIt != null && listIt.hasNext()) {
      return processList();
    }

    // loop around child2
    while (child2.hasNext()) {
      t2 = child2.next();

      // if match, create a combined tuple and fill it with the values
      // from both tuples
      List<Tuple> l = map.get(t2.getField(pred.getField2()));
      if (l == null) {
        continue;
      }
      listIt = l.iterator();

      return processList();

    }

    // child2 is done: advance child1
    child2.rewind();
    if (loadMap()) {
      return fetchNext();
    }

    return null;
  }

  @Override
  public OpIterator[] getChildren() {
    return new OpIterator[]{this.child1, this.child2};
  }

  @Override
  public void setChildren(OpIterator[] children) {
    this.child1 = children[0];
    this.child2 = children[1];
  }

}
