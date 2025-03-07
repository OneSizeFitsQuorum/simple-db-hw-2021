package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;
  private final int gbfield;
  private final Type gbfieldtype;
  private final int afield;
  private final Op what;
  private final HashMap<Field, AggregatorResult> values = new HashMap<>();

  /**
   * Aggregate constructor
   *
   * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if
   *                    there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no
   *                    grouping
   * @param afield      the 0-based index of the aggregate field in the tuple
   * @param what        the aggregation operator
   */

  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    this.gbfield = gbfield;
    this.gbfieldtype = gbfieldtype;
    this.afield = afield;
    this.what = what;
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   *
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  public void mergeTupleIntoGroup(Tuple tup) {

  }

  /**
   * Create a OpIterator over group aggregate results.
   *
   * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a
   * single (aggregateVal) if no grouping. The aggregateVal is determined by the type of aggregate
   * specified in the constructor.
   */
  public OpIterator iterator() {
    // some code goes here
    throw new
        UnsupportedOperationException("please implement me for lab2");
  }

}
