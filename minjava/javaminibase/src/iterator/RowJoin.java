package iterator;

import BigT.Map;
import BigT.Stream;
import bufmgr.PageNotReadException;
import global.*;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import index.IndexException;
import global.GlobalConst;

import java.io.IOException;

/**
 *
 * This file contains an implementation of the nested loops join algorithm as
 * described in the Shapiro paper. The algorithm is extremely simple:
 *
 * foreach tuple r in R do foreach tuple s in S do if (ri == sj) then add (r, s)
 * to the result.
 */

public class RowJoin extends Iterator {
  private AttrType _in1[], _in2[];
  private int in1_len, in2_len;
  private Iterator outer;
  private short t2_str_sizescopy[];
  private CondExpr OutputFilter[];
  private CondExpr RightFilter[];
  private int n_buf_pgs; // # of buffer pages available.
  private boolean done, // Is the join complete
      get_from_outer; // if TRUE, a tuple is got from outer
  private Map outer_map, inner_map;
  private Map Jmap; // Joined tuple
  private FldSpec perm_mat[];
  private int nOutFlds;
  private Heapfile hf;
  private Scan inner;

  /**
   * constructor Initialize the two relations which are joined, including relation
   * type,
   *
//   * @param in1          Array containing field types of R.
//   * @param len_in1      # of columns in R.
//   * @param t1_str_sizes shows the length of the string fields.
//   * @param in2          Array containing field types of S
//   * @param len_in2      # of columns in S
//   * @param t2_str_sizes shows the length of the string fields.
//   * @param amt_of_mem   IN PAGES
//   * @param am1          access method for left i/p to join
//   * @param relationName access hfapfile for right i/p to join
//   * @param outFilter    select expressions
//   * @param rightFilter  reference to filter applied on right i/p
//   * @param proj_list    shows what input fields go where in the output tuple
//   * @param n_out_flds   number of outer relation fileds
   * @exception IOException         some I/O fault
   * @exception NestedLoopException exception from this class
   */
//  public RowJoin(AttrType in1[], int len_in1, short t1_str_sizes[], AttrType in2[], int len_in2,
//                 short t2_str_sizes[], int amt_of_mem, Iterator am1, String relationName, CondExpr outFilter[],
//                 CondExpr rightFilter[], FldSpec proj_list[], int n_out_flds) throws IOException, NestedLoopException {
    public RowJoin(int amtofmem, Stream LeftBigT, String RightBigTName, String ColumnName)
            throws IOException, NestedLoopException, iterator.FileScanException, iterator.MapUtilsException, iterator.InvalidRelation {
    //RowJoin(int amtofmem, Stream leftStream, String RightBigTName,attrString ColumnName
//    amt_of_mem - IN PAGES
//    leftStream - a stream for the left data source
//    RightBigTName - name of the BigTable at the right side of the join
//    ColumnName - condition to match the column labels
//      The output is a stream of maps corresponding to a BigT consist
//      ing of the maps of the matching rows based on
//      the given conditions, such that
//∗
//      Two rows match only if they have the same column and the most re
//      cent values
//      for the two columns are
//      the same.
//∗
//      The resulting rowlabel is the concatenation of the two input
//      rowlabels, seperated with a “:”
//∗
//      The resulting row has all the columnlabels of the two input ro
//      ws, except for the joined column which
//      occurs only once in the bigtable – and only with the most recen
//      t three values
//
    AttrType[] Schema = MapSchema.MapAttrType();
    int MapFldCount = MapSchema.MapFldCount();
    short[] MapStrLengths = MapSchema.MapStrLengths();
    FldSpec[] OutputSchema = MapSchema.OutputMapSchema();
    _in1 = Schema;
    _in2 = Schema;
    in1_len = MapFldCount;
    in2_len = MapFldCount;

    outer = LeftBigT.GetStreamIterator();
    t2_str_sizescopy = MapStrLengths;
    inner_map = new Map();
    Jmap = new Map();
    OutputFilter = QueryHelper.EqualityFilterByValue();
    RightFilter = QueryHelper.queryFilter("*", ColumnName, "*");

    n_buf_pgs = amtofmem;
    inner = null;
    done = false;
    get_from_outer = true;

    AttrType[] Jtypes = Schema;
    short[] t_size;

    perm_mat = OutputSchema;
    nOutFlds = MapFldCount;
    try {
      t_size = MapUtils.setup_op_map(Jmap, Jtypes, Schema, MapFldCount, Schema, MapFldCount, MapStrLengths, MapStrLengths,
              OutputSchema, nOutFlds);
    } catch (MapUtilsException e) {
      throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
    }

    try {
      hf = new Heapfile(RightBigTName);

    } catch (Exception e) {
      throw new NestedLoopException(e, "Create new heapfile failed.");
    }
  }

  /**
   * @return The joined tuple is returned
   * @exception IOException               I/O errors
   * @exception JoinsException            some join exception
   * @exception IndexException            exception from super class
   * @exception InvalidTupleSizeException invalid tuple size
   * @exception InvalidTypeException      tuple type not valid
   * @exception PageNotReadException      exception from lower layer
   * @exception TupleUtilsException       exception from using tuple utilities
   * @exception PredEvalException         exception from PredEval class
   * @exception SortException             sort exception
   * @exception LowMemException           memory error
   * @exception UnknowAttrType            attribute type unknown
   * @exception UnknownKeyTypeException   key type unknown
   * @exception Exception                 other exceptions
   *
   */
  public Map get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
      InvalidTypeException, PageNotReadException, MapUtilsException, PredEvalException, SortException,
      LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    // This is a DUMBEST form of a join, not making use of any key information...

    if (done)
      return null;
    outer_map = new Map(outer.get_next());
    System.out.println("****************************************");
    System.out.println("Entering for the first time with following tuple:");
    System.out.print("\t");
    outer_map.print(MapSchema.MapAttrType());
    do {
//      System.out.println("****************************************");
//      System.out.print("\t:");
//      outer_map.print(MapSchema.MapAttrType());

      // If get_from_outer is true, Get a tuple from the outer, delete
      // an existing scan on the file, and reopen a new scan on the file.
      // If a get_next on the outer returns DONE?, then the nested loops
      // join is done too.

      if (get_from_outer == true) {
        get_from_outer = false;
        if (inner != null) // If this not the first time,
        {
          // close scan
          inner = null;
        }

        try {
          inner = hf.openScan();
        } catch (Exception e) {
          throw new NestedLoopException(e, "openScan failed");
        }

        if (outer_map == null) {
          done = true;
          if (inner != null) {

            inner = null;
          }

          return null;
        }
      } // ENDS: if (get_from_outer == TRUE)

      // Left Table is ordered by RL CL TS
      // We check the latest value per RL-CL
      boolean eof = false;
      while(outer_map != null){
        Map peek = outer.get_next();
//        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++");
//        System.out.println("The outer map");
//        System.out.print("\t:");
//        outer_map.print(MapSchema.MapAttrType());
//        System.out.println("The peek map");
//        System.out.print("\t:");
//        if(peek != null){
//          peek.print(MapSchema.MapAttrType());
//        }
//        else{
//          System.out.println("no peek");
//        }
        if(peek != null){
          // If RL CL match, the current map is not the latest value TS
          if(MapUtils.CompareMapWithMap(new AttrType(AttrType.attrString), outer_map, GlobalConst.ROW_LABEL, peek, GlobalConst.ROW_LABEL) == 0 &&
                  MapUtils.CompareMapWithMap(new AttrType(AttrType.attrString), outer_map, GlobalConst.COL_LABEL, peek, GlobalConst.COL_LABEL) == 0){

            outer_map = new Map(outer.get_next());
          }
          else{
            outer_map = peek;
            break;
          }
        }
        else{
          eof = true;
          break;
        }
      }
      if(outer_map != null){
        // The next step is to get a tuple from the inner,
        // while the inner is not completely scanned && there
        // is no match (with pred),get a tuple from the inner.

        MID mid = new MID();
        while ((inner_map = inner.getNext(mid)) != null) {
          inner_map.setHdr((short) in2_len, _in2, t2_str_sizescopy);
          if (PredEval.Eval(RightFilter, inner_map, null, _in2, null) == true) {
            //inner_map.print(MapSchema.MapAttrType());
            System.out.println("inner eval");
            if (PredEval.Eval(OutputFilter, outer_map, inner_map, _in1, _in2) == true) {
              System.out.println("output filter satisfied");
              // Apply a projection on the outer and inner tuples.
              //System.out.println("OutputFilter satisfied");
              Projection.RowJoin(outer_map, _in1, inner_map, _in2, Jmap, perm_mat, nOutFlds);
              return Jmap;
            }
          }
        }

        // There has been no match. (otherwise, we would have
        // returned from t//he while loop. Hence, inner is
        // exhausted, => set get_from_outer = TRUE, go to top of loop
      }
      // if at last element, close
      if(eof)
        outer_map = null;
      get_from_outer = true; // Loop back to top and get next outer tuple.
    } while (true);
  }

  /**
   * implement the abstract method close() from super class Iterator to finish
   * cleaning up
   *
   * @exception IOException    I/O error from lower layers
   * @exception JoinsException join error from lower layers
   * @exception IndexException index access error
   */
  public void close() throws JoinsException, IOException, IndexException {
    if (!closeFlag) {

      try {
        outer.close();
      } catch (Exception e) {
        throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
      }
      closeFlag = true;
    }
  }
}
