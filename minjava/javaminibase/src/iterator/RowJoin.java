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
import programs.Query;

import java.awt.desktop.SystemSleepEvent;
import java.io.File;
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
  private Map outer_map, inner_map, peek;
  private Map Jmap; // Joined tuple
  private FldSpec perm_mat[];
  private int nOutFlds;
  private Heapfile hf;
  private Scan inner;
  private String hfName;

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
      // Sort the Heapfile on RL CL TS by asc
      // This makes it easier to find the
      // latest value for each CL to determine
      // whether to compare val
      FileScan fscan = new FileScan(RightBigTName, Schema, MapStrLengths, (short) MapFldCount, MapFldCount, OutputSchema, RightFilter);
      Iterator sort = SortHelper.BuildSortOrder(fscan, Schema, MapStrLengths, OrderType.type1, amtofmem);
      this.hf = HeapHelper.BuildHeap(sort);
      hfName = "rowjoin";
      fscan.close();
      sort.close();
      //outer_map = new Map(outer.get_next());

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
  public Map get_next2() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
      InvalidTypeException, PageNotReadException, MapUtilsException, PredEvalException, SortException,
      LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    // This is a DUMBEST form of a join, not making use of any key information...

    if (done)
      return null;
    if(outer_map == null){
      outer_map = outer.get_next();
    }
    do {
      // If get_from_outer is true, Get a tuple from the outer, delete
      // an existing scan on the file, and reopen a new scan on the file.
      // If a get_next on the outer returns DONE?, then the nested loops
      // join is done too.

      if (get_from_outer == true) {
        get_from_outer = false;
        if (inner != null) // If this not the first time,
        {
          // close scan
          inner.closescan();
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
      //Map peek = null;
      //TODO
      //peek = null;
      findLatestMap(outer, eof);
//      while(outer_map != null){
//        peek = outer.get_next();
//        if(peek != null){
//          // If RL CL match, the current map is not the latest value TS
//          peek = new Map(peek.getMapByteArray(), 0, GlobalConst.MAP_LEN);
//          peek.setHdr((short) MapSchema.MapFldCount(),MapSchema.MapAttrType(), MapSchema.MapStrLengths());
//          if(MapUtils.CompareMapWithMap(new AttrType(AttrType.attrString), outer_map, GlobalConst.ROW_LABEL, peek, GlobalConst.ROW_LABEL) == 0 &&
//                  MapUtils.CompareMapWithMap(new AttrType(AttrType.attrString), outer_map, GlobalConst.COL_LABEL, peek, GlobalConst.COL_LABEL) == 0){
//            //if(outer_map.getRowLabel().equalsIgnoreCase(peek.getRowLabel())){
//            try{
//              outer_map = peek;//new Map(peek.getMapByteArray(), 0, GlobalConst.MAP_LEN);;//new Map(peek.getMapByteArray(), 0 , GlobalConst.MAP_LEN);
//              //peek.setHdr((short) MapSchema.MapFldCount(),MapSchema.MapAttrType(), MapSchema.MapStrLengths());
//            }
//            catch (Exception e){
//              e.printStackTrace();
//            }
//          }
//          else{
//            //outer_map = peek;
//            break;
//          }
//        }
//        else{
//          eof = true;
//          break;
//        }
//      }
      if(outer_map != null){
        // The next step is to get a tuple from the inner,
        // while the inner is not completely scanned && there
        // is no match (with pred),get a tuple from the inner.

        MID mid = new MID();
        while ((inner_map = inner.getNext(mid)) != null) {
//          System.out.println("GOT SOMETHING FROM INNER MAP");
//          System.out.println("RL " + inner_map.getRowLabel());

//          inner_map = new Map(inner_map.getMapByteArray(), 0, GlobalConst.MAP_LEN);
          inner_map.setHdr((short) in2_len, _in2, t2_str_sizescopy);
          if (PredEval.Eval(RightFilter, inner_map, null, _in2, null) == true) {
            //inner_map.print(MapSchema.MapAttrType());
            if (PredEval.Eval(OutputFilter, outer_map, inner_map, _in1, _in2) == true) {
              // Apply a projection on the outer and inner tuples.
              Projection.RowJoin(outer_map, _in1, inner_map, _in2, Jmap, perm_mat, nOutFlds);
              //inner = null;
              //inner.closescan();
              //hf = new Heapfile(hfName);
              //get_from_outer = true;
              //outer_map = peek;
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
      outer_map = peek;
    } while (true);
  }


  public Map get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
          InvalidTypeException, PageNotReadException, MapUtilsException, PredEvalException, SortException,
          LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
    // This is a DUMBEST form of a join, not making use of any key information...

    if (done)
      return null;
    if(outer_map == null){
      outer_map = outer.get_next();
    }
    do {
      // If get_from_outer is true, Get a tuple from the outer, delete
      // an existing scan on the file, and reopen a new scan on the file.
      // If a get_next on the outer returns DONE?, then the nested loops
      // join is done too.

      if (get_from_outer == true) {
        get_from_outer = false;
        if (inner != null) // If this not the first time,
        {
          // close scan
          inner.closescan();
          inner = null;
        }

        try {
          inner = hf.openScan();
        } catch (Exception e) {
          throw new NestedLoopException(e, "openScan failed");
        }
        boolean eof = false;
        findLatestMap(outer, eof);

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
      //Map peek = null;
      //TODO
      //peek = null;

      if(outer_map != null){
        // The next step is to get a tuple from the inner,
        // while the inner is not completely scanned && there
        // is no match (with pred),get a tuple from the inner.

        MID mid = new MID();
        while ((inner_map = inner.getNext(mid)) != null) {
//          System.out.println("GOT SOMETHING FROM INNER MAP");
//          System.out.println("RL " + inner_map.getRowLabel());

//          inner_map = new Map(inner_map.getMapByteArray(), 0, GlobalConst.MAP_LEN);
          inner_map.setHdr((short) in2_len, _in2, t2_str_sizescopy);
          if (PredEval.Eval(RightFilter, inner_map, null, _in2, null) == true) {
            //inner_map.print(MapSchema.MapAttrType());
            if (PredEval.Eval(OutputFilter, outer_map, inner_map, _in1, _in2) == true) {
              // Apply a projection on the outer and inner tuples.
              Projection.RowJoin(outer_map, _in1, inner_map, _in2, Jmap, perm_mat, nOutFlds);
              //inner = null;
              //inner.closescan();
              //hf = new Heapfile(hfName);
              //get_from_outer = true;
              //outer_map = peek;
              return Jmap;
            }
          }
        }

        // There has been no match. (otherwise, we would have
        // returned from t//he while loop. Hence, inner is
        // exhausted, => set get_from_outer = TRUE, go to top of loop
      }
      // if at last element, close
//      if(eof)
//        outer_map = null;
      get_from_outer = true; // Loop back to top and get next outer tuple.
      outer_map = peek;
    } while (true);
  }


  // Assume for now that am comes in with something
  private void findLatestMap(Iterator am, boolean eof)
    throws Exception{
    //boolean eof = false;
    System.out.println("RL " + outer_map.getRowLabel());
    //Map peek = null;
    while(outer_map != null){
      peek = am.get_next();
      if(peek != null){
        // If RL CL match, the current map is not the latest value TS
        peek = new Map(peek.getMapByteArray(), 0, GlobalConst.MAP_LEN);
        peek.setHdr((short) MapSchema.MapFldCount(),MapSchema.MapAttrType(), MapSchema.MapStrLengths());
        if(MapUtils.CompareMapWithMap(new AttrType(AttrType.attrString), outer_map, GlobalConst.ROW_LABEL, peek, GlobalConst.ROW_LABEL) == 0 &&
                MapUtils.CompareMapWithMap(new AttrType(AttrType.attrString), outer_map, GlobalConst.COL_LABEL, peek, GlobalConst.COL_LABEL) == 0){
          //if(outer_map.getRowLabel().equalsIgnoreCase(peek.getRowLabel())){
          try{
            outer_map = peek;//new Map(peek.getMapByteArray(), 0, GlobalConst.MAP_LEN);;//new Map(peek.getMapByteArray(), 0 , GlobalConst.MAP_LEN);
            //peek.setHdr((short) MapSchema.MapFldCount(),MapSchema.MapAttrType(), MapSchema.MapStrLengths());
          }
          catch (Exception e){
            e.printStackTrace();
          }
        }
        else{
          break;
        }
      }
      else{
        eof = true;
        break;
      }
    }
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
        inner.closescan();
        hf.deleteFile();
      } catch (Exception e) {
        throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
      }
      closeFlag = true;
    }
  }
}
