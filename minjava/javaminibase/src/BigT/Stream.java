package BigT;

import btree.BTreeFile;
import btree.StringKey;
import btree.IntegerKey;
import global.*;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.*;
import diskmgr.*;
import heap.*;
import java.util.*;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This class will be similar to heap.Scan, however, will provide different
 * types of accesses to the bigtable
 */
public class Stream implements GlobalConst {

    private int nscan = 0;
    /** The bigT that will be used for creating sorted btree files to return results */
    private bigt _bigTable;
    /**
     * Strings are set to use size 62 bytes, 2 extra bytes are added when setting
     * hdr for maps, hence 64
     */
    private IndexScan iscan = null;

    /**
     * Initialize a stream of maps on bigtable.
     * 
     * @param bigtable
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @throws UnknownIndexTypeException
     * @throws InvalidTypeException
     * @throws IndexException
     */
    public Stream(bigt bigtable, int orderType, java.lang.String rowFilter, java.lang.String columnFilter,
            java.lang.String valueFilter) throws InvalidTupleSizeException, IOException, IndexException,
            InvalidTypeException, UnknownIndexTypeException {

        _bigTable = bigtable;

        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrString);
        attrType[3] = new AttrType(AttrType.attrInteger);
        short[] attrSize = new short[3];
        attrSize[0] = STR_LEN;
        attrSize[1] = STR_LEN;
        attrSize[2] = STR_LEN;

        MapOrder[] order = new MapOrder[2];
        order[0] = new MapOrder(MapOrder.Ascending);
        order[1] = new MapOrder(MapOrder.Descending);
        // create empty map we will use for reading data
        Map m = new Map();
        try {
            // set the header info for the new map
            m.setHdr((short) 4, attrType, attrSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);

        // OutFilter for limiting results from BigTable search
        CondExpr[] outFilter = queryFilter(rowFilter, columnFilter, valueFilter);

        FileScan fscan = null;

        System.out.println("rowfilter: " + rowFilter + " colfilter: " + columnFilter + " valfilter: " + valueFilter);

        try {
            fscan = new FileScan(bigtable.name, attrType, attrSize, (short) 4, 4, projlist, outFilter[0] == null ? null : outFilter );
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        m = null;
        MID mid = new MID();

        try {
            //m = fscan.get_next();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        BTreeFile btf = null;

        try {
            String key = null;
            Map temp = null;
            switch (orderType){
                case OrderType.type1:
                    //results ordered by rowLabel then columnLabel then time stamp
                    try {
                        // btf = new BTreeFile("StreamOrderIndex", AttrType.attrString, STR_LEN*3, 1/*delete*/);
                        btf = new BTreeFile("StreamOrderIndex", AttrType.attrString, STR_LEN, 1/*delete*/);
                        // btf.destroyFile();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    while ( m != null) {
                        System.out.println("stream line 140: rowlabel: " + m.getRowLabel());
                        try {
                            // key = m.getRowLabel() + " " + m.getColumnLabel() + " " + m.getTimeStamp();
                            key = m.getRowLabel();
                            mid = fscan._mid;
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            btf.insert(new StringKey(key), mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            m = fscan.get_next();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case OrderType.type2:
                    //ordered columnLabel, rowLabel, timestamp
                    try {
                        btf = new BTreeFile("StreamOrderIndex", AttrType.attrString, STR_LEN*3, 2/*delete*/);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    while ( m != null) {
                        try {
                            System.out.println("stream line 157: rowlabel: " + m.getRowLabel());
                            key = m.getColumnLabel() + " " + m.getRowLabel() + " " + m.getTimeStamp();
                            mid = fscan._mid;
                            //System.out.println("stream mid: " + mid.pageNo + " " + mid.slotNo);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            btf.insert(new StringKey(key), mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            temp = fscan.get_next();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case OrderType.type3:
                    //row label then timestamp
                    try {
                        btf = new BTreeFile("StreamOrderIndex", AttrType.attrString, STR_LEN*2, 3/*delete*/);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    while ( temp != null) {
                        try {
                            key = m.getRowLabel() + " " + m.getTimeStamp();
                            mid = fscan._mid;
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            btf.insert(new StringKey(key), mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            temp = fscan.get_next();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case OrderType.type4:
                    //column label then time stamp
                    System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++");
                    System.out.println("TYPE 4");
                    FileScan fscan2 = new FileScan(bigtable.name, attrType, attrSize, (short) 4, 4, projlist, outFilter[0] == null ? null : outFilter );
                    Sort am1 = new Sort(attrType, (short) 4, attrSize, fscan, 2, order[0], STR_LEN, 12);
                    Sort am2 = new Sort(attrType, (short) 4, attrSize, fscan2, 4, order[0], STR_LEN, 12);
                    FldSpec[] projlist2 = new FldSpec[4];
                    RelSpec rel2 = new RelSpec(RelSpec.outer);
                    RelSpec rel3 = new RelSpec(RelSpec.innerRel);
                    projlist2[0] = new FldSpec(rel2, 1);
                    projlist2[1] = new FldSpec(rel2, 2);
                    projlist2[2] = new FldSpec(rel2, 3);
                    projlist2[3] = new FldSpec(rel3, 4);
                    SortMerge sm = null;
                    sm = new SortMerge(attrType, 4, attrSize,
                            attrType, 4, attrSize,
                            3, STR_LEN,
                            3, 4,
                            12,
                            am1, am2,
                            true, true, order[0],
                            mapComparison(), projlist2, 4);
                    Map t = sm.get_next();
                    System.out.println("***********************************");
                    System.out.println("entering sort merge");
                    while(t != null){
                        
                        t = sm.get_next();
                    }
                    System.out.println("exiting sort merge");


                    try {
                        //btf = new BTreeFile("StreamOrderIndex", AttrType.attrString, STR_LEN*2, 4/*delete*/);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    //while ( temp != null) {
                    while ((temp = sm.get_next()) != null) {
                        System.out.println("************************************************");
                        try {
                            //key = m.getColumnLabel() + " " + m.getTimeStamp();
                            //mid = fscan._mid;
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            //btf.insert(new StringKey(key), mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            System.out.println(temp.getRowLabel() + " " + temp.getColumnLabel() + " " + temp.getValue() + " " + temp.getTimeStamp());
                            //temp = fscan.get_next();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case OrderType.type5:
                    //time stamp
                    int tsKey = 0;
                    Sort sort = null;
                    sort = new Sort(attrType, (short) 4, attrSize, fscan, 4, order[0], STR_LEN, 12);
                    temp = sort.get_next();
                    try {
                        //btf = new BTreeFile("StreamOrderIndex", AttrType.attrInteger, 4, 5/*delete*/);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    while ( temp != null) {
                        try {
                            //tsKey = m.getTimeStamp();
                            //mid = fscan._mid;
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            //btf.insert(new IntegerKey(tsKey), mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            System.out.println("**********************************************************");
                            System.out.println(temp.getRowLabel() + " " + temp.getColumnLabel() + " " + temp.getValue() + " " + temp.getTimeStamp());
                            temp = sort.get_next();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                default:
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        
        iscan = new IndexScan(new IndexType(IndexType.B_Index), SystemDefs.JavabaseDB.table.name, "StreamOrderIndex", attrType, attrSize, 4, 4, projlist, null, 1, false);
    }


    /**
     * Each filter can either be *, a single value, or a range [x,y]. This function takes the filters and determines
     * What kind of operation each filter is then converts it into the appropriate CondExpr to be used with a FIleScan
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @return the outFilter to be used with the FileScan
     */
    private CondExpr[] queryFilter(String rowFilter, String columnFilter, String valueFilter) {
        List<CondExpr> query = new ArrayList<CondExpr>();
        // If RowFilter is a range, create a range expression
        if(isRangeQuery(rowFilter)){
            String[] bounds = getBounds(rowFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 1)){
                query.add(expr);
            }
        }
        // Else if rowFilter is an equality, create an identity expression
        else if(isEqualityQuery(rowFilter)){
            query.add(equalityQuery(rowFilter, 1));
        }
        // If ColumnFilter is a range, create a range expression
        if(isRangeQuery(columnFilter)){
            String[] bounds = getBounds(rowFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 2)){
                query.add(expr);
            }
        }
        // Else if columnFilter is an equality, create an identity expression
        else if(isEqualityQuery(columnFilter)){
            query.add(equalityQuery(columnFilter, 2));
        }

        // If ValueFilter is a range, create a range expression
        if(isRangeQuery(valueFilter)){
            String[] bounds = getBounds(rowFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 3)){
                query.add(expr);
            }
        }
        // Else if valueFilter is an equality, create an identity expression
        else if(isEqualityQuery(valueFilter)){
            query.add(equalityQuery(valueFilter, 3));
        }
        // convert results to the format required for scans
        return ConvertToArray(query);
    }

    /**
     * We Determine if a filter is a range by checking for the presence of the ',' character
     * A range query would be in the format [x,y]
     * @param filter
     * @return
     */
    private boolean isRangeQuery(java.lang.String filter){
        return filter.contains(",");
    }

    /**
     * This filter check is context specific, it is always called after isRangeQuery(). In those cases, the only other
     * possible filter under consideration is the single value. A single value will be anything not containing '*'
     * @param filter
     * @return
     */
    private boolean isEqualityQuery(java.lang.String filter){
        return !filter.contains("*");
    }

    /**
     * Parses a range and returns the upper and lower bounds
     * @param filter [x,y] range
     * @return
     */
    private String[] getBounds(java.lang.String filter){
        return filter.replaceAll("(\\[|\\])", "").split(",");
    }

    /**
     * Converts a List of queries into the format required by FileScan, CondExpr[].
     * @param queries A List of Queries
     * @return
     */
    private CondExpr[] ConvertToArray(List<CondExpr> queries){
        // The CondExpr needs to terminate with a null CondExpr,
        // Hence we take the query size + 1
        CondExpr[] plan = new CondExpr[queries.size()+1];
        for (int i = 0; i < queries.size(); i++) {
            plan[i] = queries.get(i);
        }
        plan[queries.size()] = null;
        return plan;
    }

    /**
     * Generates a CondExpr for an equality operation
     * @param rowFilter Single Value Filter
     * @param offSet The OffSet of the map attribute under consideration
     * @return
     */
    private CondExpr equalityQuery(java.lang.String rowFilter, int offSet){
        // set up an equality expression
        CondExpr expr = new CondExpr();
        expr.op = new AttrOperator(AttrOperator.aopEQ);
        expr.type1 = new AttrType(AttrType.attrSymbol);
        expr.type2 = new AttrType(AttrType.attrString);
        expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offSet);
        expr.operand2.string = rowFilter;
        expr.next = null;
        return expr;
    }

    /**
     * Generates a set of CondExpr needed for a range search operation
     * @param lowerBound String value of lower bound of range
     * @param upperBound String value of upper bound of range
     * @param offset The OffSet of the map attribute under consideration
     * @return
     */
    private CondExpr[] rangeQuery(java.lang.String lowerBound, java.lang.String upperBound, int offset){
        //range scan
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopGE);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
        expr[0].operand2.string = lowerBound;
        expr[0].next = null;
        expr[1] = new CondExpr();
        expr[1].op = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
        expr[1].operand2.string = upperBound;
        expr[1].next = null;
        return expr;
    }

    private CondExpr[] mapComparison(){
        CondExpr [] expr  = new CondExpr[4];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();
        expr[2] = new CondExpr();
        expr[3] = new CondExpr();


        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        expr[1].next  = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);

        expr[2].next  = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
        expr[2].type2 = new AttrType(AttrType.attrSymbol);
        expr[2].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);

        expr[3] = null;
        return expr;
    }
    /**
     * Closes the stream object.
     */
    void closestream(){
        if(iscan != null){
            try {
                iscan.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Retrieves the next map in the sorted order stream.
     * @return
     */
    public Map getNext() {
        Map recptrmap = null;
        try {
            recptrmap = iscan.get_next();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        nscan++;
        return recptrmap;

    }
}
