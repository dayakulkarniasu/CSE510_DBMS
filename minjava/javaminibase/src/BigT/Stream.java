package BigT;

import btree.BTreeFile;
import btree.StringKey;
import global.*;
import heap.HFBufMgrException;
import index.IndexException;
import index.IndexScan;
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
public class Stream implements GlobalConst{

    /** in-core copy (pinned) of the same */
    private bigt _bigTable;
    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();
    /** pageId of bigT*/
    private PageId datapageId = new PageId();
    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();
    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     *
     */
    private MID datapageMid = new MID();
    /** Status of next user status */
    private boolean nextUserStatus;
    /** in-core copy (pinned) of the same */
    private HFPage datapage = new HFPage();

    /** record ID of the current record (from the current data page) */
    private MID userrid = new MID();
    /** The amount of pages available for sorting*/
    int SORTPGNUM = 12;
    /** Strings are set to use size 62 bytes, 2 extra bytes are added when setting hdr for maps, hence 64 */
    private short RECLENGTH = 62;

    private final static boolean OK = true;
    private IndexScan iscan = null;



    /**
     * Initialize a stream of maps on bigtable.
     * 
     * @param bigtable
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     */
    Stream(bigt bigtable, int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter)
            throws InvalidTupleSizeException, IOException{
        _bigTable = bigtable;
        /** copy data about first directory page */

        dirpageId.pid = _bigTable.hf._firstDirPageId.pid;
        nextUserStatus = true;

        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[3];
        attrSize[0] = RECLENGTH;
        attrSize[1] = RECLENGTH;
        attrSize[2] = RECLENGTH;

        // create empty map we will use for reading data
        Map m = new Map();
        try {
            // set the header info for the new map
            m.setHdr((short) 4, attrType, attrSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        int size = m.size();

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

        try {
            fscan = new FileScan(_bigTable.Name, attrType, attrSize, (short) 4, 4, projlist, outFilter[0] == null ? null : outFilter );
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        int count = 0;
        m = null;
        MID mid = new MID();

        try {
            m = fscan.get_next();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        BTreeFile btf = null;

        boolean flag = true;

        // Sort "test1.in"
        Sort sort = null;
        try {

            //TODO
            //sort = new Sort(attrType, (short) 4, attrSize, fscan, 1, order[0], 64, SORTPGNUM);
            switch (orderType){
                case OrderType.type1:
                    //results ordered by rowLabel then columnLabel then time stamp
                    String key = null;
                    MID MID = new MID();
                    Map temp = null;


                    try {
                        //TODO make sure key size when making btree is correct
                        btf = new BTreeFile("StreamOrderIndex", AttrType.attrString, RECLENGTH*3, 1/*delete*/);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        Runtime.getRuntime().exit(1);
                    }
                    while ( temp != null) {
                        try {
                            key = m.getRowLabel() + " " + m.getColumnLabel() + " " + m.getTimeStamp();
                            mid = fscan.MID;
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
                            temp = fscan.getNext();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case OrderType.type2:
                    //ordered columnLabel, rowLabel, timestamp
                    break;
                case OrderType.type3:
                    //row label then timestamp
                    break;
                case OrderType.type4:
                    //column label then time stamp
                    break;
                case OrderType.type5:
                    //time stamp
                    //sort = new Sort(attrType, (short) 4, attrSize, fscan, 3, order[0], 4, SORTPGNUM);
                    break;
                default:
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        iscan = new IndexScan(new IndexType(IndexType.B_Index), _bigTable.Name, "StreamOrderIndex", attrType, attrSize, 4, 4, projlist, null, 1, false);


    }


    /**
     * Each filter can either be *, a single value, or a range [x,y]. This function takes the filters and determines
     * What kind of operation each filter is then converts it into the appropriate CondExpr to be used with a FIleScan
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @return the outFilter to be used with the FileScan
     */
    private CondExpr[] queryFilter(java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter) {
        List<CondExpr> query = new ArrayList<CondExpr>();
        // If RowFilter is a range, create a range expression
        if(isRangeQuery(rowFilter)){
            String[] bounds = getBounds(rowFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 0)){
                query.add(expr);
            }
        }
        // Else if rowFilter is an equality, create an identity expression
        else if(isEqualityQuery(rowFilter)){
            query.add(equalityQuery(rowFilter, 0));
        }
        // If ColumnFilter is a range, create a range expression
        if(isRangeQuery(columnFilter)){
            String[] bounds = getBounds(rowFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 1)){
                query.add(expr);
            }
        }
        // Else if columnFilter is an equality, create an identity expression
        else if(isEqualityQuery(columnFilter)){
            query.add(equalityQuery(columnFilter, 1));
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
//        CondExpr [] expr2 = new CondExpr[3];
//        expr2[0] = new CondExpr();
//
//
//        expr2[0].next  = null;
//        expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
//        expr2[0].type1 = new AttrType(AttrType.attrSymbol);
//
//        expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
//        expr2[0].type2 = new AttrType(AttrType.attrSymbol);
//
//        expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
//
//        expr2[1] = new CondExpr();
//        expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
//        expr2[1].next = null;
//        expr2[1].type1 = new AttrType(AttrType.attrSymbol);
//
//        expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
//        expr2[1].type2 = new AttrType(AttrType.attrReal);
//        expr2[1].operand2.real = (float)40.0;
//
//
//        expr2[1].next = new CondExpr();
//        expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
//        expr2[1].next.next = null;
//        expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
//        expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
//        expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
//        expr2[1].next.operand2.integer = 7;
//
//        expr2[2] = null;
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
        if (_bigTable != null) {

            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        _bigTable = null;

        if (dirpage != null) {

            try{
                unpinPage(dirpageId, false);
            }
            catch (Exception e){
                //     System.err.println("SCAN: Error in Scan: " + e);
                e.printStackTrace();
            }
        }
        dirpage = null;

        nextUserStatus = true;

    }

    /**
     * Retrieve the next map in the stream.
     * 
     * @param mid
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
        return recptrmap;

//        if (nextUserStatus != true) {
//            nextDataPage();
//        }
//
//        if (datapage == null)
//            return null;
//
//        mid.pageNo.pid = userrid.pageNo.pid;
//        mid.slotNo = userrid.slotNo;
//
//        try {
//            recptrmap = datapage.getMap(mid);
//        }
//
//        catch (Exception e) {
//            //    System.err.println("SCAN: Error in Scan" + e);
//            e.printStackTrace();
//        }
//
//        try {
//            userrid = datapage.nextMap(mid);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        if(userrid == null) nextUserStatus = false;
//        else nextUserStatus = true;
//
//        return recptrmap;

    }

    /** Move to the next data page in the file and
     * retrieve the next data page.
     *
     * @return 		true if successful
     *			false if unsuccessful
     */
    private boolean nextDataPage()
            throws InvalidTupleSizeException,
            IOException
    {
        DataPageInfo dpinfo;

        boolean nextDataPageStatus;
        PageId nextDirPageId = new PageId();
        Map recmap = null;

        // ASSERTIONS:
        // - this->dirpageId has Id of current directory page
        // - this->dirpage is valid and pinned
        // (1) if bigTable empty:
        //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
        // (2) if overall first record in bigTable:
        //    - this->datapage==NULL, but this->datapageId valid
        //    - this->datapageRid valid
        //    - current data page unpinned !!!
        // (3) if somewhere in bigTable
        //    - this->datapageId, this->datapage, this->datapageRid valid
        //    - current data page pinned
        // (4)- if the scan had already been done,
        //        dirpage = NULL;  datapageId = INVALID_PAGE

        if ((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;

        if (datapage == null) {
            if (datapageId.pid == INVALID_PAGE) {
                // heapfile is empty to begin with

                try{
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch (Exception e){
                    //  System.err.println("Scan: Chain Error: " + e);
                    e.printStackTrace();
                }

            } else {

                // pin first data page
                try {
                    datapage  = new HFPage();
                    pinPage(datapageId, (Page) datapage, false);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

                try {
                    userrid = datapage.firstMap();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }

                return true;
            }
        }

        // ASSERTIONS:
        // - this->datapage, this->datapageId, this->datapageRid valid
        // - current datapage pinned

        // unpin the current datapage
        try{
            unpinPage(datapageId, false /* no dirty */);
            datapage = null;
        }
        catch (Exception e){

        }

        // read next datapagerecord from current directory page
        // dirpage is set to NULL at the end of scan. Hence

        if (dirpage == null) {
            return false;
        }

        datapageMid = dirpage.nextMap(datapageMid);

        if (datapageMid == null) {
            nextDataPageStatus = false;
            // we have read all datapage records on the current directory page

            // get next directory page
            nextDirPageId = dirpage.getNextPage();

            // unpin the current directory page
            try {
                unpinPage(dirpageId, false /* not dirty */);
                dirpage = null;

                datapageId.pid = INVALID_PAGE;
            }

            catch (Exception e) {

            }

            if (nextDirPageId.pid == INVALID_PAGE)
                return false;
            else {
                // ASSERTION:
                // - nextDirPageId has correct id of the page which is to get

                dirpageId = nextDirPageId;

                try {
                    dirpage  = new HFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }

                catch (Exception e){

                }

                if (dirpage == null)
                    return false;

                try {
                    datapageMid = dirpage.firstMap();
                    nextDataPageStatus = true;
                }
                catch (Exception e){
                    nextDataPageStatus = false;
                    return false;
                }
            }
        }

        // ASSERTION:
        // - this->dirpageId, this->dirpage valid
        // - this->dirpage pinned
        // - the new datapage to be read is on dirpage
        // - this->datapageRid has the Rid of the next datapage to be read
        // - this->datapage, this->datapageId invalid

        // data page is not yet loaded: read its record from the directory page
        try {
            recmap = dirpage.getMap(datapageMid);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        if (recmap.getLength() != DataPageInfo.size)
            return false;

        dpinfo = new DataPageInfo(recmap);
        datapageId.pid = dpinfo.pageId.pid;

        try {
            datapage = new HFPage();
            pinPage(dpinfo.pageId, (Page) datapage, false);
        }

        catch (Exception e) {
            System.err.println("HeapFile: Error in Scan" + e);
        }


        // - directory page is pinned
        // - datapage is pinned
        // - this->dirpageId, this->dirpage correct
        // - this->datapageId, this->datapage, this->datapageRid correct

        userrid = datapage.firstMap();

        if(userrid == null)
        {
            nextUserStatus = false;
            return false;
        }

        return true;
    }

    /**
     * short cut to access the pinPage function in bufmgr package.
     * @see bufmgr
     */
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: pinPage() failed");
        }

    } // end of pinPage

    /**
     * short cut to access the unpinPage function in bufmgr package.
     * @see bufmgr
     */
    private void unpinPage(PageId pageno, boolean dirty)
            throws HFBufMgrException {

        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch (Exception e) {
            throw new HFBufMgrException(e,"Scan.java: unpinPage() failed");
        }

    } // end of unpinPage
}
