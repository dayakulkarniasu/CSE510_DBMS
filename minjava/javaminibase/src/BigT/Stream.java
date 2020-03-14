package BigT;

import global.*;
import heap.HFBufMgrException;
import iterator.*;
import diskmgr.*;
import heap.*;

import java.io.IOException;

/**
 * This class will be similar to heap.Scan, however, will provide different
 * types of accesses to the bigtable
 */
public class Stream implements GlobalConst{

    /** in-core copy (pinned) of the same */
    private bigt bigTable;
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
    /** the actual PageId of the data page with the current record */


    /**
     * Initialize a stream of maps on bigtable.
     * 
     * @param bigtable
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     */
    Stream(bigt bigtable, int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter){
        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[3];
        attrSize[0] = 64;
        attrSize[1] = 64;
        attrSize[2] = 64;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

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

        FileScan fscan = null;

        try {
            fscan = new FileScan(bigtable.name, attrType, attrSize, (short) 4, 4, projlist, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Sort "test1.in"
        Sort sort = null;
        try {

            //TODO we need to make modifications to the sort to work with maps instead of tuples
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 1, order[0], 64, SORTPGNUM);
            //sort = new Sort(attrType, (short) 4, attrSize, fscan, 3, order[0], 4, SORTPGNUM);
        }
        catch (Exception e) {
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        switch (orderType){
            case OrderType.type1:
                //results ordered by rowLabel then columnLabel then time stamp
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
                break;
            default:

        }
    }

    /**
     * Closes the stream object.
     */
    //TODO need to refactor to work with maps
    // we still need to pin/unpin data from buffer manager (i think)
    void closestream(){
        if (bigTable != null) {

            try{
                unpinPage(datapageId, false);
            }
            catch (Exception e){
                // 	System.err.println("SCAN: Error in Scan" + e);
                e.printStackTrace();
            }
        }
        datapageId.pid = 0;
        bigTable = null;

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
    public Map getNext(MID mid) {
        Map recptrmap = null;

        if (nextUserStatus != true) {
            nextDataPage();
        }

        if (datapage == null)
            return null;

        mid.pageNo.pid = userrid.pageNo.pid;
        mid.slotNo = userrid.slotNo;

        try {
            recptrmap = datapage.getMap(mid);
        }

        catch (Exception e) {
            //    System.err.println("SCAN: Error in Scan" + e);
            e.printStackTrace();
        }

        try {
            userrid = datapage.nextMap(mid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(userrid == null) nextUserStatus = false;
        else nextUserStatus = true;

        return recptrmap;

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