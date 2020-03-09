package BigT;

import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import global.*;

interface Tabletype {
    int TEMP = 0;
    int ORDINARY = 1;
}

/**
 * BigT.bigt, which creates and maintains all the relevant heapﬁles 
 *(and index ﬁles of your choice to organize the data)
 */
public class bigt implements Tabletype, GlobalConst{

    PageId _firstDirPageId; // page number of the header page
    int _ftype;
    private boolean _file_deleted;
    private String _fileName;
    private static int tempfilecount = 0;

    private int rowCnt;
    private int columnCnt;
    private int mapCnt;
    private int BTType;

    // get a new datapage from the buffer manager and initialize dpinfo
    // @param dpinfop the information in the new HFPage
    private HFPage _newDatapage(DataPageInfo dpinfop)
        throws HFException,
        HFBufMgrException,
        HFDiskMgrException,
        IOException
    {
        Page apage = new Page();
        PageId pageId = new PageId();
        pageId = newPage(apage, 1);

        if(pageId == null)
            throw new HFException(null, "can't new table");

        // initialize internal values of the new page

        HFPage hfpage = new HFPage();
        hfpage.init(pageId, apage);

        dpinfop.pageId.pid = pageId.pid;
        dpinfop.recct = 0;
        dpinfop.availspace = hfpage.available_space();

        return hfpage;
    } // end of _newDatapage

    // TODO: change all tuple/record API to corresponding map's

    // internal heapfile function (used in getMap and updateMap):
    // returns pinned directroy page and pinned data page of the specified user record (mid)
    // and true if record is found.
    // If the user record cannot be found, return false.
    private boolean _findDataPage(MID mid, PageId dirPageId, HFPage dirpage,
                                    PageId dataPageId, HFPage datapage, MID rpDataPageMid)
        throws InvalidSlotNumberException,
        HFException,
        HFBufMgrException,
        HFDiskMgrException,
    {
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        HFPage currentDirPage = new HFPage();
        HFPage currentDataPage = new HFPage();
        MID currentDataPageMid = new PageId();
        PageId nextDirPageId = new PageId();
        // datapageId is stored in dpinfo.pageId

        pinPage(currentDirPageId, currentDirPage, false/*read disk*/);

        Map amap = new Map();

        while(currentDirPageId.pid != INVALID_PAGE)
        {
            // start while01
            // ASSERTIONS:
            // currentDirPage, currentDirPageId valid and pinned and locked.

            for(currentDataPageMid = currentDirPage.firstRecord();
                currentDataPageMid != null;
                currentDataPageMid = currentDirPage.nextRecord(currentDataPageMid))
            {
                try
                {
                    amap = currentDirPage.getRecord(currentDataPageMid);
                }
                catch(InvalidSlotNumberException e)
                {
                    return false;
                }

                DataPageInfo dpinfo = new DataPageInfo(amap);

                try
                {
                    pinPage(dpinfo.pageId, currentDataPage, false/*read disk*/);
                }
                catch(Exception e)
                {
                    unpinPage(currentDirPageId, false/*read disk*/);
                    dirpage = null;
                    datapage = null;
                    throw e;
                }

                // ASSERTIONS:
                // - currentDataPage, currentDataPageId, dpinfo valid
                // - currentDataPage pinned

                if(dpinfo.pageId.pid == mid.pageNo.pid)
                {
                    amap = currentDataPage.returnRecord(mid);
                    // found user's record on the current datapage with itself
                    // is indexed on the current dirpage. Return both of these

                    dirpage.setpage(currentDirPage.getpage());
                    dirPageId.pid = currentDirPageId.pid;

                    datapage.setpage(currentDataPage.getpage());
                    dirPageId.pid = dpinfo.pageId.pid;

                    rpDataPageMid.pageNo.pid = currentDataPageMid.pageNo.pid;
                    rpDataPageMid.slotNo = currentDataPageMid.slotNo;
                    return true;
                }
                else
                {
                    // user record not found on this datpage; unpin it
                    // and try the next one
                    unpinPage(dpinfo.pageId, false/*read disk*/);
                }
            }
            
            // if we would have found the correct datapage on the current directory page
            // we would have already returned. therefore: read in the next directroy page:

            nextDirPageId = currentDirPage.getNextPage();
            try
            {
                unpinPage(currentDirPageId, false/*read disk*/);
            }
            catch(Exception e)
            {
                throw new HFException(e, "heapfile,_find,unpinpage failed");
            }

            currentDirPageId.pid = nextDirPageId.pid;
            if(currentDirPageId.pid != INVALID_PAGE)
            {
                pidPage(currentDirPageId, currentDirPage, false/*read disk*/);
                if(currentDirPage == null)
                    throw new HFException(null, "pinPage return null page");
            }
        } // end of while01
            // checked all dir pages and all data pages; use record not found.

        dirPageId.pid = dataPageId.pid = INVALID_PAGE;

        return false;
    } // end of _findDataPage


    // Initialize the big table. A null name produces a temporary heapfile while will be deleted
    // by the destructor. If the name already denotes a file, the file is opened; otherwise, a new empty file
    // is created.
    // @param type an integer between 1 and 5 and the different types will correspond 
    //      to different clustering and indexing strategies you will use for the bigtable.
    bigt(String name, int type)
        throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException
    {
        // give us a prayer of destructing cleanly if construction fails.
        _file_deleted = true;
        _fileName = null;

        if(name == null)
        {
            // If the name is null, allocate a temporary name
            // and no logging is required
            _fileName = "tempTableFile";
            String useId = new String("user.name");
            String userAccName;
            userAccName = System.getProperty(useId);
            _fileName = _fileName + userAccName;

            String filenum = Integer.toString(tempfilecount);
            _fileName = _fileName + filenum;
            _ftype = TEMP;
            tempfilecount++;
        }
        else
        {
            _fileName = name;
            _ftype = ORDINARY;
        }

        // The constructor gets run in two different cases.
        // In the first case, the file is new and the header page
        // must be initialized. This case is detected via a failure
        // in the db->get_file_entry() call. I the second case, the
        // file already exists and all that must be done is to fetch
        // the header page into the buffer pool

        // try to open the file

        Page apage = new Page();
        _firstDirPageId = null;
        if(_ftype == ORDINARY)
            _firstDirPageId = get_file_entry(_fileName);
        
        if(_firstDirPageId == null)
        {
            // file doesn't exist. First create it.
            _firstDirPageId = newPage(apage, 1);
            // check error
            if(_firstDirPageId == null)
                throw new HFException(null, "can't new page");
            
            add_file_entry(_fileName, _firstDirPageId);
            // check error(new exception: could not add file entry)

            HFPage firstDirPage = new HFPage();
            firstDirPage.init(_firstDirPageId, apage);
            PageId pageId = new PageId(INVALID_PAGE);

            firstDirPage.setNextPage(pageId);
            firstDirPage.setPrevPage(pageId);
            unpinPage(_firstDirPageId, true/*dirty*/);
        }

        _file_deleted = false;

        // ASSERTIONS:
        // - ALL private data members of class Heapfile are valid:
        //
        // - _firstDirPageId valid
        // - _fileName valid
        // -no datapage pinned yet

        // BTname = name;
        // BTtype = type;
        // rowCnt = 0;
        // columnCnt = 0;
        // mapCnt = 0;

    } // end of constructor

    //Delete the bigtable from the database.
    void deleteBigt(){}

    //Return number of maps in the bigtable.
    public int getMapCnt()
        throws InvalidSlotNumberException,
            InvalidMapSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException
    {
        int answer = 0;
        PageId currentDirPageId = new PageId(_firstDirPageId.pid);

        PageId nextDirPageId = new PageId(0);

        HFPage currentDirPage = new HFPage();
        Page pageinbuffer = new Page();

        while(currentDirPageId.pid != INVALID_PAGE)
        {
            pinPage(currentDirPageId, currentDirPage, false);

            MID mid = new MID();
            Map amap;
            for(mid = currentDirPage.firstRecord();
                mid != null;
                mid = currentDirPage.nextRecord(mid))
            {
                amap = currentDirPage.getRecord(mid);
                DataPageInfo dpinfo = new DataPageInfo(amap);

                answer += dpinfo.recct;
            }

            // ASSERTIONS: no more record
            // - we have read all datapage records on the current directory page.

            nextDirPageId = currentDirPage.getNextPage();
            unpinPage(currentDirPageId, false);
            currentDirPageId.pid = nextDirPageId.pid;
        }
        return answer;
    }

    //Return number of distinct row labels in the bigtable.
    int getRowCnt()
    {
        return 0;
    }

    //Return number of distinct column labels in the bigtable.
    int getColumnCnt()
    {
        return 0;
    }

    //Insert map into the big table, return its Mid. 
    //The insertMap() method ensures that there are at most three maps with 
    //the same row and column labels, but different timestamps, in the bigtable. 
    //When a fourth is inserted, the one with the oldest label is dropped from the big table.
    MID insertMap(byte[] mapPtr){}

    /*
    Initialize a stream of maps where row label matching rowFilter, 
    column label matching columnFilter, and value label matching valueFilter. 
    If any of the ﬁlter are null strings, then that ﬁlter is not considered 
    (e.g., if rowFilter is null, then all row labels are OK). 
    If orderType is:
    1, then results are ﬁrst ordered in row label, then column label, then time stamp · 
    2, then results are ﬁrst ordered in column label, then row label, then time stamp · 
    3, then results are ﬁrst ordered in row label, then time stamp · 
    4, then results are ﬁrst ordered in column label, then time stamp · 
    5, then results are ordered in time stamp
    */
    Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter){}
}