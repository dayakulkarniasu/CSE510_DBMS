package  heap;

import global.*;
import iterator.*;
import diskmgr.*;
import heap.*;
import java.io.*;
import java.lang.*;
import BigT.*;

/**
 * This class will be similar to heap.Scan, however, will provide different
 * types of accesses to the bigtable
 */
public class Stream implements GlobalConst {
    /**
     * Note that one record in our way-cool HeapFile implementation is
     * specified by six (6) parameters, some of which can be determined
     * from others:
     */

    /** The heapfile we are using. */
    private Heapfile _hf;

    /** PageId of current directory page (which is itself an HFPage) */
    private PageId dirpageId = new PageId();

    /** pointer to in-core data of dirpageId (page is pinned) */
    private HFPage dirpage = new HFPage();

    /** record ID of the DataPageInfo struct (in the directory page) which
     * describes the data page where our current record lives.
     */
    private MID datapageMid = new MID();

    /** the actual PageId of the data page with the current record */
    private PageId datapageId = new PageId();

    /** in-core copy (pinned) of the same */
    private HFPage datapage = new HFPage();

    /** record ID of the current record (from the current data page) */
    private MID usermid = new MID();

    /** Status of next user status */
    private boolean nextUserStatus;

    /** The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @exception InvalidMapSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter)
            throws InvalidMapSizeException, IOException
    {
        init(bigtable);
    }

    /** Do all the constructor work
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param bigtable A bigt object
     */
    private void init(bigt bigtable)
        throws InvalidMapSizeException,
            IOException
    {
        _hf = bigtable.getHeapfile();
        firstDataPage();
    }

    /** Move to the first data page in the file. 
     * @exception InvalidMapSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @return true if successful
     *         false otherwise
     */
    private boolean firstDataPage()
        throws InvalidMapSizeException,
            IOException
    {
        DataPageInfo dpinfo;
        Map recmap = null;
        Boolean bst;

        // copy data about first directory page
        dirpageId.pid = _hf._firstDirPageId.pid;
        nextUserStatus = true;

        // get first directory page and pin it
        try
        {
            dirpage = new HFPage();
            pinPage(dirpageId, (Page) dirpage, false);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // now try to get a pointer to the first datapage
        datapageMid = dirpage.firstMap();

        if(datapageMid != null)
        // there is a datapage map on the first directory page
        {
            try
            {
                recmap = dirpage.getMap(datapageMid);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            dpinfo = new DataPageInfo(recmap);
            datapageId.pid = dpinfo.pageId.pid;
        }
        else
        // the first directory page is the only one which can possibly remain empty
        // therefore try to get the next directory page and check it. The next one
        // has to contain a datapage map, unless the heapfile is empty.
        {
            PageId nextDirPageId = new PageId();
            nextDirPageId = dirpage.getNextPage();

            if(nextDirPageId.pid != INVALID_PAGE)
            {
                try
                {
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }

                try
                {
                    dirpage = new HFPage();
                    pinPage(nextDirPageId, (Page) dirpage, false);
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }

                //now try again to read a data record
                try
                {
                    datapageMid = dirpage.firstMap();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                    datapageId.pid = INVALID_PAGE;
                }

                if(datapageMid != null)
                {
                    try
                    {
                        recmap = dirpage.getMap(datapageMid);
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }

                    if(recmap.getLength() != DataPageInfo.size)
                        return false;

                    dpinfo = new DataPageInfo(recmap);
                    datapageId.pid = dpinfo.pageId.pid;
                }
                else
                // heapfile empty
                {
                    datapageId.pid = INVALID_PAGE;
                }
            }
            else
            // heapfile empty
            {
                datapageId.pid = INVALID_PAGE;
            }
        }

        datapage = null;

        try
        {
            nextDataPage();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    // shortcut to access the pinPage function in bufmgr package.
    // @see bufmgr.pinPage
    private void pinPage(PageId pageno, Page page, boolean emptyPage)
        throws HFBufMgrException
    {
        try
        {
            SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
        }
        catch(Exception e)
        {
            throw new HFBufMgrException(e, "Scan.java: pinPage() failed");
        }
    } // end of pinPage

    // shortcut to access the unpinPage function in bufmgr package.
    // @see bufmgr.pinPage
    private void unpinPage(PageId pageno, boolean dirty)
        throws HFBufMgrException
    {
        try
        {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        }
        catch(Exception e)
        {
            throw new HFBufMgrException(e, "Scan.java: unpinPage() failed");
        }
    } // end of unpinPage

    // Move to the next data page in the file and retrieve the next data page
    // @return true if successful, false if unsuccessful
    private boolean nextDataPage()
        throws InvalidMapSizeException,
            IOException
    {
        DataPageInfo dpinfo;

        boolean nextDataPageStatus;
        PageId nextDirPageId = new PageId();
        Map recmap = null;

        // ASSERTIONS:
        // - this->dirpageId has Id of current directory page
        // - this->dirpage is valid and pinned
        // (1) if heapfile empty:
        //    - this->datapage==NULL; this->datapageId == INVALID_PAGE
        // (2) if overall first record in heapfile:
        //    - this->datapage==NULL, but this->datapageId valid
        //    - this->datapageMid valid
        //    - current data page unpinned !!!
        // (3) if somewhere in heapfile
        //    - this->datapageId, this->datapage, this->datapageRid valid
        //    - current data page pinned
        // (4)- if the scan had already been done,
        //        dirpage = NULL;  datapageId = INVALID_PAGE
        if((dirpage == null) && (datapageId.pid == INVALID_PAGE))
            return false;
        if(datapage == null)
        {
            if(datapageId.pid == INVALID_PAGE)
            // heapfile is empty to begin with
            {
                try
                {
                    unpinPage(dirpageId, false);
                    dirpage = null;
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                // pin the first data page
                try
                {
                    datapage = new HFPage();
                    pinPage(datapageId, (Page) datapage, false);
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }

                try
                {
                    usermid = datapage.firstMap();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }

                return true;
            }
        }

        // ASSERTIONS:
        // - this->datapage, this->datapageId, this->datapageRid valid
        // - current datapage pinned

        // unpin the current page
        try
        {
            unpinPage(datapageId, false);
            datapage = null;
        }
        catch(Exception e)
        {

        }

        // read next datapagemap from current directory page
        // dirpage is set to NULL at the end of scan.

        if(dirpage == null)
            return false;
        
        datapageMid = dirpage.nextMap(datapageMid);

        if(datapageMid == null)
        {
            nextDataPageStatus = false;
            // we have read all datapage maps on the current dirctory page
            
            // get next directory page
            nextDirPageId = dirpage.getNextPage();

            // unpin the current directory page
            try
            {
                unpinPage(dirpageId, false);
                dirpage = null;
                datapageId.pid = INVALID_PAGE;
            }
            catch(Exception e)
            {

            }

            if(nextDirPageId.pid = INVALID_PAGE)
            {
                return false;
            }
            else
            {
                // ASSERTION:
                // - nextDirPageId has correct id of the page which is to get
                
                dirpageId = nextDirPageId;

                try
                {
                    dirpage = new HFPage();
                    pinPage(dirpageId, (Page)dirpage, false);
                }
                catch(Exception e)
                {

                }

                if(dirpage == null)
                    return false;
                
                try
                {
                    datapageMid = dirpage.firstMap();
                    nextDataPageStatus = true;
                }
                catch(Exception e)
                {
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
        try
        {
            recmap = dirpage.getMap(datapageMid);
        }
        catch(Exception e)
        {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        if(recmap.getLength() != DataPageInfo.size)
            return false;
        
        dpinfo = new DataPageInfo(recmap);
        datapageId.pid = dpinfo.pageId.pid;

        try
        {
            datapage = new HFPage();
            pinPage(dpinfo.pageId, (Page) datapage, false);
        }
        catch(Exception e)
        {
            System.err.println("HeapFile: Error in Scan" + e);
        }

        // - directory page is pinned
        // - datapage is pinned
        // - this->dirpageId, this->dirpage correct
        // - this->datapageId, this->datapage, this->datapageRid correct

    }

}