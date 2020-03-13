package BigT;

import global.*;
import iterator.*;

/**
 * This class will be similar to heap.Scan, however, will provide different
 * types of accesses to the bigtable
 */
public class Stream{

    /** in-core copy (pinned) of the same */
    private bigt bigTable;
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
        // TODO need to set the string size for each map
        // I don't know if this is the length of each string in map.
        short[] attrSize = new short[3];

        attrSize[0] = 0;
        attrSize[1] = 0;
        attrSize[2] = 0;

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

        // the "pointer" to records
        MID             mid;

        // not sure why we're doing this again
        m = new Map();
        //m = new Map(size); originally (for tuples) would call with size param
        try {
            m.setHdr((short) 2, attrType, attrSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //At this point we're going to use our indexes to return
        //maps. The indexes should have been created during the batch
        // insertion process (probabally inside of the bigT constructor)
        // that means that at this step all we need to do is call the
        // names of the btree index files.
        // Index File Nmes
        // type 1 - no index
        // type 2 - type2Idx
        // type 3 - type3Idx
        // type 4 - type4CombKeyIdx & type4TSIdx
        // type 5 - type5CombKeyIdx & type5TSIdx

        // create an iterator by open a file scan
        //TODO don't totally understand hte projlist stuff
        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test1.in", attrType, attrSize, (short) 2, 2, projlist, null);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Sort "test1.in"
        Sort sort = null;
        try {

            //TODO we need to make modifications to the sort to work with maps instead of tuples
            sort = new Sort(attrType, (short) 2, attrSize, fscan, 1, order[0], REC_LEN1, SORTPGNUM);
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

        boolean flag = true;

        switch (orderType){
            case OrderType.type1:
                //TODO need to research this one a little more
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
        return null;
    }
}