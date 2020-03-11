package BigT;

import global.*;

/**
 * This class will be similar to heap.Scan, however, will provide different types of accesses to the bigtable
 */
public class Stream{
    /**
     * Initialize a stream of maps on bigtable.
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
        switch (orderType){
            case DBType.type1:
                //TODO need to research this one a little more
                //no index
                break;
            case DBType.type2:
                //one btree to index row labels
                break;
            case DBType.type3:
                // one btree to index column labels
                break;
            case DBType.type4:
                // one btree to index column label and row label (combined key)
                // one btree to index time stamps
                break;
            case DBType.type5:
                // one btree to index row label and value (combined key)
                // one btree to index time stamps
                break;
            default:

        }
    }

    /**
     * Closes the stream object.
     */
    void closestream(){}

    /**
     * Retrieve the next map in the stream.
     * @param mid
     * @return
     */
    Map getNext(MID mid){}
}