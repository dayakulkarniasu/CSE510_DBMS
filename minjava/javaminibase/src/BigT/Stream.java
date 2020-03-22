package BigT;

import global.*;

/**
 * This class will be similar to heap.Scan, however, will provide different
 * types of accesses to the bigtable
 */
public class Stream implements GlobalConst {
    /**
     * Initialize a stream of maps on bigtable.
     * 
     * @param bigtable
     * @param orderType
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     */
    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter) {
    }

    /**
     * Closes the stream object.
     */
    public void closestream() {
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