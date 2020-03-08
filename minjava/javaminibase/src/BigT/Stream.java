package BigT;
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
    Stream(bigt bigtable, int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter){}

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