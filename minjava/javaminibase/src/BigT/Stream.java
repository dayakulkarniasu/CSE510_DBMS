package BigT;

import global.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.*;
import heap.*;
import iterator.Iterator;
import java.io.IOException;

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
    private Iterator QueryResultSet = null;

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
    public Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter,
            String valueFilter, int numbuf) throws InvalidTupleSizeException, IOException, IndexException,
            InvalidTypeException, UnknownIndexTypeException {

        _bigTable = bigtable;
        AttrType[] attrType = MapSchema.MapAttrType();
        short[] attrSize = MapSchema.MapStrLengths();
        // create empty map we will use for reading data
        Map m = new Map();
        try {
            // set the header info for the new map
            m.setHdr((short) 4, attrType, attrSize);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        // used to define what output of fscan will look like
        FldSpec[] schema = MapSchema.OutputMapSchema();
        // OutFilter for limiting results from BigTable search
        CondExpr[] outFilter = QueryHelper.queryFilter(rowFilter, columnFilter, valueFilter);

        FileScan fscan = null;

        System.out.println("rowfilter: " + rowFilter + " colfilter: " + columnFilter + " valfilter: " + valueFilter);

        try {
            fscan = new FileScan(bigtable.name, attrType, attrSize, (short) 4, 4, schema, outFilter[0] == null ? null : outFilter );
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        m = null;
        MID mid = new MID();

        Iterator queryResults = BuildSortOrder(fscan, attrType, attrSize, orderType, numbuf);
        QueryResultSet = queryResults;
    }

    private Iterator BuildSortOrder(Iterator fscan, AttrType[] attrType, short[] attrSize, int orderType, int numbuf){
        MapOrder ReturnOrder = new MapOrder(MapOrder.Ascending);
        // the fields we will sort
        int[] sort_flds = null;
        // the length of those fields
        int[] fld_lens = null;
        switch (orderType){
            //results ordered by rowLabel then columnLabel then time stamp
            case OrderType.type1:
                sort_flds = new int[]{1, 2, 4};
                fld_lens = new int[]{STR_LEN, STR_LEN, 4};
            break;
            //ordered columnLabel, rowLabel, timestamp
            case OrderType.type2:
                sort_flds = new int[]{2, 1, 4};
                fld_lens = new int[]{STR_LEN, STR_LEN, 4};
                break;
            //row label then timestamp
            case OrderType.type3:
                sort_flds = new int[]{1, 4};
                fld_lens = new int[]{STR_LEN, 4};
                break;
            //column label then time stamp
            case OrderType.type4:
                sort_flds = new int[]{2, 4};
                fld_lens = new int[]{STR_LEN, 4};
                break;
            //time stamp
            case OrderType.type5:
                break;
        }
        Sort sort = null;
        try{
            if(orderType == OrderType.type5){
                sort = new Sort(attrType, (short) 4, attrSize, fscan, 4, ReturnOrder, 4, numbuf);
            }
            else
                sort = new Sort(attrType, (short) 4, attrSize, fscan, sort_flds, ReturnOrder, fld_lens, numbuf);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return sort;
    }

    /**
     * Retrieves the next map in the sorted order stream.
     * @return
     */
    public Map getNext() {
        Map recptrmap = null;
        try {
            recptrmap = QueryResultSet.get_next();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        nscan++;
        return recptrmap;

    }

    public Iterator GetStreamIterator(){
        return QueryResultSet;
    }
}
