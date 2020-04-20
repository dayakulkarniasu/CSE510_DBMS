package global;

import iterator.Iterator;
import iterator.Sort;

public class SortHelper {
    public static Iterator BuildSortOrder(Iterator fscan, AttrType[] attrType, short[] attrSize, int orderType, int numbuf){
        MapOrder ReturnOrder = new MapOrder(MapOrder.Ascending);
        // the fields we will sort
        int[] sort_flds = null;
        // the length of those fields
        int[] fld_lens = null;
        switch (orderType){
            //results ordered by rowLabel then columnLabel then time stamp
            case OrderType.type1:
                sort_flds = new int[]{1, 2, 4};
                fld_lens = new int[]{GlobalConst.STR_LEN, GlobalConst.STR_LEN, 4};
                break;
            //ordered columnLabel, rowLabel, timestamp
            case OrderType.type2:
                sort_flds = new int[]{2, 1, 4};
                fld_lens = new int[]{GlobalConst.STR_LEN, GlobalConst.STR_LEN, 4};
                break;
            //row label then timestamp
            case OrderType.type3:
                sort_flds = new int[]{1, 4};
                fld_lens = new int[]{GlobalConst.STR_LEN, 4};
                break;
            //column label then time stamp
            case OrderType.type4:
                sort_flds = new int[]{2, 4};
                fld_lens = new int[]{GlobalConst.STR_LEN, 4};
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
}
