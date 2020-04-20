package global;

import BigT.Map;
import btree.CombinedKey;
import btree.StringKey;
import heap.Heapfile;
import iterator.Iterator;

public class HeapHelper {
    public static Heapfile BuildHeap(Iterator am)
        throws Exception{
        Heapfile hf = new Heapfile("rowjoin");
        Map temp = new Map(GlobalConst.MAP_LEN);
        temp.setHdr((short) MapSchema.MapFldCount(), MapSchema.MapAttrType(), MapSchema.MapStrLengths());
        MID mid = new MID();
        temp = am.get_next();
        while (temp != null) {
            Map amap = new Map(temp.getMapByteArray(), 0, GlobalConst.MAP_LEN);
            mid = hf.insertMap(amap.getMapByteArray());
            temp = am.get_next();
        }
        return hf;
    }
}
