package BigT;

import global.GlobalConst;
import global.MID;
import global.MapSchema;
import global.SystemDefs;
import heap.*;

public class VirtualBigTable {
    public static bigt Create(String btName)
            throws  heap.InvalidTupleSizeException,
            heap.HFException,
            heap.HFBufMgrException,
            heap.HFDiskMgrException,
            heap.InvalidTupleSizeException,
            heap.InvalidSlotNumberException,
            heap.SpaceNotAvailableException,
            heap.InvalidTypeException,
            java.io.IOException{
        // Create Temporary Heap File to store all maps in the bigT
        bigt temp = new bigt();
        for(bigt bigt : SystemDefs.JavabaseDB.table){
            if(bigt.name.equalsIgnoreCase(btName)){
                Scan scan = bigt.hf.openScan();
                Map map = new Map(GlobalConst.MAP_LEN);
                map.setHdr((short) MapSchema.MapFldCount(),MapSchema.MapAttrType(), MapSchema.MapStrLengths());
                MID mid = new MID();
                map = scan.getNext(mid);
                while(map  != null){
                    Map amap = new Map(map.getMapByteArray(), 0, GlobalConst.MAP_LEN);
                    map = scan.getNext(mid);
                    mid = temp.hf.insertMap(amap.getMapByteArray());
                }
                scan.closescan();
            }
        }
        return temp;
    }
}
