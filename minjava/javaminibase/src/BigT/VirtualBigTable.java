package BigT;

import java.util.concurrent.TimeUnit;

import global.GlobalConst;
import global.MID;
import global.MapSchema;
import global.SystemDefs;
import heap.*;
import programs.batchInsert;

public class VirtualBigTable {
    public static bigt Create(String btName)
            throws heap.InvalidTupleSizeException, heap.HFException, heap.HFBufMgrException, heap.HFDiskMgrException,
            heap.InvalidTupleSizeException, heap.InvalidSlotNumberException, heap.SpaceNotAvailableException,
            heap.InvalidTypeException, java.io.IOException, InterruptedException {
        // Create Temporary Heap File to store all maps in the bigT
        bigt temp = new bigt();
        for (bigt bigt : SystemDefs.JavabaseDB.table) {
            System.out.println("bigt.name:" + bigt.name);
            if (bigt.name.equalsIgnoreCase(btName)) {
                Scan scan = bigt.hf.openScan();
                Map map = new Map(GlobalConst.MAP_LEN);
                map.setHdr((short) MapSchema.MapFldCount(), MapSchema.MapAttrType(), MapSchema.MapStrLengths());
                MID mid = new MID();
                map = scan.getNext(mid);
                while (map != null) {
                    Map amap = new Map(map.getMapByteArray(), 0, GlobalConst.MAP_LEN);
                    map = scan.getNext(mid);

                    // boolean found_delete_flag = batchInsert.found_delete(temp.hf,
                    // map.getRowLabel(),
                    // map.getColumnLabel(), map.getTimeStamp(), map.getValue());

                    // if (found_delete_flag /* found delete flag is false */ ) {
                    mid = temp.hf.insertMap(amap.getMapByteArray());
                    // }
                }

                // System.out.println(" ********** VirtualBigTable END **********");
                // System.in.read();

                scan.closescan();
            }
        }
        return temp;
    }
}
