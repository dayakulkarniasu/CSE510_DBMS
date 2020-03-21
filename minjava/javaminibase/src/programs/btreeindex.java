package programs;

import global.*;
import heap.Scan;
import BigT.Map;
import btree.BTreeFile;

/**
 * get rowLabel (String), columnLabel (String), timestamp (Integer) from
 * BigT.Map code refers to IndexTest.java
 */
public class btreeindex implements GlobalConst {

    public static void BTreeIndex_Row(String rowLabel) {

        MID mid = new MID();
        int keyType = AttrType.attrString;
        int keySize = rowLabel.length();
        int delete_Fashion = 1;

        // create the index file
        BTreeFile btf = null;
        try {
            btf = new BTreeFile("BTreeIndex", keyType, keySize, delete_Fashion/* delete */);
        } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
        // create an scan on the heapfile
        Scan scan = null;
        try {
            scan = new Scan(f);
          } catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
          }

        Map temp = null;
        String temp_key = null;
        // while (temp = Map.getNext(mid))!= null) {
        // int temp_key = temp.getRowLabel();
        // btf.insert(new btree.StringKey(temp_key), mid);

        // }
        System.out.println("Successfully created BTree index on RowLabel !");
    }
}