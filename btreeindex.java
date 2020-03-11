import btree.* ;
import global.*;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.*;

// get rowLabel (String), columnLabel (String), timestamp (Integer) from BigT.Map
// code refers to IndexTest.java

rowLabel = getRowLabel();
colLabel = getColumnLabel();
timestamp = getTimeStamp();

 public BTreeIndex_ROW(String rowLabel) {
		
		MID mid = new MID();

        int keyType = AttrType.attrString;
        int keySize = size(rowLabel);   //???size() get size of rowLabel, not the whole tuple???
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+rowLabel, keyType, keySize, delete_Fashion);   
        
		Tuple temp = null;     // ???not sure if Tuple is correct defined???
		String temp_key = null;
        while (temp = Map.getNext(mid)!= null) {
            int temp_key = temp.getStrFld(1);    // not sure if 1 is correct
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on RowLabel !");
    }

 public BTreeIndex_ROW(String rowLabel) {
		
		MID mid = new MID();

        int keyType = AttrType.attrString;
        int keySize = size(rowLabel);   //???size() get size of rowLabel, not the whole tuple???
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+rowLabel, keyType, keySize, delete_Fashion);   
        
		Tuple temp = null;     // ???not sure if Tuple is correct defined???
		String temp_key = null;
        while (temp = Map.getNext(mid)!= null) {
            int temp_key = temp.getStrFld(1);    // not sure if 1 is correct
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on RowLabel !");
    }

 public BTreeIndex_COL(String ColLabel) {
		
		MID mid = new MID();

        int keyType = AttrType.attrString;
        int keySize = size(colLabel);   //???size() get size of colLabel, not the whole tuple???
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+colLabel, keyType, keySize, delete_Fashion);   
        
		Tuple temp = null;     // ???not sure if Tuple is correct defined???
		String temp_key = null;
        while (temp = Map.getNext(mid)!= null) {
            int temp_key = temp.getStrFld(2);    // not sure if 2 is correct
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on ColumnLabel !");
    }

 public BTreeIndex_TS(Integer timestamp) {
		
		MID mid = new MID();

        int keyType = AttrType.attrInteger;
        int keySize = size(timestamp);   //???size() get size of timestamp, not the whole tuple???
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+timestamp, keyType, keySize, delete_Fashion);   
        
		Tuple temp = null;     // ???not sure if Tuple is correct defined???
		String temp_key = null;
        while (temp = Map.getNext(mid)!= null) {
            int temp_key = temp.getIntFld(3);    // not sure if 3 is correct
            btf.insert(new btree.IntegerKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on TimeStamp !");
    }
