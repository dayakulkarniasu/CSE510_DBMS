package programs;

import btree.* ;
import global.*;
import heap.*;
import BitT.*;
import iterator.*;

import java.io.IOException;
import java.util.*;

// get rowLabel (String), columnLabel (String), timestamp (Integer) from BigT.Map
// code refers to IndexTest.java

public class btreeindex{

	//boolean status = true;
    //AttrType[] attrType=new AttrType[2];
    //attrType[0] = new AttrType(AttrType.attrString);
    //attrType[1] = new AttrType(AttrType.attrString);
	BTreeFile btf  = null;
	
	
	public void BTreeIndex_Row(){
		MID mid = new MID();
		
		//create an index file
        int keyType = AttrType.attrString;
        int keySize = bigt.getRowCnt();   //get number of row labels
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+rowLabel, keyType, keySize, delete_Fashion); 
		
		//insert index key
		Map temp = null; 
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            int temp_key = temp.getRowLabel();    
            btf.insert(new btree.StringKey(temp_key), mid);
		}
	System.out.println("Successfully created BTree index on RowLabel !");
	}


	public void BTreeIndex_Col() {
		
		MID mid = new MID();
		//create an index file
        int keyType = AttrType.attrString;
        int keySize = bigt.getColumnCnt();   //get number of column labels
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+colLabel, keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null;     
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            int temp_key = temp.getColumnLabel();    
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on ColumnLabel !");
	}

	public BTreeIndex_TS() {
		
		MID mid = new MID();
		//create an index file
        int keyType = AttrType.attrInteger;
        int keySize = bigt.getMapCnt()  //get number of timestamp?
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+timestamp, keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            int temp_key = temp.getTimeStamp();   
            btf.insert(new btree.IntegerKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on TimeStamp !");
	}

	public BTreeIndex_Val() {
		
		MID mid = new MID();
		//create index file
        int keyType = AttrType.attrString;
        int keySize = bigt.getMapCnt()  //get number of values?
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+timestamp, keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            int temp_key = temp.getValue();   
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on Value !");
	}
	
	public BTreeIndex_RowCol() {
		
		MID mid = new MID();
		//create index file
        int keyType = AttrType.attrString;
        int keySize1 = bigt.getRowCnt();   //get number of row labels 
		int keySize2 = bigt.getColumnCnt();   //get number of column labels
		int keySize = max(keySize1, keySize2);
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+rowcol, keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key1 = null;
		String temp_key2 = null;
        while (temp = Map.getNext(mid))!= null) {
            int temp_key1 = temp.getRowLabel();    
			int temp_key2 = temp.getColLabel();
            btf.insert(new btree.StringKey(temp_key1), mid);
			btf.insert(new btree.StringKey(temp_key2), mid);
			
        }
		System.out.println("Successfully created BTree index on RowLabel and ColumnLabel !");
	}
	
	public BTreeIndex_RowVal() {
		
		MID mid = new MID();
		//create index file
        int keyType = AttrType.attrString;
        int keySize1 = bigt.getRowCnt();   //get number of row labels 
		int keySize2 = bigt.getMapCnt();   //get number of values
		int keySize = max(keySize1, keySize2);
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btree"+rowval, keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key1 = null;
		String temp_key2 = null;
        while (temp = Map.getNext(mid))!= null) {
            int temp_key1 = temp.getRowLabel();    
			int temp_key2 = temp.getValue();
            btf.insert(new btree.StringKey(temp_key1), mid);
			btf.insert(new btree.StringKey(temp_key2), mid);
			
        }
		System.out.println("Successfully created BTree index on RowLabel and Value !");
	}
	
}