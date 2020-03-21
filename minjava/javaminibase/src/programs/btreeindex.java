package programs;

import btree.* ;
import global.*;
import heap.*;
import BigT.*;
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
	MAXLEN_S = 32;  //set max length of row label, column lable, values as 32
	MAXLEN_I = 4;	//set max length of timestamp, int as 4
	
	public void BTreeIndex_Row(){
		MID mid = new MID();
		
		//create an index file
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_S;
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btreerow", keyType, keySize, delete_Fashion); 
		
		//insert index key
		Map temp = null; 
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            temp_key = temp.getRowLabel();    
            btf.insert(new btree.StringKey(temp_key), mid);
		}
	System.out.println("Successfully created BTree index on RowLabel !");
	}


	public void BTreeIndex_Col() {
		
		MID mid = new MID();
		//create an index file
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_S;
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btreecol", keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null;     
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            temp_key = temp.getColumnLabel();    
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on ColumnLabel !");
	}

	public void BTreeIndex_TS() {
		
		MID mid = new MID();
		//create an index file
        int keyType = AttrType.attrInteger;
        int keySize = MAXLEN_I;
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btreeTS", keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		Integer temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            temp_key = temp.getTimeStamp();   
            btf.insert(new btree.IntegerKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on TimeStamp !");
	}

	public void BTreeIndex_Val() {
		
		MID mid = new MID();
		//create index file
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_S;
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btreeval", keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            temp_key = temp.getValue();   
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on Value !");
	}
	
	public void BTreeIndex_ColRow() {
		
		MID mid = new MID();
		//create index file
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_S;
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btreecolrow", keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key1 = null;
		String temp_key2 = null;
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            temp_key1 = temp.getColLabel();    
			temp_key2 = temp.getRowLabel();
			temp_key = temp_key1 + " " + temp_key2;			
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on Column Label and Row Label !");
	}
	
	public void BTreeIndex_RowVal() {
		
		MID mid = new MID();
		//create index file
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_S;
		int delete_Fashion =1;
        BTreeFile btf = new BTreeFile("btreerowval", keyType, keySize, delete_Fashion);   
        //insert index key
		Map temp = null; 
		String temp_key1 = null;
		String temp_key2 = null;
		String temp_key = null;
        while (temp = Map.getNext(mid))!= null) {
            temp_key1 = temp.getRowLabel();    
			temp_key2 = temp.getValue();
			temp_key = temp_key1 + " " + temp_key2;
            btf.insert(new btree.StringKey(temp_key), mid);
			
        }
		System.out.println("Successfully created BTree index on Row Label and Value !");
	}
	
}
