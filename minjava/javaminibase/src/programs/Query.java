package programs;

import btree.BTreeFile;
import global.*;
import BigT.*;
import heap.Tuple;
import java.io.*;
import java.util.StringTokenizer;
import diskmgr.PCounter;
import iterator.*;

//piazza notes?: So I think we can clear this hashtable using flushAllPages() -> this method writes all the dirty pages to disk and clears the hashtable or 
//create a new instance of buffer manager everytime, but I think making it work with the flushAllPages() would be a better solution-need to be double check

public class Query {

	public static void main(String args[]) throws Exception {
		// Query BIGTABLENAME, TYPE, ORDERTYPE, ROWFILTER, COLUMNFILTER, VALUEFILTER,
		// NUMBUF
		// Example Query: query ExampleBTable 3 4 Uruguay * [050000, 090000] 200
		// Type: btree index type: 1-5 Ordertype: ascendingorder in row, column,
		// timestamp, and combination: 1-5
		// [S,Z] :range of rowfilter, no column and valuefilter

		String bigtablename = args[0];
		int type = Integer.parseInt(args[1]);
		int ordertype = Integer.parseInt(args[2]);
		String[] rowfilter = args[3];
		String[] columnfilter = args[4];
		String[] valuefilter = args[5];
		int numbuf = Integer.parseInt(args[6]);

		// TODO System Defs
		// SystemDefsBigDB sysDefs = new SystemDefsBigDB();

		runquery(bigtablename, type, ordertype, rowfilter, columnfilter, valuefilter, numbuf);

		// SystemDefs.JavabaseBM.flushAllPages(); //?-copied from minibase, but might
		// need to modify
		// SystemDefs.JavabaseDB.closebigDB();

		System.out.println("Reads: " + PCounter.rcounter);
		System.out.println("Writes: " + PCounter.wcounter);
	}

	public static void runquery(String bigtablename, Integer type, Integer ordertype, String rowfilter, String columnfilter,  String valuefilter, numbuf) throws Exception {

		//Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter, String valueFilter)
		// type - index type
		// type 1 - no index
		// type 2 - rowLabel index
		// type 3 - columnLabel index
		// type 4 - columnLabel combined rowLabel index, and timestamp index
		// type 5 - rowLabel combined value index, and timestamp index

		BufferedReader br = null;
		br = new BufferedReader(bigtablename)); // need to check BufferReader, type included?
		Heapfile hf =  ?
		switch (type)
		{
		case 1:
           //type == 1;
		   bigT.Stream(bigt br, int orderType, String rowFilter, String columnFilter, String valueFilter);

	query_op=

	hf.openscan
		   // while loop
		   //*
		   //signle value match
		   //range search
		   
		   System.out.println("select items:"+query_op);break;case 2:
	// type == 2;
	bigT.Stream(bigt br, int orderType, String rowFilter, String columnFilter, String valueFilter);
	// hf.openscan
	System.out.println("select items"+query_op);break;case 3:
	// type == 3;
	bigT.Stream(bigt br, int orderType, String rowFilter, String columnFilter, String valueFilter);
	// hf.openscan
	System.out.println("select items"+query_op);break;case 4:
	// type == 4;
	bigT.Stream(bigt br, int orderType, String rowFilter, String columnFilter, String valueFilter);
	// hf.openscan
	System.out.println("select items"+query_op);break;case 5:
	// type == 5;
	bigT.Stream(bigt br, int orderType, String rowFilter, String columnFilter, String valueFilter);
	// hf.openscan
	System.out.println("select items"+query_op);break;}}catch(

	Exception e)
	{
		e.printStackTrace();
	}
}
