
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

	bigt bigtable;
	String btname;
	int type;
	int ordertype;
	String rowfilter;
	String columnfilter;
	String valuefilter;
	int numbuf;

	// constructor
	public Query(String _btname, int _type, int _ordertype, String _rowfilter, String _columnfilter,
			String _valuefilter, int _numbuf) {
		if (SystemDefs.JavabaseDB == null) {
			System.out.println("Database not exist.");
			System.exit(1);
		} else if (SystemDefs.JavabaseDB.table[0] == null) {
			System.out.println("Database does not have any Table.");
		} else {
			boolean found = false;
			int i;
			System.out.println("btname " + _btname);
			for (i = 0; i < SystemDefs.JavabaseDB.NumberOfTables; i++) {
				System.out.println("tables: " + i + " " + SystemDefs.JavabaseDB.table[i].name);
				if (_btname.equals(SystemDefs.JavabaseDB.table[i].name)) {
					SystemDefs.JavabaseDB.CurrentTableIndex = i;
					found = true;
					System.out.println("bigt: DB existing");
					System.out.println("bigDB tablename: " + SystemDefs.JavabaseDB.table[i].name);
				}
			}
			if (found == false) {
				System.out.println("Table name not match.");
			} else {
				bigtable = SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex];
				btname = _btname;
				type = _type;
				ordertype = _ordertype;
				rowfilter = _rowfilter;
				columnfilter = _columnfilter;
				valuefilter = _valuefilter;
				numbuf = _numbuf;
			}
		}
	}

	public void runquery() throws Exception {

		// Stream(bigt bigtable, int orderType, String rowFilter, String columnFilter,
		// String valueFilter)
		// type - index type
		// type 1 - no index
		// type 2 - rowLabel index
		// type 3 - columnLabel index
		// type 4 - columnLabel combined rowLabel index, and timestamp index
		// type 5 - rowLabel combined value index, and timestamp index
		int readBeforeQuery = PCounter.rcounter;
		int writeBeforeQuery = PCounter.wcounter;
		if (bigtable == null) {
			return;
		}
		Stream stream = new Stream(bigtable, ordertype, rowfilter, columnfilter, valuefilter, numbuf);

		System.out.println("Query: initialized stream.");

		Map map = null;
		AttrType[] types = new AttrType[4];
		types[0] = new AttrType(AttrType.attrString);
		types[1] = new AttrType(AttrType.attrString);
		types[2] = new AttrType(AttrType.attrString);
		types[3] = new AttrType(AttrType.attrInteger);

		try {
			map = stream.getNext();
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (map != null) {
			map.print(types);
			map = stream.getNext();
		}
		System.out.println("Diskpage read " + (PCounter.rcounter - readBeforeQuery) + " Disk page written "
				+ (PCounter.wcounter - writeBeforeQuery));
	}
}
