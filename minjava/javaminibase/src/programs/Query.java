package programs;

import global.*;
import iterator.MapUtils;

import java.util.concurrent.TimeUnit;

import BigT.*;
import diskmgr.PCounter;

//piazza notes?: So I think we can clear this hashtable using flushAllPages() -> this method writes all the dirty pages to disk and clears the hashtable or 
//create a new instance of buffer manager everytime, but I think making it work with the flushAllPages() would be a better solution-need to be double check

public class Query {

	bigt bigtable;
	String btname;
	int ordertype;
	String rowfilter;
	String columnfilter;
	String valuefilter;
	int numbuf;

	// constructor
	public Query(String _btname, int _ordertype, String _rowfilter, String _columnfilter, String _valuefilter,
			int _numbuf) {
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
				try {
					bigtable = VirtualBigTable.Create(_btname);
					btname = _btname;
					ordertype = _ordertype;
					rowfilter = _rowfilter;
					columnfilter = _columnfilter;
					valuefilter = _valuefilter;
					numbuf = _numbuf;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	// original codes
	// public void runquery() throws Exception {
	// int readBeforeQuery = PCounter.rcounter;
	// int writeBeforeQuery = PCounter.wcounter;
	// if (bigtable == null) {
	// return;
	// }
	// Stream stream = new Stream(bigtable, ordertype, rowfilter, columnfilter,
	// valuefilter, numbuf);

	// System.out.println("Query: initialized stream.");

	// Map map = null;
	// AttrType[] types = MapSchema.MapAttrType();

	// try {
	// map = stream.getNext();
	// } catch (Exception e) {
	// e.printStackTrace();
	// }

	// while (map != null) {
	// map.print(types);
	// map = stream.getNext();
	// }
	// System.out.println("Diskpage read " + (PCounter.rcounter - readBeforeQuery) +
	// " Disk page written "
	// + (PCounter.wcounter - writeBeforeQuery));
	// bigtable.hf.deleteFile();
	// }//original codes end

	//eliminate duplicates in returned records
	public void runquery() throws Exception {
		int readBeforeQuery = PCounter.rcounter;
		int writeBeforeQuery = PCounter.wcounter;
		if (bigtable == null) {
			return;
		}
		Stream stream = new Stream(bigtable, ordertype, rowfilter, columnfilter, valuefilter, numbuf);

		System.out.println("Query: initialized stream.");

		Map map = null;
		AttrType[] types = MapSchema.MapAttrType();

		try {
			Map _map = stream.getNext();
			if (_map != null)
				map = new Map(_map);
			else {
				map = _map;
				System.out.println("No matches.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (map != null) {
			map.print(types);
			Map tmp_map = stream.getNext();
			while (tmp_map != null && MapUtils.Equal(map, tmp_map, types, 4)) {
				tmp_map = stream.getNext();
			}
			if (tmp_map == null)
				break;
			map = new Map(tmp_map);
		}
		System.out.println("Diskpage read " + (PCounter.rcounter - readBeforeQuery) + " Disk page written "
				+ (PCounter.wcounter - writeBeforeQuery));
		// bigtable.hf.deleteFile();
		stream.close();
	}
}
