package programs;

import global.*;
import BigT.Map;

/**
 * get rowLabel (String), columnLabel (String), timestamp (Integer) from BigT.Map
*code refers to IndexTest.java
 */
public class btreeindex implements GlobalConst{
    boolean status = true;
    AttrType[] attrType=new AttrType[2];
    attrType[0] = new AttrType(AttrType.attrString);
    attrType[1] = new AttrType(AttrType.attrString);
}

    // create the index file
//     BTreeFile btf = null;

//  public static void BTreeIndex_Row(String rowLabel) {

// 		MID mid = new MID();

//         int keyType = AttrType.attrString;
//         int keySize = rowLabel.length(); 
// 		int delete_Fashion =1;
//         BTreeFile btf = new BTreeFile("btree"+rowLabel, keyType, keySize, delete_Fashion);

// 		Map temp = null;
// 		String temp_key = null;
//         while (temp = Map.getNext(mid))!= null) {
//             int temp_key = temp.getRowLabel();
//             btf.insert(new btree.StringKey(temp_key), mid);

//         }
// 		System.out.println("Successfully created BTree index on RowLabel !");
//     }

// public static BTreeIndex_Col(String colLabel) {

// MID mid = new MID();

// int keyType = AttrType.attrString;
// int keySize = size(colLabel); //???size() get size of colLabel
// int delete_Fashion =1;
// BTreeFile btf = new BTreeFile("btree"+colLabel, keyType, keySize,
// delete_Fashion);

// Map temp = null;
// String temp_key = null;
// while (temp = Map.getNext(mid))!= null) {
// int temp_key = temp.getColumnLabel();
// btf.insert(new btree.StringKey(temp_key), mid);

// }
// System.out.println("Successfully created BTree index on ColumnLabel !");
// }

// public static BTreeIndex_TS(Integer timestamp) {

// MID mid = new MID();

// int keyType = AttrType.attrInteger;
// int keySize = size(timestamp); //???size() get size of timestamp
// int delete_Fashion =1;
// BTreeFile btf = new BTreeFile("btree"+timestamp, keyType, keySize,
// delete_Fashion);

// Map temp = null;
// String temp_key = null;
// while (temp = Map.getNext(mid))!= null) {
// int temp_key = temp.getTimeStamp();
// btf.insert(new btree.IntegerKey(temp_key), mid);

// }
// System.out.println("Successfully created BTree index on TimeStamp !");
// }

// public static BTreeIndex_Val(String value) {

// MID mid = new MID();

// int keyType = AttrType.attrString;
// int keySize = size(value); //???size() get size of value
// int delete_Fashion =1;
// BTreeFile btf = new BTreeFile("btree"+timestamp, keyType, keySize,
// delete_Fashion);

// Map temp = null;
// String temp_key = null;
// while (temp = Map.getNext(mid))!= null) {
// int temp_key = temp.getTimeStamp();
// btf.insert(new btree.StringKey(temp_key), mid);

// }
// System.out.println("Successfully created BTree index on Value !");
// }

// public static BTreeIndex_RowCol(String rowLabel, String colLabel) {

// MID mid = new MID();

// int keyType = AttrType.attrString;
// int keySize1 = size(rowLabel);
// int keySize2 = size(colLabel);
// int keySize = max(keySize1, keySize2);
// int delete_Fashion =1;
// BTreeFile btf = new BTreeFile("btree"+rowcol, keyType, keySize,
// delete_Fashion);

// Map temp = null;
// String temp_key1 = null;
// String temp_key2 = null;
// while (temp = Map.getNext(mid))!= null) {
// int temp_key1 = temp.getRowLabel();
// int temp_key2 = temp.getColLabel();
// btf.insert(new btree.StringKey(temp_key1), mid);
// btf.insert(new btree.StringKey(temp_key2), mid);

// }
// System.out.println("Successfully created BTree index on RowLabel and
// ColumnLabel !");
// }

// public BTreeIndex_RowVal(String rowLabel, String value) {

// MID mid = new MID();

// int keyType = AttrType.attrString;
// int keySize1 = size(rowLabel);
// int keySize2 = size(value);
// int keySize = max(keySize1, keySize2);
// int delete_Fashion =1;
// BTreeFile btf = new BTreeFile("btree"+rowval, keyType, keySize,
// delete_Fashion);

// Map temp = null;
// String temp_key1 = null;
// String temp_key2 = null;
// while (temp = Map.getNext(mid))!= null) {
// int temp_key1 = temp.getRowLabel();
// int temp_key2 = temp.getValue();
// btf.insert(new btree.StringKey(temp_key1), mid);
// btf.insert(new btree.StringKey(temp_key2), mid);

// }
// System.out.println("Successfully created BTree index on RowLabel and Value
// !");

