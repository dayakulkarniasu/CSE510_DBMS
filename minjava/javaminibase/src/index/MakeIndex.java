package index;

import BigT.Map;
import btree.* ;
import global.*;
import heap.*;
import BigT.*;
import iterator.*;
import java.io.IOException;
import java.util.*;

public class MakeIndex {
    static final short MAXLEN_S = 32;  //set max length of row label, column lable, values as 32
    static final short MAXLEN_COMB_KEY = 64; // size of two strings
    static final short MAXLEN_I = 4;	//set max length of timestamp, int as 4

    /**
     * Instantiates an index file for use during the insertion process.
     * @param propName The property name we'll build an index on: Row, Column, TimeStamp, Value
     * @return a btf file that will be assigned to the bigDB class and accessible globally
     */
    public static BTreeFile IndexForOneKey(java.lang.String propName){
        //create an index file
        int keyType = propName.equalsIgnoreCase("timestamp") ? AttrType.attrInteger : AttrType.attrString;
        int keySize = propName.equalsIgnoreCase("timestamp") ? MAXLEN_I : MAXLEN_S;
        int delete_Fashion = 1;
        BTreeFile btf = null;
        try {
            btf = new BTreeFile(String.format("btree-",propName), keyType, keySize, delete_Fashion);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Successfully created BTree index on ", propName, "!"));
        return btf;
    }

    /**
     * Instantiates an index file for use during the insertion process.
     * @param propName1 the name of the property we are indexing on that will be used to create a combined key
     * @param propName2 the name of the property we are indexing on that will be used to create a combined key
     * @return a btf file with a combined key setup that will be assigned to the bigDB class and accessible globally
     */
    public static BTreeFile IndexForCombinedKey(java.lang.String propName1, java.lang.String propName2) {
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_COMB_KEY;
        int delete_Fashion =1;
        String keyName = propName1 + " " +propName2;
        BTreeFile btf = null;
        try {
            btf = new BTreeFile(String.format("btree-", keyName), keyType, keySize, delete_Fashion);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Successfully created BTree index on", keyName, "!"));
        return btf;
    }

    /**
     * @param amap The map that has been inserted into the BigTable. We use this object to get the relevant fields to
     *             create the key that will be used in the btree
     * @param mid The MapId for the newly created Map record. This will serve as the pointer to our record for the btree
     *            index
     */
    public static void InsertIntoIndex(Map amap , MID mid){
        String key = null;
        int key2 = 0;
        try{
            switch (SystemDefs.JavabaseDB.dbType){
                case 1:
                    // No Index
                    break;
                case 2:
                    // Index on RowLabel
                    key = amap.getRowLabel();
                    SystemDefs.JavabaseDB.indexStrat1.insert(new StringKey(key), mid);
                    break;
                case 3:
                    // Index on ColumnLabel
                    key = amap.getColumnLabel();
                    SystemDefs.JavabaseDB.indexStrat1.insert(new StringKey(key), mid);
                    break;
                case 4:
                    // Index on combKey for ColumnLabel and RowLabel & Index on TimeStamp
                    key = String.format(amap.getColumnLabel(), " ", amap.getRowLabel());
                    key2 = amap.getTimeStamp();
                    SystemDefs.JavabaseDB.indexStrat1.insert(new StringKey(key), mid);
                    SystemDefs.JavabaseDB.indexStrat2.insert(new IntegerKey(key2), mid);
                    break;
                case 5:
                    // Index on combKey for RowLabel and Value & Index on TimeStamp
                    key = String.format(amap.getRowLabel(), " ", amap.getValue());
                    key2 = amap.getTimeStamp();
                    SystemDefs.JavabaseDB.indexStrat1.insert(new StringKey(key), mid);
                    SystemDefs.JavabaseDB.indexStrat2.insert(new IntegerKey(key2), mid);
                    break;
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
