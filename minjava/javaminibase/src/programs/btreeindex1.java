package index;

import btree.* ;
import global.*;
import heap.*;
import BigT.*;
import iterator.*;

import java.io.IOException;
import java.util.*;

public class btreeindex{
    static final short MAXLEN_S = 32;  //set max length of row label, column lable, values as 32
    static final short MAXLEN_COMB_KEY = 64; // size of two strings
    static final short MAXLEN_I = 4;	//set max length of timestamp, int as 4

    /**
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
}
