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
    short MAXLEN_S = 32;  //set max length of row label, column lable, values as 32
    short MAXLEN_COMB_KEY = 64; // size of two strings
    short MAXLEN_I = 4;	//set max length of timestamp, int as 4

    public BTreeFile IndexForOneKey(java.lang.String labelName){
        //create an index file
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_S;
        int delete_Fashion = 1;
        BTreeFile btf = null;
        try {
            btf = new BTreeFile(String.format("btree-",labelName), keyType, keySize, delete_Fashion);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Successfully created BTree index on ", labelName, "!"));
        return btf;
    }

    public BTreeFile IndexForOneKey(int labelName){
        //create an index file
        int keyType = AttrType.attrInteger;
        int keySize = MAXLEN_I;
        int delete_Fashion = 1;
        BTreeFile btf = null;
        try {
            btf = new BTreeFile(String.format("btree-",labelName), keyType, keySize, delete_Fashion);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Successfully created BTree index on ", labelName, "!"));
        return btf;
    }

    public BTreeFile IndexForCombinedKey(java.lang.String labelName1, java.lang.String labelName2) {
        int keyType = AttrType.attrString;
        int keySize = MAXLEN_COMB_KEY;
        int delete_Fashion =1;
        String keyName = labelName1 + " " +labelName2;
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