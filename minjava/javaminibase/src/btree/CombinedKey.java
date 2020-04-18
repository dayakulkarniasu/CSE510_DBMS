package btree;

import global.AttrType;
import global.GlobalConst;

/**
 * CombinedKey: It extends the KeyClass. It defines the combined Key.
 */
public class CombinedKey extends KeyClass {

    private String s1;
    private String s2;

    public String toString() {
        return new String(s1 + " " + s2);
    }

    public CombinedKey(String str1, String str2) {
        s1 = new String(str1);
        s2 = new String(str2);
    }

    /**
     * get a copy of the istring key
     *
     * @return the reference of the copy
     */
    public String[] getKey() {
        String[] rkey = new String[2];
        rkey[0] = new String(s1);
        rkey[1] = new String(s2);
        return rkey;
    }

    /**
     * set the string key value
     */
    public void setKey(String str1, String str2) {
        s1 = new String(str1);
        s2 = new String(str2);
    }
}
