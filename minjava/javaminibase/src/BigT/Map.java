package BigT;

import java.io.*;
import java.lang.*;
import global.*;

/**
 * You will need to create a class BigT.Map, similar to heap.Tuple but having a
 * ﬁxed structure (and thus a ﬁxed header) as described above. Thus, the
 * constructor and get/set methods associated with the BigT.Map should be
 * adapted as appropriate
 */
public class Map implements GlobalConst {

    /**
     * Maximum size of any map
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte[] data;

    /**
     * start position of this map in data[]
     */
    private int map_offset;

    /**
     * length of this map
     */
    private int map_length;

    /**
     * private field Number of fields in this map
     */
    private static final int fldCnt = 4;

    /**
     * private field Array of offsets of the fields
     */
    private short[] fldOffset;

    /**
     * Define the Map structure
     *
     **/
    private String rowLabel;
    private String columnLabel;
    private int timeStamp;
    private String value;

    /**
     * Class constructor create a new map with the appropriate size.
     */
    public Map() {
        // Creat a new map
        data = new byte[max_size];
        map_offset = 0;
        map_length = max_size;
    }

    /**
     * Construct a map from a byte array.
     * 
     * @param amap   a byte array which contains the map
     * @param offset the offset of the map in the byte array
     */
    public Map(byte[] amap, int offset) {
        data = amap;
        map_offset = offset;
    }

    /**
     * Construct a map from another map through copy.
     * 
     * @param fromMap
     */
    public Map(Map fromMap) {
        data = fromMap.getMapByteArray();
        map_length = fromMap.getLength();
        map_offset = 0;
        fldOffset = fromMap.copyFldOffset();
    }

    /**
     * Returns the row label.
     * 
     * @return
     */
    public String getRowLabel() {
        return rowLabel;
    }

    /**
     * Returns the column label.
     * 
     * @return
     */
    public String getColumnLabel() {
        return columnLabel;
    }

    /**
     * Returns the timestamp.
     * 
     * @return
     */
    public int getTimeStamp() {
        return timeStamp;
    }

    /**
     * Returns the value.
     * 
     * @return
     */
    public String getValue() {
        return value;
    }

    /**
     * Set the row label.
     * 
     * @param val
     * @return
     */
    public Map setRowLabel(String val){
       rowLabel=val;
       return this;
    }

    /**
     * Set the column label.
     * 
     * @param val
     * @return
     */
    public Map setColumnLabel(String val) {
        columnLabel=val;
        return this;
    }

    /**
     * Set the timestamp.
     * 
     * @param val
     * @return
     */
    public Map setTimeStamp(int val) {
        timeStamp=val;
        return this;
    }

    /**
     * Set the value.
     * 
     * @param val
     * @return
     */
    public Map setValue(String val) {
        value=val;
        return this;
    }

    /**
     * Copy the map to byte array out.
     * 
     * @return byte[], a byte array contains the map, the length of byte[] = length
     *         of the map
     */
    public byte[] getMapByteArray() {
        byte[] mapcopy = new byte[map_length];
        System.arraycopy(data, map_offset, mapcopy, 0, map_length);
        return mapcopy;
    }

    /**
     * Print out the map.
     */
    public void print() {
        System.out.println("row:"+rowLabel+", column:"+columnLabel+", time:"+timeStamp);
    }

    /**
     * get the length of a map, call this method if you did call setHdr () before
     * 
     * @return size of this tuple in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    /**
     * Copy the given map
     * 
     * @param fromMap
     */
    public void mapCopy(Map fromMap) {
    }

    /**
     * This is used when you don’t want to use the constructor
     * 
     * @param amap
     * @param offset
     */
    public void mapInit(byte[] amap, int offset) {
    }

    /**
     * Set a map with the given byte array and offset.
     * 
     * @param frommap
     * @param offset
     */
    public void mapSet(byte[] frommap, int offset) {
    }

    // ----------------------functions added by shunchi---------------------------

    /**
     * get the length of a map, call this method if you did not call setHdr ()
     * before
     * 
     * @return length of this map in bytes
     */
    public int getLength() {
        return map_length;
    }

    /**
     * Makes a copy of the fldOffset array
     *
     * @return a copy of the fldOffset arrray
     *
     */

    public short[] copyFldOffset() {
        short[] newFldOffset = new short[fldCnt + 1];
        for (int i = 0; i <= fldCnt; i++) {
            newFldOffset[i] = fldOffset[i];
        }
        return newFldOffset;
    }
}
