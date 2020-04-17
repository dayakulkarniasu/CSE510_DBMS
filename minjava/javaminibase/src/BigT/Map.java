package BigT;

import java.io.*;
import global.*;
import heap.*;

/**
 * You will need to create a class BigT.Map, similar to heap.Tuple but having a
 * ﬁxed structure (and thus a ﬁxed header) as described above. Thus, the
 * constructor and get/set methods associated with the BigT.Map should be
 * adapted as appropriate
 */
public class Map implements GlobalConst {
    /**
     * Maximum size of any Map
     */
    public static final int max_size = MINIBASE_PAGESIZE;

    /**
     * a byte array to hold data
     */
    private byte[] data;

    /**
     * start position of this Map in data[]
     */
    private int map_offset;

    /**
     * length of this Map
     */
    private int map_length;

    /**
     * private field Number of fields in this Map
     */
    private short fldCnt = 4;

    /**
     * private field Array of offsets of the fields
     */

    private short[] fldOffset;

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
     *
     * Construct a map from a byte array.
     *
     * @param amap   a byte array which contains the map
     * @param offset the offset of the map in the byte array
     * @param length
     */
    public Map(byte[] amap, int offset, int length) { // length needs to be declared
        data = amap;
        map_offset = offset;
        map_length = length;
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
        fldCnt = 4;
        fldOffset = fromMap.copyFldOffset();
    }

    /**
     * Class constructor Creat a new map with length = size, map offset = 0.
     */

    public Map(int size) {
        // Creat a new map
        data = new byte[size];
        map_offset = 0;
        map_length = size;
    }

    /**
     * Returns the row label.
     *
     * @return
     * @throws FieldNumberOutOfBoundException
     */
  public String getRowLabel() throws IOException {
        String tmp = null;
        short val1, val2 ;
        try {
          val1 = Convert.getShortValue(map_offset + 2,   data);
        //   System.out.println("data_length: " + data.length);
          val2 = Convert.getShortValue(map_offset + 4, data);
          tmp = Convert.getStrValue(val1, data, val2-val1);
        }
         catch (IOException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    /**
     * Returns the column label.
     *
     * @return
     * @throws FieldNumberOutOfBoundException
     */
      public String getColumnLabel() throws IOException {
        String tmp = null;
        short val1, val2 ;
        try {
          val1 = Convert.getShortValue(map_offset + 4, data);
          val2 = Convert.getShortValue(map_offset + 6, data);
          tmp = Convert.getStrValue(val1, data, val2-val1);
        }
         catch (IOException e) {
            e.printStackTrace();
        }
        return tmp;
    }

    /**
     * Returns the timestamp.
     *
     * @return
     * @throws FieldNumberOutOfBoundException
     */
      public int getTimeStamp() throws IOException {
        int tmp = -1;
      short val1, val2 ;
      try {
        val1 = Convert.getShortValue(map_offset + 8,   data);
        // val2 = Convert.getShortValue(10, data);
        tmp = Convert.getIntValue(val1, data);
      }
       catch (IOException e) {
          e.printStackTrace();
      }
        return tmp;
    }

    /**
     * Returns the value.
     *
     * @return
     * @throws FieldNumberOutOfBoundException
     */
      public String getValue() throws IOException {
        String tmp = null;
      short val1, val2 ;
      try {
        val1 = Convert.getShortValue(map_offset + 6,   data);
        val2 = Convert.getShortValue(map_offset + 8, data);
        tmp = Convert.getStrValue(val1, data, val2-val1);
      }
       catch (IOException e) {
          e.printStackTrace();
      }
        return tmp;
    }

    /**
     * Set the row label.
     *
     * @param val
     * @return
     */
    public Map setRowLabel(String val) throws IOException, FieldNumberOutOfBoundException {
        try {
            this.setStrFld(1, val);
              System.out.println (" In setRowLabel, val = " + val + " \n");
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Set the column label.
     *
     * @param val
     * @return
     */
    public Map setColumnLabel(String val) throws IOException, FieldNumberOutOfBoundException {
        try {
            this.setStrFld(2, val);
            System.out.println (" In setColumnLabel, val = " + val + " \n");
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Set the timestamp.
     *
     * @param val
     * @return
     */
    public Map setTimeStamp(int val) throws IOException, FieldNumberOutOfBoundException {
        try {
            this.setIntFld(4, val);
            System.out.println (" In setTimeStamp, val = " + val + " \n");
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return this;
    }

    /**
     * Set the value.
     *
     * @param val
     * @return
     */
    public Map setValue(String val) throws IOException, FieldNumberOutOfBoundException {
        try {
            this.setStrFld(3, val);
            System.out.println (" In setValue, val = " + val + " \n");
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
     * Print out the map
     *
     * @param type the types in the map
     * @Exception IOException I/O exception
     */
    public void print(AttrType type[]) throws IOException {
        int i, val;
        float fval;
        String sval;

        System.out.print("[");
        for (i = 0; i < fldCnt - 1; i++) {
            switch (type[i].attrType) {

            case AttrType.attrInteger:
                val = Convert.getIntValue(fldOffset[i], data);
                System.out.print(val);
                break;

            case AttrType.attrReal:
                fval = Convert.getFloValue(fldOffset[i], data);
                System.out.print(fval);
                break;

            case AttrType.attrString:
                sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
                System.out.print(sval);
                break;

            case AttrType.attrNull:
            case AttrType.attrSymbol:
                break;
            }
            System.out.print(", ");
        }

        switch (type[fldCnt - 1].attrType) {

        case AttrType.attrInteger:
            val = Convert.getIntValue(fldOffset[i], data);
            System.out.print(val);
            break;

        case AttrType.attrReal:
            fval = Convert.getFloValue(fldOffset[i], data);
            System.out.print(fval);
            break;

        case AttrType.attrString:
            sval = Convert.getStrValue(fldOffset[i], data, fldOffset[i + 1] - fldOffset[i]);
            System.out.print(sval);
            break;

        case AttrType.attrNull:
        case AttrType.attrSymbol:
            break;
        }
        System.out.println("]");

    }

    /**
     * get the length of a map, call this method if you did call setHdr () before
     *
     * @return size of this map in bytes
     */
    public short size() {
        return ((short) (fldOffset[fldCnt] - map_offset));
    }

    /**
     * Copy the given map
     *
     * @param fromMap the map being copied
     */
    public void mapCopy(Map fromMap) {
        byte[] temparray = fromMap.getMapByteArray();
        //System.out.println("Length of map length" +map_length);
        //System.out.println("Length of fromMap.map_length " + fromMap.map_length);
        //System.out.println("Length of fromMap.map_length " + fromMap.getLength());
        System.arraycopy(temparray, 0, data, map_offset, fromMap.map_length);
    }

    /**
     * This is used when you don’t want to use the constructor
     *
     * @param amap
     * @param offset
     */
    public void mapInit(byte[] amap, int offset) {
        data = amap;
        map_offset = offset;
        map_length = amap.length;
    }

    /**
     * Set a map with the given byte array and offset.
     *
     * @param frommap
     * @param offset
     */
    public void mapSet(byte[] frommap, int offset, int length) {
        System.arraycopy(frommap, offset, data, 0, length);
        map_offset = 0;
        map_length = length;

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

    /**
     * get the offset of a map
     *
     * @return offset of the map in byte array
     */
    public int getOffset() {
        return map_offset;
    }

    /**
     * return the data byte array
     *
     * @return data byte array
     */
    public byte[] returnMapByteArray() {
        return data;
    }

    /**
     * Convert this field into integer
     *
     * @param fldNo the field number
     * @return the converted integer if success
     *
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */
    public int getIntFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
        int val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getIntValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field in to float
     *
     * @param fldNo the field number
     * @return the converted float number if success
     *
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */
    public float getFloFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
        float val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getFloValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Convert this field into String
     *
     * @param fldNo the field number
     * @return the converted string if success
     *
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */
    public String getStrFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
        String val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getStrValue(fldOffset[fldNo - 1], data, fldOffset[fldNo] - fldOffset[fldNo - 1]); // strlen+2
            return val;
        }
        else {
          throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");
        }
    }

    /**
     * Convert this field into a character
     *
     * @param fldNo the field number
     * @return the character if success
     *
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */
    public char getCharFld(int fldNo) throws IOException, FieldNumberOutOfBoundException {
        char val;
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            val = Convert.getCharValue(fldOffset[fldNo - 1], data);
            return val;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to integer value
     *
     * @param fldNo the field number
     * @param val   the integer value
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setIntFld(int fldNo, int val) throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setIntValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");
    }

    /**
     * Set this field to float value
     *
     * @param fldNo the field number
     * @param val   the float value
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setFloFld(int fldNo, float val) throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setFloValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");

    }

    /**
     * Set this field to String value
     *
     * @param fldNo the field number
     * @param val   the string value
     * @exception IOException                    I/O errors
     * @exception FieldNumberOutOfBoundException Map field number out of bound
     */

    public Map setStrFld(int fldNo, String val) throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            Convert.setStrValue(val, fldOffset[fldNo - 1], data);
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");
    }
    public Map setFieldOffset(short fldNo, short val) throws IOException, FieldNumberOutOfBoundException {
        if ((fldNo > 0) && (fldNo <= fldCnt)) {
            fldOffset[fldNo - 1] = val;
            return this;
        } else
            throw new FieldNumberOutOfBoundException(null, "MAP:TUPLE_FLDNO_OUT_OF_BOUND");
    }
    /**
     * setHdr will set the header of this map.
     *
     * @param numFlds    number of fields
     * @param types[]    contains the types that will be in this map
     * @param strSizes[] contains the sizes of the string
     *
     * @exception IOException               I/O errors
     * @exception InvalidTypeException      Invalid tupe type
     * @exception InvalidTupleSizeException map size too big
     *
     */

    public void setHdr(short numFlds, AttrType types[], short strSizes[])
            throws IOException, InvalidTypeException, InvalidTupleSizeException {
        if ((numFlds + 2) * 2 > max_size)
            throw new InvalidTupleSizeException(null, "MAP: TUPLE_TOOBIG_ERROR");
        // numFlds = 4;
        fldCnt = numFlds;
        Convert.setShortValue(numFlds, map_offset, data);
        fldOffset = new short[numFlds + 1];
        int pos = map_offset + 2; // start position for fldOffset[]

        fldOffset[0] = (short) (MAPHEADER_LEN + map_offset);
        Convert.setShortValue(fldOffset[0], pos, data);
        pos += 2;
        short strCount = 0;
        short incr;
        int i;

        for (i = 1; i < numFlds; i++) {
            switch (types[i - 1].attrType) {

            case AttrType.attrInteger:
                incr = 4;
                break;

            case AttrType.attrReal:
                incr = 4;
                break;

            case AttrType.attrString:
                incr = (short) (strSizes[strCount] + 2); // strlen in bytes = strlen +2
                strCount++;
                break;

            default:
                throw new InvalidTypeException(null, "MAP: TUPLE_TYPE_ERROR");
            }
            fldOffset[i] = (short) (fldOffset[i - 1] + incr);
            Convert.setShortValue(fldOffset[i], pos, data);
            pos += 2;

        }
        switch (types[numFlds - 1].attrType) {

        case AttrType.attrInteger:
            incr = 4;
            break;

        case AttrType.attrReal:
            incr = 4;
            break;

        case AttrType.attrString:
            incr = (short) (strSizes[strCount] + 2); // strlen in bytes = strlen +2
            break;

        default:
            throw new InvalidTypeException(null, "MAP: TUPLE_TYPE_ERROR");
        }

        fldOffset[numFlds] = (short) (fldOffset[i - 1] + incr);
        Convert.setShortValue(fldOffset[numFlds], pos, data);

        if (map_length > max_size)
            throw new InvalidTupleSizeException(null, "MAP: TUPLE_TOOBIG_ERROR");
    }
}
