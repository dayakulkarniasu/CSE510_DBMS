/* File Map.java */

package BigT;

import java.io.*;
import java.lang.*;
import global.*;


public class Map implements GlobalConst{


 /**
  * Maximum size of any Map
  */
  public static final int max_size = MINIBASE_PAGESIZE;

 /**
   * a byte array to hold data
   */
  private byte [] data;

  /**
   * start position of this Map in data[]
   */
  private int Map_offset;

  /**
   * length of this Map
   */
  private int Map_length;

  /**
   * private field
   * Number of fields in this Map
   */
  private final short fldCnt = 4;

  /**
   * private field
   * Array of offsets of the fields
   */
  private short [] fldOffset;

  /**
   * private field
   * Row label of the Map
   */
  // private String Rowlabel;

  /**
   * private field
   * Column label of the Map
   */
  // private String Columnlabel;

  /**
   * private field
   * Timestamp of the Map
   */
  // private int Timestamp;

   /**
    * Class constructor
    * Creat a new Map with length = max_size,Map offset = 0.
    */

  public Map()
  {
    // Creat a new Map
    data = new byte[max_size];
    Map_offset = 0;
    Map_length = max_size;
  }

  /** Constructor
  * @param aMap a byte array which contains the Map
  * @param offset the offset of the Map in the byte array
  * @param length the length of the Map
  */

  public Map(byte [] aMap, int offset, int length)
  {
    data = aMap;
    Map_offset = offset;
    Map_length = length;
  //  fldCnt = getShortValue(offset, data);
  }

  /** Constructor(used as Map copy)
  * @param fromMap   a byte array which contains the Map
  *
  */
  public Map(Map fromMap)
  {
    data = fromMap.getMapByteArray();
    Map_length = fromMap.getLength();
    Map_offset = 0;
    fldOffset = fromMap.copyFldOffset();
  }

  /**
  * Class constructor
  * Creat a new Map with length = size,Map offset = 0.
  */

  public  Map(int size)
  {
    // Creat a new Map
    data = new byte[size];
    Map_offset = 0;
    Map_length = size;
  }

   /** Copy a Map to the current Map position
    *  you must make sure the Map lengths must be equal
    * @param fromMap the Map being copied
    */
  public void MapCopy(Map fromMap)
  {
      byte [] temparray = fromMap.getMapByteArray();
      System.arraycopy(temparray, 0, data, Map_offset, Map_length);
//       fldCnt = fromMap.noOfFlds();
//       fldOffset = fromMap.copyFldOffset();
  }

  /** This is used when you don't want to use the constructor
  * @param aMap  a byte array which contains the Map
  * @param offset the offset of the Map in the byte array
  * @param length the length of the Map
  */

  public void MapInit(byte [] aMap, int offset, int length)
  {
    data = aMap;
    Map_offset = offset;
    Map_length = length;
  }

 /**
  * Set a Map with the given Map length and offset
  * @param	record	a byte array contains the Map
  * @param	offset  the offset of the Map ( =0 by default)
  * @param	length	the length of the Map
  */
 public void MapSet(byte [] record, int offset, int length)
  {
    System.arraycopy(record, offset, data, 0, length);
    Map_offset = 0;
    Map_length = length;
  }

 /** get the length of a Map, call this method if you did not
  *  call setHdr () before
  * @return 	length of this Map in bytes
  */
  public int getLength()
   {
      return Map_length;
   }

  /** get the length of a Map, call this method if you did
    *  call setHdr () before
    * @return     size of this Map in bytes
    */
  public short size()
  {
    return ((short) (fldOffset[fldCnt] - Map_offset));
  }

  /** get the offset of a Map
  *  @return offset of the Map in byte array
  */
  public int getOffset()
  {
    return Map_offset;
  }

  /** Copy the Map byte array out
  *  @return  byte[], a byte array contains the Map
  *		the length of byte[] = length of the Map
  */

  public byte [] getMapByteArray()
  {
    byte [] Mapcopy = new byte [Map_length];
    System.arraycopy(data, Map_offset, Mapcopy, 0, Map_length);
    return Mapcopy;
  }

  /** return the data byte array
  *  @return  data byte array
  */

  public byte [] returnMapByteArray()
  {
      return data;
  }

  /* String getRowLabel(): Returns the row label.
  */
  public String getRowLabel() throws IOException
  {
    String val;
    val = Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]);
    return val;
  }

  /* java.lang.String getColumnLabel(): Returns the ColumnLabel label.
  */
  public String getColumnLabel() throws IOException
  {
    String val;
    val = Convert.getStrValue(fldOffset[1], data, fldOffset[2] - fldOffset[1]);
    return val;
  }

  /* int getTimeStamp(): Returns the timestamp.
  */
  public int getTimeStamp() throws IOException
  {
    int val;
      val = Convert.getIntValue(fldOffset[2], data);
      return val;
  }

    /* java.lang.String getValue(): Returns the value.
    */
  public String getValue() throws IOException
  {
    String val;
    val = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]);
    return val;
  }

  /*Map setRowLabel(java.lang.String val): Set the row label.
  */
  public Map setRowLabel(String val) throws IOException
  {
    // int length = val.length();
    // need to modify offset[1] if varied length
    Convert.setStrValue (val, fldOffset[0], data);
    return this;
  }

  /*Map setColumnLabel(java.lang.String val): Set the column label.
  */
  public Map setColumnLabel(String val) throws IOException
  {
    // int length = val.length();
    // need to modify offset[2] if varied length
    Convert.setStrValue (val, fldOffset[1], data);
    return this;
  }

  // Map setTimeStamp(int val): Set the timestamp
  public Map setTimeStamp(int val)
  throws IOException
  {
    Convert.setIntValue (val, fldOffset[2], data);
    return this;
  }

  /*Map setValue(java.lang.String val): Set the value.
  */
  public Map setValue(String val) throws IOException
  {
    Convert.setStrValue (val, fldOffset[3], data);
    return this;
  }

   /**
    * setHdr will set the header of this Map.
    *
    * @param	numFlds	  number of fields
    * @param	types[]	  contains the types that will be in this Map
    * @param	strSizes[]      contains the sizes of the string
    *
    * @exception IOException I/O errors
    * @exception InvalidTypeException Invalid tupe type
    * @exception InvalidMapSizeException Map size too big
    *
    */

  // TODO: setHdr
  // public void setHdr (short numFlds,  AttrType types[], short strSizes[])
  // throws IOException, InvalidTypeException, InvalidMapSizeException
  // {
  //   if(numFlds * 2 > max_size)
  //     throw new InvalidMapSizeException (null, "Map: Map_TOOBIG_ERROR");

  //   // Convert.setShortValue(fldCnt, Map_offset, data);
  //   fldOffset = new short[fldCnt]; // 4
  //   int pos = Map_offset;  // start position for fldOffset[]

  //   /* Rowlabel position FldOffset is fldOffset[0]
  //   */
  //   fldOffset[0] = (short) (numFlds * 2 + Map_offset);
  //   Convert.setShortValue(fldOffset[0], pos, data);
  //   short strCount =0;
  //   short incr;
  //   int i;

  //   for (i=1; i<numFlds; i++)
  //   {
  //     switch(types[i-1].attrType) {

  //     case AttrType.attrInteger:
  //       incr = 4;
  //       break;

  //     case AttrType.attrReal:
  //       incr =4;
  //       break;

  //     case AttrType.attrString:
  //       incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
  //       strCount++;
  //       break;

  //     default:
  //       throw new InvalidTypeException (null, "Map: Map_TYPE_ERROR");
  //     }
  //     fldOffset[i]  = (short) (fldOffset[i-1] + incr);
  //     Convert.setShortValue(fldOffset[i], pos, data);
  //     pos +=2;
  //   }
  //   switch(types[numFlds -1].attrType) {

  //     case AttrType.attrInteger:
  //       incr = 4;
  //       break;

  //     case AttrType.attrReal:
  //       incr =4;
  //       break;

  //     case AttrType.attrString:
  //       incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
  //       break;

  //     default:
  //       throw new InvalidTypeException (null, "Map: Map_TYPE_ERROR");
  //   }

  //   fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
  //   Convert.setShortValue(fldOffset[numFlds], pos, data);

  //   Map_length = fldOffset[numFlds] - Map_offset;

  //   if(Map_length > max_size)
  //   throw new InvalidMapSizeException (null, "Map: Map_TOOBIG_ERROR");
  // }


  /**
   * Returns number of fields in this Map
   *
   * @return the number of fields in this Map
   *
   */

  public short noOfFlds()
  {
    return fldCnt;
  }

  /**
   * Makes a copy of the fldOffset array
   *
   * @return a copy of the fldOffset arrray
   *
   */

  public short[] copyFldOffset()
  {
    short[] newFldOffset = new short[fldCnt + 1];
    for (int i=0; i<=fldCnt; i++) {
      newFldOffset[i] = fldOffset[i];
    }

    return newFldOffset;
  }

  // TODO: print method


 /**
  * Print out the Map
  * @param type  the types in the Map
  * @Exception IOException I/O exception
  */
 public void print(AttrType type[])
    throws IOException
 {
  int i, val;
  float fval;
  String sval;

  System.out.print("[");
  for (i=0; i< fldCnt-1; i++)
   {
    switch(type[i].attrType) {

   case AttrType.attrInteger:
     val = Convert.getIntValue(fldOffset[i], data);
     System.out.print(val);
     break;

   case AttrType.attrReal:
     fval = Convert.getFloValue(fldOffset[i], data);
     System.out.print(fval);
     break;

   case AttrType.attrString:
     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
     System.out.print(sval);
     break;

   case AttrType.attrNull:
   case AttrType.attrSymbol:
     break;
   }
   System.out.print(", ");
 }

 switch(type[fldCnt-1].attrType) {

   case AttrType.attrInteger:
     val = Convert.getIntValue(fldOffset[i], data);
     System.out.print(val);
     break;

   case AttrType.attrReal:
     fval = Convert.getFloValue(fldOffset[i], data);
     System.out.print(fval);
     break;

   case AttrType.attrString:
     sval = Convert.getStrValue(fldOffset[i], data,fldOffset[i+1] - fldOffset[i]);
     System.out.print(sval);
     break;

   case AttrType.attrNull:
   case AttrType.attrSymbol:
     break;
   }
   System.out.println("]");

 }

  /**
   * private method
   * Padding must be used when storing different types.
   *
   * @param	offset
   * @param type   the type of Map
   * @return short typle
   */

  private short pad(short offset, AttrType type)
   {
      return 0;
   }
}
