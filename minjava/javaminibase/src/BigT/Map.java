/* File Map.java */

package BigT;

import java.io.*;
import java.lang.*;
import global.*;
import heap.FieldNumberOutOfBoundException;
import heap.InvalidTypeException;


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
  private short fldCnt = 4;

  /**
   * private field
   * Array of offsets of the fields
   */

  private short [] fldOffset;

/** Define the Map structure
*
**/
 private String RowLabel;
 private String ColumnLabel ;
 private int TimeStamp ;
 private String Value;

   /**
    * Class constructor
    * Creat a new Map with length = max_size,Map offset = 0.
    */

  public  Map()
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
       fldCnt = 4 ; //fromMap.noOfFlds();
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
  public int getMapLength()
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

   /**
    * Convert this field into integer
    *
    * @param	fldNo	the field number
    * @return		the converted integer if success
    *
    * @exception   IOException I/O errors
    * @exception FieldNumberOutOfBoundException Map field number out of bound
    *
    * For BigDB, the Map has only 4 fields and their locations are also fixed
    * Fld_no : 1 --> RowLabel
    * Fld_no : 2 --> ColumnLabel
    * Fld_no : 3 --> TimeStamp
    * Fld_no : 4 --> Value
    *
    */



   /**
    * Convert this field in to float
    *
    * @param    fldNo   the field number
    * @return           the converted float number  if success
    *
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Map field number out of bound
    */

/*    public float getFloFld(int fldNo)
    	throws IOException, FieldNumberOutOfBoundException
     {
	float val;
      if ( (fldNo > 0) && (fldNo <= fldCnt))
       {
        val = Convert.getFloValue(fldOffset[fldNo -1], data);
        return val;
       }
      else
       throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
     }

*/
   /**
    * Convert this field into String
    *
    * @param    fldNo   the field number
    * @return           the converted string if success
    *
    * @exception   IOException I/O errors
    * @exception   FieldNumberOutOfBoundException Map field number out of bound
    */

/* java.lang.String getRowLabel(): Returns the row label.
*/
   public String getRowLabel()
   	throws IOException, FieldNumberOutOfBoundException
   {
         String val;
    if ( 4 <= fldCnt)
     {
        val = Convert.getStrValue(fldOffset[0], data, fldOffset[1] - fldOffset[0]); //strlen+2
        return val;
     }
    else
     throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
  }

  /* java.lang.String getColumnLabel(): Returns the ColumnLabel label.
  */
     public String getColumnLabel()
     	throws IOException, FieldNumberOutOfBoundException
     {
           String val;
      if ( 4 <= fldCnt)
       {
          val = Convert.getStrValue(fldOffset[1], data, fldOffset[2] - fldOffset[1]); //strlen+2
          return val;
       }
      else
       throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    /* int getTimeStamp(): Returns the timestamp.
    */
  public int getTimeStamp()
    throws IOException, FieldNumberOutOfBoundException
  {
    int val;
    if ( 4 <= fldCnt)
     {
      val = Convert.getIntValue(fldOffset[2], data);
      return val;
     }
    else
     throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
  }

    /* java.lang.String getValue(): Returns the value.
    */
       public String getValue()
       	throws IOException, FieldNumberOutOfBoundException
       {
             String val;
        if ( 4 <= fldCnt)
         {
            val = Convert.getStrValue(fldOffset[3], data, fldOffset[4] - fldOffset[3]); //strlen+2
            return val;
         }
        else
         throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
      }

  /**
   * Set this field to integer value
   *
   * @param	fldNo	the field number
   * @param	val	the integer value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Map field number out of bound
   */

  public Map setIntFld(int fldNo, int val)
  	throws IOException, FieldNumberOutOfBoundException
  {
    if ( (fldNo > 0) && (fldNo <= fldCnt))
     {
	Convert.setIntValue (val, fldOffset[fldNo -1], data);
	return this;
     }
    else
     throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
  }

  /**
   * Set this field to float value
   *
   * @param     fldNo   the field number
   * @param     val     the float value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Map field number out of bound
   */

/*  public Map setFloFld(int fldNo, float val)
  	throws IOException, FieldNumberOutOfBoundException
  {
   if ( (fldNo > 0) && (fldNo <= fldCnt))
    {
     Convert.setFloValue (val, fldOffset[fldNo -1], data);
     return this;
    }
    else
     throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");

  }
  */

  /**
   * Set this field to String value
   *
   * @param     fldNo   the field number
   * @param     val     the string value
   * @exception   IOException I/O errors
   * @exception   FieldNumberOutOfBoundException Map field number out of bound
   */

  /*Map setRowLabel(java.lang.String val): Set the row label.
  */
  public Map setRowLabel(java.lang.String val)

		throws IOException, FieldNumberOutOfBoundException
   {
     if ( 4 <= fldCnt)
      {
         Convert.setStrValue (val, fldOffset[0], data);
         return this;
      }
     else
       throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
    }

    /*Map setColumnLabel(java.lang.String val): Set the column label.
    */
    public setColumnLabel(java.lang.String val)

  		throws IOException, FieldNumberOutOfBoundException
     {
       if ( 4 <= fldCnt)
        {
           Convert.setStrValue (val, fldOffset[1], data);
           return this;
        }
       else
         throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
      }

  //    Map setTimeStamp(int val): Set the timestamp

       public Map setTimeStamp(int val)
       	throws IOException, FieldNumberOutOfBoundException
       {
         if ( 4 <= fldCnt)
          {
             	Convert.setIntValue (val, fldOffset[2], data);
             	return this;
          }
         else
          throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
       }

       /*Map setValue(java.lang.String val): Set the value.
       */
       public setValue(java.lang.String val)

     		throws IOException, FieldNumberOutOfBoundException
        {
          if ( 4 <= fldCnt)
           {
              Convert.setStrValue (val, fldOffset[3], data);
              return this;
           }
          else
            throw new FieldNumberOutOfBoundException (null, "Map:Map_FLDNO_OUT_OF_BOUND");
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

public void setHdr (short numFlds,  AttrType types[], short strSizes[])
 throws IOException, InvalidTypeException, InvalidMapSizeException
{
  if((numFlds +2)*2 > max_size)
    throw new InvalidMapSizeException (null, "Map: Map_TOOBIG_ERROR");
  numFlds = 4;
  fldCnt = numFlds;
  Convert.setShortValue(numFlds, Map_offset, data);
  fldOffset = new short[numFlds+1];
  int pos = Map_offset+2;  // start position for fldOffset[]

  //sizeof short =2  +2: array siaze = numFlds +1 (0 - numFilds) and
  //another 1 for fldCnt

  /* Rowlabel position FldOffset is fldOffset[0]
  */
  fldOffset[0] = (short) ((numFlds +2) * 2 + Map_offset);
  Convert.setShortValue(fldOffset[0], pos, data);
  pos +=2;
  short strCount =0;
  short incr;
  int i;

  for (i=1; i<numFlds; i++)
  {
    switch(types[i-1].attrType) {

   case AttrType.attrInteger:
     incr = 4;
     break;

   case AttrType.attrReal:
     incr =4;
     break;

   case AttrType.attrString:
     incr = (short) (strSizes[strCount] +2);  //strlen in bytes = strlen +2
     strCount++;
     break;

   default:
    throw new InvalidTypeException (null, "Map: Map_TYPE_ERROR");
   }
  fldOffset[i]  = (short) (fldOffset[i-1] + incr);
  Convert.setShortValue(fldOffset[i], pos, data);
  pos +=2;

}
 switch(types[numFlds -1].attrType) {

   case AttrType.attrInteger:
     incr = 4;
     break;

   case AttrType.attrReal:
     incr =4;
     break;

   case AttrType.attrString:
     incr =(short) ( strSizes[strCount] +2);  //strlen in bytes = strlen +2
     break;

   default:
    throw new InvalidTypeException (null, "Map: Map_TYPE_ERROR");
   }

  fldOffset[numFlds] = (short) (fldOffset[i-1] + incr);
  Convert.setShortValue(fldOffset[numFlds], pos, data);

  Map_length = fldOffset[numFlds] - Map_offset;

  if(Map_length > max_size)
   throw new InvalidMapSizeException (null, "Map: Map_TOOBIG_ERROR");
}


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
