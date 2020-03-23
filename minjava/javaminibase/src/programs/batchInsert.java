package programs;

import btree.*;
import diskmgr.PCounter;
import global.*;
import BigT.*;
import heap.*;

import java.io.*;
//import global.*;

public class batchInsert implements GlobalConst{

  public static void insertTable(String datafilename, String tablename) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {

    InsertBTmap(datafilename, tablename);
    System.out.println("Diskpage read " + PCounter.rcounter + " Disk page written " + PCounter.wcounter);

  }// end of main

  public static boolean InsertBTmap(String datafileN, String tablename)
      throws HFException, HFBufMgrException, HFDiskMgrException, IOException {

    bigt big = null;
    if(SystemDefs.JavabaseDB.table == null)
    {
      big = new bigt(tablename, SystemDefs.JavabaseDB.dbType);
      System.out.println("JavabaseDB name: " + SystemDefs.JavabaseDB.table.name);
      System.out.println("datafileN: " + datafileN);
    }
    else if(SystemDefs.JavabaseDB.table.name.equals(tablename))
    {
      big = SystemDefs.JavabaseDB.table;
      System.out.println("Table exist.");
    }
    else
    {
      System.out.println("defname: " + SystemDefs.JavabaseDB.table.name);
      System.out.println("datafileN: " + datafileN);
      System.err.println("Table name not match.." );
    }

    // bigt big = new bigt(datafileN, SystemDefs.JavabaseDB.dbType);
    
    BufferedReader br = null;
    int linecount = 0;
    String line = "";
    String csvSplitBy = ",";
    int recleng2 = MAP_LEN;

    System.out.println("\n  Test 1: Insert and scan fixed-size records\n");
    boolean status = true;
    MID rid = new MID();
    Heapfile f = null;

    System.out.println("  - Create a heap file\n");
    try {
      f = new Heapfile("file_1");
    } catch (Exception e) {
      status = false;
      System.err.println("*** Could not create heap file\n");
      e.printStackTrace();
    }

    if (status == true) {
      System.out.println("  - Add " + linecount + " records to the file\n");

      // for (int i =0; (i < choice) && (status == true); i++) {
      try {
        br = new BufferedReader(new FileReader(datafileN));
        while ((line = br.readLine()) != null) {
          String[] arryfields = line.split(csvSplitBy);
          String rowLabel = arryfields[0];
          String columnLabel = arryfields[1];
          String timeStamp = arryfields[3];
          String value = arryfields[2];

          // fixed length record
          DummyRecord rec = new DummyRecord(recleng2);
          rec.rowlabname = rowLabel;
          rec.collabname = columnLabel;
          rec.timestampname = Integer.parseInt(timeStamp);
          rec.valuename = value;

          try {
            // Map recMap = new Map(rec.toByteArray(),0, rec.getRecLength()) ;
            Map recMap = new Map(rec.toByteArray(), 0, rec.getRecLength());
            // System.out.println(" RecMap created successfully ");
            AttrType[] types = new AttrType[4];
            types[0] = new AttrType(0);
            types[1] = new AttrType(0);
            types[2] = new AttrType(1);
            types[3] = new AttrType(0);
            short[] strSizes = new short[4];
            strSizes[0] = (short) (rowLabel.length());
            strSizes[1] = (short) (columnLabel.length());
            strSizes[2] = (short) (4);
            strSizes[3] = (short) (value.length());

            recMap.setHdr((short) 4, types, strSizes);

            rid = big.insertMap(recMap.getMapByteArray());
          } catch (Exception e) {
            status = false;
            System.err.println("*** Error inserting record " + linecount + "\n");
            e.printStackTrace();
          }

          linecount++;
        } // end of while loop
        System.out.println("After reading the file, LineCoiunt = " + linecount);

      } // end of try
      catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } // if status okay

    // In general, a sequential scan won't be in the same order as the
    // insertions. However, we're inserting fixed-length records here, and
    // in this case the scan must return the insertion order.

    Scan scan = null;

    if (status == true) {
      System.out.println("In batchInsert - Scan the records just inserted\n");

      try {
        scan = big.hf.openScan();
        // System.out.println (" In batchInsert - done with f.openScan() \n") ;
      } catch (Exception e) {
        status = false;
        System.err.println("*** Error opening scan\n");
        e.printStackTrace();
      }
    }

    if (status == true) {
      int len, i = 0;
      DummyRecord rec = null;
      Map aMap = new Map();

      boolean done = false;
      while (!done) {
        try {

          aMap = scan.getNext(rid);
          if (aMap == null) {
            done = true;
            break;
          }
        } catch (Exception e) {
          status = false;
          e.printStackTrace();
        }

        if (status == true && !done) {
          try {
            rec = new DummyRecord(aMap);
          } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
          }

          len = aMap.getLength();
          if (len != recleng2) {
            System.err.println("*** Record " + i + " had unexpected length " + len + "\n");
            status = false;
            break;
          } else if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()) {
            System.err.println("On record " + i + ":\n");
            System.err.println("*** The heap-file scan has not left its " + "page pinned\n");
            status = false;
            break;
          }
          System.out.println("record: " + i);
          System.out.println("rec.row " + i + " : " + rec.rowlabname);
          System.out.println("rec.col " + i + " : " + rec.collabname);
          System.out.println("rec.timestamp " + i + " : " + rec.timestampname);
          System.out.println("rec.value " + i + " : " + rec.valuename);
          System.out.println();
        }
        ++i;
      } // end of while not done

    } // end of bigger status ok

    if (status == true)
      System.out.println("Test 1 completed successfully.\n");

    return status;
  }

}

class DummyRecord implements GlobalConst {

  // content of the record
  public String rowlabname;
  public String collabname;
  public int timestampname;
  public String valuename;

  // length under control
  private int reclen1;

  private byte[] data;

  /**
   * Default constructor
   */
  public DummyRecord() {
  }

  /**
   * another constructor
   */
  public DummyRecord(int _reclen) {
    setRecLen(_reclen);
    data = new byte[_reclen];
  }

  /**
   * constructor: convert a byte array to DummyRecord object.
   * 
   * @param arecord a byte array which represents the DummyRecord object
   */
  public DummyRecord(byte[] arecord) throws java.io.IOException {
    // TODO fix the functions and create one mor function
    /*
     * setIntRec (arecord); setFloRec (arecord); setStrRec (arecord);
     */
    // Setting the 4 fields in the data object
    setRowLabelRec(arecord);
    setColumnLabelRec(arecord);
    setTimeStampRec(arecord);
    setValueRec(arecord);
    data = arecord;
    // setRecLen(name.length());s
    // Setting the record length = arecord getLength
    int RecordLength = rowlabname.length() + collabname.length() + valuename.length() + valuename.length();
    // setRecLen(data.length());
    setRecLen(RecordLength);
  }

  /**
   * constructor: translate a tuple to a DummyRecord object it will make a copy of
   * the data in the tuple
   * 
   * @param atuple: the input tuple
   */
  public DummyRecord(Map _atuple) throws java.io.IOException {
    // System.out.println (" the length of the map in dummy reccord is: " +
    // _atuple.getLength());
    data = new byte[_atuple.getLength()];
    data = _atuple.getMapByteArray();
    setRecLen(_atuple.getLength());
    rowlabname = _atuple.getRowLabel();

    collabname = _atuple.getColumnLabel();

    timestampname = _atuple.getTimeStamp();

    valuename = _atuple.getValue();

  }

  /**
   * convert this class objcet to a byte array this is used when you want to write
   * this object to a byte array
   */
  public byte[] toByteArray() throws java.io.IOException {
    // data = new byte[reclen];
    /*
     * Convert.setIntValue (ival, 0, data); Convert.setFloValue (fval, 4, data);
     * Convert.setStrValue (name, 8, data);
     */
    int RL_Length = rowlabname.length();
    int CL_Length = collabname.length();
    int TS_Length = 4;
    int V_Length = valuename.length();
    // System.out.println("In toByte Array : TotalLength : " + (16 + RL_Length + 2 +
    // CL_Length + 2 + TS_Length +4 + V_Length + 2));
    setRecLen(MAP_LEN);
    // setRecLen (16 + RL_Length + 2 + CL_Length + 2 + TS_Length +4 + V_Length + 2);
    Convert.setStrValue(rowlabname, MAPHEADER_LEN, data);
    Convert.setStrValue(collabname, MAPHEADER_LEN + RL_Length + 2, data);
    Convert.setIntValue(timestampname, MAPHEADER_LEN + RL_Length + CL_Length + 4, data);
    Convert.setStrValue(valuename, MAPHEADER_LEN + RL_Length + CL_Length + TS_Length + 4, data);
    // recln1 = RL_Length + CL_Length + TS_Length ;
    // System.out.println("In toByte Array : Data = " + data + " Record length : " +
    // reclen1);

    return data;
  }

  /**
   * get the integer value out of the byte array and set it to the int value of
   * the DummyRecord object
   */
  /*
   * public void setIntRec (byte[] _data) throws java.io.IOException { ival =
   * Convert.getIntValue (0, _data); }
   * 
   * /** get the float value out of the byte array and set it to the float value
   * of the DummyRecord object
   */
  /*
   * public void setFloRec (byte[] _data) throws java.io.IOException { fval =
   * Convert.getFloValue (4, _data); }
   */
  /**
   * get the String value out of the byte array and set it to the float value of
   * the HTDummyRecorHT object
   */
  /*
   * public void setStrRec (byte[] _data) throws java.io.IOException { //
   * System.out.println("reclne= "+reclen); //
   * System.out.println("data size "+_data.size()); name = Convert.getStrValue (8,
   * _data, reclen1-8); }
   */
  public void setRowLabelRec(byte[] _data) throws java.io.IOException {
    // System.out.println("reclne= "+reclen);
    // System.out.println("data size "+_data.size());
    rowlabname = Convert.getStrValue(MAPHEADER_LEN, _data, STR_LEN);
  }

  public void setColumnLabelRec(byte[] _data) throws java.io.IOException {
    // System.out.println("reclne= "+reclen);
    // System.out.println("data size "+_data.size());
    collabname = Convert.getStrValue(MAPHEADER_LEN + STR_LEN, _data, STR_LEN);
  }

  public void setTimeStampRec(byte[] _data) throws java.io.IOException {
    timestampname = Convert.getIntValue(MAPHEADER_LEN + STR_LEN * 2, _data);
  }

  public void setValueRec(byte[] _data) throws java.io.IOException {
    // System.out.println("reclne= "+reclen);
    // System.out.println("data size "+_data.size());
    valuename = Convert.getStrValue(MAPHEADER_LEN + STR_LEN * 2 + 4, _data, STR_LEN);
  }

  // Other access methods to the size of the String field and
  // the size of the record
  public void setRecLen(int size) {
    reclen1 = size;
  }

  public int getRecLength() {
    return reclen1;
  }
}
