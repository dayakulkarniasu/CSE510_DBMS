package programs;

import btree.*;
import diskmgr.PCounter;
import global.*;
import BigT.*;
import heap.*;
import iterator.*;
import java.io.*;

public class batchInsert implements GlobalConst {

  public static boolean found_delete(Heapfile fd, String fd_rowLabel, String fd_columnLabel, int fd_timestampname,
      String fd_value) {

    Scan scan = null;
    boolean status = true;
    int len, i = 0;
    DummyRecord rec = null;
    MID rid = new MID();

    int pgid1 = 0;
    int slotno1 = 0;
    int pgid2 = 0;
    int slotno2 = 0;
    int pgid3 = 0;
    int slotno3 = 0;

    MID delete_rid = new MID();

    Map aMapfd = new Map();
    int no_of_maps = 0;
    // int recleng22 = 64;
    boolean done = false;
    Map aMapfd_1 = new Map();
    Map aMapfd_2 = new Map();
    Map aMapfd_3 = new Map();
    int aMapfd_1_timeStamp = 0, aMapfd_2_timeStamp = 0, aMapfd_3_timeStamp = 0;

    try {
      scan = fd.openScan();
      // System.out.println (" In batchInsert - done with fd.openScan() \n") ;
    } catch (Exception e) {
      status = false;
      System.err.println("*** Error opening scan\n");
      e.printStackTrace();
    }

    if (status == true && SystemDefs.JavabaseBM.getNumUnpinnedBuffers() == SystemDefs.JavabaseBM.getNumBuffers()) {
      System.err.println("*** The heap-file scan has not pinned the first page\n");
      status = false;
    }

    if (status == true) {

      while (!done) {
        try {

          aMapfd = scan.getNext(rid);

          if (aMapfd == null) {
            done = true;
            break;
          }
        } catch (Exception e) {
          status = false;
          e.printStackTrace();
        }

        if (status == true && !done) {
          try {
            // System.out.println ("From Scan, getting next Map and converting in to
            // DummyRecord \n");
            rec = new DummyRecord(aMapfd);
            // System.out.println ("From Scan, After converting in to DummyRecord \n");
          } catch (Exception e) {
            System.err.println("" + e);
            e.printStackTrace();
          }

          len = aMapfd.getLength();

          if ((fd_rowLabel.equals(rec.rowlabname)) && (fd_columnLabel.equals(rec.collabname))) {

            System.out.println();
            System.out.println("rec.row " + i + " :" + rec.rowlabname + " rec.col : " + rec.collabname
                + " rec.timestamp : " + rec.timestampname + " rec.value : " + rec.valuename);
            System.out.println();

            no_of_maps++;

            if (no_of_maps == 1) {
              pgid1 = rid.pageNo.pid;
              slotno1 = rid.slotNo;

              aMapfd_1 = aMapfd;
              aMapfd_1_timeStamp = rec.timestampname;

            }
            if (no_of_maps == 2) {
              pgid2 = rid.pageNo.pid;
              slotno2 = rid.slotNo;

              aMapfd_2 = aMapfd;
              aMapfd_2_timeStamp = rec.timestampname;

            }
            if (no_of_maps == 3) {
              // rid3.copyMid(rid);

              pgid3 = rid.pageNo.pid;
              slotno3 = rid.slotNo;

              aMapfd_3 = aMapfd;
              aMapfd_3_timeStamp = rec.timestampname;
              // System.out.println("First Map for delete with timestamp = " +
              // aMapfd_1_timeStamp + " rid1.pageNo.pid = "+ rid1.pageNo.pid + " SlotNo =
              // "+rid1.slotNo);
              // System.out.println("Second Map for delete with timestamp = " +
              // aMapfd_2_timeStamp + " rid2.pageNo.pid = "+ rid2.pageNo.pid + " SlotNo =
              // "+rid2.slotNo);
              // System.out.println("Third Map for delete with timestamp = " +
              // aMapfd_3_timeStamp + " rid3.pageNo.pid = "+ rid3.pageNo.pid + " SlotNo =
              // "+rid3.slotNo);

            }
          } // end of Map field comparision
        } // end of if (status == true && !done)
        ++i;
      } // end of while not done

      // output no of maps after comparison
      System.out.println();
      System.out.println("no_of_maps: " + no_of_maps);
      System.out.println();
      // end output

      System.out.println();
      System.out.println("rid1.pageNo:" + pgid1 + " rid1.slotNo:" + slotno1);
      System.out.println("rid2.pageNo:" + pgid2 + " rid2.slotNo:" + slotno2);
      System.out.println("rid3.pageNo:" + pgid3 + " rid3.slotNo:" + slotno3);
      System.out.println();

      // If the no_of_maps = 3, then, delete the last timeStamp record.
      if (no_of_maps == 3) {
        if (aMapfd_1_timeStamp <= aMapfd_2_timeStamp) {
          if (aMapfd_1_timeStamp <= aMapfd_3_timeStamp) {
            delete_rid = new MID(new PageId(pgid1), slotno1);
          } else {
            delete_rid = new MID(new PageId(pgid3), slotno3);
          }
        } else if (aMapfd_2_timeStamp <= aMapfd_3_timeStamp) {
          delete_rid = new MID(new PageId(pgid2), slotno2);
        } else {
          delete_rid = new MID(new PageId(pgid3), slotno3);
        }
        // identified the map to be deleted.
        // System.out.println("Ready to delete the Map with delete_rid.pageNo.pid = "+
        // delete_rid.pageNo.pid + " SlotNo = "+delete_rid.slotNo);

        try {

          status = fd.deleteMap(delete_rid);
          if (status == true) {
            // System.out.println("Successfully deleted the Map : delete_rid.pageNo.pid = "+
            // delete_rid.pageNo.pid + " SlotNo = "+delete_rid.slotNo);

          }
        } catch (Exception e) {
          status = false;
          System.err.println("*** Error deleting record " + i + "\n");
          e.printStackTrace();
        }
      }

      scan.closescan();

      if (status == false) {
        return false;
      }
    }
    return true;
  }

  public static void insertTable(String datafilename, String tablename, int InputIndexType, int NumBuf)
      throws HFException, HFBufMgrException, HFDiskMgrException, IOException, iterator.FileScanException,
      iterator.MapUtilsException, iterator.InvalidRelation {
    boolean status_1 = false;
    status_1 = ReadFromFile(datafilename, tablename, InputIndexType, NumBuf);
    if (status_1 == true) {
      System.out.println("Diskpage read " + PCounter.rcounter + " Disk page written " + PCounter.wcounter);
    }
  }// end of main

  public static boolean ReadFromFile(String datafileN, String tablename, int InputIndexType_1, int NumBuf)
      throws HFException, HFBufMgrException, HFDiskMgrException, IOException, FileScanException, MapUtilsException,
      iterator.InvalidRelation {
    boolean status = true;
    BufferedReader br = null;
    int linecount = 0;
    String line = "";
    String csvSplitBy = ",";
    MID rid = new MID();
    // Temporary HeapFile to read from file
    Heapfile f = new Heapfile(null);
    boolean found_delete_flag = true;

    if (status == true) {
      try {
        br = new BufferedReader(new FileReader(datafileN));
        while ((line = br.readLine()) != null) {
          String[] arryfields = line.split(csvSplitBy);
          // Get User Input
          String rowLabel = arryfields[0];
          String columnLabel = arryfields[1];
          String value = arryfields[2];
          String timeStamp = arryfields[3];
          // Create map
          Map temp = new Map(GlobalConst.MAP_LEN);
          temp.setHdr((short) MapSchema.MapFldCount(), MapSchema.MapAttrType(), MapSchema.MapStrLengths());
          temp.setRowLabel(rowLabel);
          temp.setColumnLabel(columnLabel);
          temp.setValue(value);
          temp.setTimeStamp(Integer.parseInt(timeStamp));
          try {
            rid = f.insertMap(temp.getMapByteArray());
            // }
          } catch (Exception e) {
            status = false;
            System.err.println("*** Error inserting record " + linecount + "\n");
            e.printStackTrace();
          }
          linecount++;
        }
      } // end of try
      catch (Exception e) {
        e.printStackTrace();
      }
    } // if status okay
    // TODO Bring Duplicate Map Elimination back
    // Sort by RL CL TS DESC
    // This makes it easier to find duplicate maps
    // and we can make one pass

    if (linecount == 0)
      return false;
    bigt big = null;
    new bigt(tablename, InputIndexType_1);
    big = SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex];

    AttrType[] attrTypes = MapSchema.MapAttrType();
    short fldCount = (short) MapSchema.MapFldCount();
    FldSpec[] output = MapSchema.OutputMapSchema();
    short[] strLengths = MapSchema.MapStrLengths();
    // Sort by order type passed in
    FileScan fscan = new FileScan(f, attrTypes, strLengths, fldCount, fldCount, output, null);
    Iterator sort = BuildSortOrder(fscan, attrTypes, strLengths, InputIndexType_1, NumBuf);
    try {
      // build or open btreefile. File format: tablename + '-btree-' + type.
      BTreeFile btf = null;
      switch (InputIndexType_1) {
      case OrderType.type1:
        // no index
        break;
      case OrderType.type2:
        // index and sort by row label
        btf = new BTreeFile(String.format("%s-btree-%s", tablename, new Integer(InputIndexType_1).toString()),
            AttrType.attrString, STR_LEN, 1);
        break;
      case OrderType.type3:
        // index and sort by col label
        btf = new BTreeFile(String.format("%s-btree-%s", tablename, new Integer(InputIndexType_1).toString()),
            AttrType.attrString, STR_LEN, 1);
        break;
      case OrderType.type4:
        // index and sort by col + row
        btf = new BTreeFile(String.format("%s-btree-%s", tablename, new Integer(InputIndexType_1).toString()),
            AttrType.attrCombined, STR_LEN * 2, 1);
        break;
      case OrderType.type5:
        // index and sort by row + value
        btf = new BTreeFile(String.format("%s-btree-%s", tablename, new Integer(InputIndexType_1).toString()),
            AttrType.attrCombined, STR_LEN * 2, 1);
        break;
      }
      Map temp = new Map(GlobalConst.MAP_LEN);
      temp.setHdr((short) MapSchema.MapFldCount(), MapSchema.MapAttrType(), MapSchema.MapStrLengths());
      MID mid = new MID();
      temp = sort.get_next();
      while (temp != null) {
        Map amap = new Map(temp.getMapByteArray(), 0, GlobalConst.MAP_LEN);
        mid = big.insertMap(amap.getMapByteArray());
        switch (InputIndexType_1) {
        case OrderType.type1:
          break;
        case OrderType.type2:
          btf.insert(new StringKey(amap.getRowLabel()), mid);
          break;
        case OrderType.type3:
          btf.insert(new StringKey(amap.getColumnLabel()), mid);
          break;
        case OrderType.type4:
          btf.insert(new CombinedKey(amap.getColumnLabel(), amap.getRowLabel()), mid);
          break;
        case OrderType.type5:
          btf.insert(new CombinedKey(amap.getRowLabel(), amap.getValue()), mid);
          break;
        }
        temp = sort.get_next();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    // Close everything or we an error is thrown
    try {
      sort.close();
      fscan.close();
      f.deleteFile();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return status;
  }

  private static Iterator BuildSortOrder(Iterator fscan, AttrType[] attrType, short[] attrSize, int orderType,
      int numbuf) {
    if (orderType == OrderType.type1)
      return fscan;

    MapOrder ReturnOrder = new MapOrder(MapOrder.Ascending);
    // the fields we will sort
    int[] sort_flds = null;
    // the length of those fields
    int[] fld_lens = null;
    int sort_fld = -1;
    int fld_len = -1;
    switch (orderType) {
    // No Index, No Order
    case OrderType.type1:
      break;
    // rowLabel Index and Order
    case OrderType.type2:
      sort_fld = 1;
      fld_len = STR_LEN;
      break;
    // colLabel Index and Order
    case OrderType.type3:
      sort_fld = 2;
      fld_len = STR_LEN;
      break;
    // colLabel & rowLabel Index and Order
    case OrderType.type4:
      sort_flds = new int[] { 2, 1 };
      fld_lens = new int[] { STR_LEN, STR_LEN };
      break;
    // rowLabel % value Index and Order
    case OrderType.type5:
      sort_flds = new int[] { 1, 3 };
      fld_lens = new int[] { STR_LEN, STR_LEN };
      break;
    }
    Sort sort = null;
    try {
      if (orderType == OrderType.type2 || orderType == OrderType.type3) {
        sort = new Sort(attrType, (short) 4, attrSize, fscan, sort_fld, ReturnOrder, fld_len, numbuf);
      } else
        sort = new Sort(attrType, (short) 4, attrSize, fscan, sort_flds, ReturnOrder, fld_lens, numbuf);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return sort;
  }

  public static boolean InsertBTmap(String datafileN, String tablename, int InputIndexType_1)
      throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
    boolean status = true;
    bigt big = null;
    new bigt(tablename, InputIndexType_1);
    big = SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex];

    BufferedReader br = null;
    int linecount = 0;
    String line = "";
    String csvSplitBy = ",";
    int recleng2 = MAP_LEN;

    // System.out.println("\n Test 1: Insert and scan fixed-size records\n");
    MID rid = new MID();
    Heapfile f = null;

    boolean found_delete_flag = true;

    System.out.println("  - Create a heap file\n");

    if (status == true) {
      try {
        br = new BufferedReader(new FileReader(datafileN));
        while ((line = br.readLine()) != null) {
          String[] arryfields = line.split(csvSplitBy);
          String rowLabel = arryfields[0];
          String columnLabel = arryfields[1];
          String value = arryfields[2];
          String timeStamp = arryfields[3];

          // fixed length record
          DummyRecord rec = new DummyRecord(recleng2);
          rec.rowlabname = rowLabel;
          rec.collabname = columnLabel;
          rec.valuename = value;
          rec.timestampname = Integer.parseInt(timeStamp);

          try {
            Map recMap = new Map(rec.toByteArray(), 0, rec.getRecLength());
            AttrType[] types = new AttrType[4];
            types[0] = new AttrType(AttrType.attrString);
            types[1] = new AttrType(AttrType.attrString);
            types[2] = new AttrType(AttrType.attrString);
            types[3] = new AttrType(AttrType.attrInteger);
            short[] strSizes = new short[3];
            strSizes[0] = (short) (STR_LEN);
            strSizes[1] = (short) (STR_LEN);
            strSizes[2] = (short) (STR_LEN);

            recMap.setHdr((short) 4, types, strSizes);

            found_delete_flag = found_delete(big.hf, rowLabel, columnLabel, rec.timestampname, value);
            if (found_delete_flag /* found delete flag is false */ ) {
              rid = big.insertMap(recMap.getMapByteArray());
            }
          } catch (Exception e) {
            status = false;
            System.err.println("*** Error inserting record " + linecount + "\n");
            e.printStackTrace();
          }

          linecount++;
        } // end of while loop
        // System.out.println("rowmax: " + rowmax + " colmax: " + colmax + "valmax" +
        // valmax);
        System.out.println("After reading the file, LineCount = " + linecount);

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
    // Setting the 4 fields in the data object
    setRowLabelRec(arecord);
    setColumnLabelRec(arecord);
    setTimeStampRec(arecord);
    setValueRec(arecord);
    data = arecord;

    int RecordLength = MAP_LEN;
    setRecLen(RecordLength);
  }

  /**
   * constructor: translate a tuple to a DummyRecord object it will make a copy of
   * the data in the tuple
   * 
   * @param _atuple: the input tuple
   */
  public DummyRecord(Map _atuple) throws java.io.IOException {
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
    int RL_Length = STR_LEN;
    int CL_Length = STR_LEN;
    int TS_Length = 4;
    int V_Length = STR_LEN;
    setRecLen(MAP_LEN);
    Convert.setStrValue(rowlabname, MAPHEADER_LEN, data);
    Convert.setStrValue(collabname, MAPHEADER_LEN + RL_Length + 2, data);
    Convert.setStrValue(valuename, MAPHEADER_LEN + RL_Length + CL_Length + 4, data);
    Convert.setIntValue(timestampname, MAPHEADER_LEN + RL_Length + CL_Length + V_Length + 6, data);
    return data;
  }

  public void setRowLabelRec(byte[] _data) throws java.io.IOException {
    rowlabname = Convert.getStrValue(MAPHEADER_LEN, _data, STR_LEN);
  }

  public void setColumnLabelRec(byte[] _data) throws java.io.IOException {
    collabname = Convert.getStrValue(MAPHEADER_LEN + STR_LEN + 2, _data, STR_LEN);
  }

  public void setTimeStampRec(byte[] _data) throws java.io.IOException {
    timestampname = Convert.getIntValue(MAPHEADER_LEN + STR_LEN * 3 + 6, _data);
  }

  public void setValueRec(byte[] _data) throws java.io.IOException {
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
