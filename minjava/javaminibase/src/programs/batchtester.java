package programs;

import btree.*;
import global.*;
import BigT.*;
import heap.*;

import java.io.*;
//import global.*;

public class batchtester implements GlobalConst{
    public static void main(String[] args) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        String filepath = "./";
        String datafilename = args[0];
        int type = Integer.parseInt(args[1]);
        String bigTableName = args[2];

         int reclen = MAP_LEN;

        //TODO System Defs
        //SystemDefsBigDB sysDefs = new SystemDefsBigDB();
       /*BufferedReader br = null;
        String line = "";
        String csvSplitBy = ",";
        try{
            br = new BufferedReader(new FileReader(datafilename));
            while((line = br.readLine()) != null){
                String[] arryfields = line.split(csvSplitBy);
                String rowLabel = arryfields[0];
                String columnLabel = arryfields[1];
                String timeStamp = arryfields[2];
                String value = arryfields[3];
                System.out.println("row label: "+ rowLabel + " col label: " + columnLabel + " TS: " + timeStamp  + " val: " + value);

                //TODO Map
                Map m = new Map();
                */

                //TODO insert into big table

                //TODO take big table, scan it and index based on type

                // TODO
                //At this point we're going to make indexes for our
                //maps. (The indexes should probabally be inside of the bigT constructor
                // but for now I am putting it here)
                // when we create an index we need to give it a name to find it
                // I think we should use the following names
                // Index File Nmes
                // type 1 - no index
                // type 2 - type2Idx
                // type 3 - type3Idx
                // type 4 - type4CombKeyIdx & type4TSIdx
                // type 5 - type5CombKeyIdx & type5TSIdx
                /******************/
                //testing insertion of map

                 String dbpath;
                 String logpath;

                dbpath = "/tmp/"+System.getProperty("user.name")+bigTableName+".db";
                logpath = "/tmp/"+System.getProperty("user.name")+bigTableName+".log";

                SystemDefs sysdef = new SystemDefs( dbpath ,type, 8193,  100, "Clock" );
                String newdbpath;
                String newlogpath;
                String remove_logcmd;
                String remove_dbcmd;
                String remove_cmd = "/bin/rm -rf ";

                newdbpath = dbpath;
                newlogpath = logpath;

                remove_logcmd = remove_cmd + logpath;
                remove_dbcmd = remove_cmd + dbpath;

                // Commands here is very machine dependent.  We assume
                // user are on UNIX system here
                try {
                  Runtime.getRuntime().exec(remove_logcmd);
                  Runtime.getRuntime().exec(remove_dbcmd);
                }
                catch (IOException e) {
                  System.err.println ("IO error: "+e);
                }

                bigt big = new bigt(bigTableName, type);
                BufferedReader br = new BufferedReader(new FileReader(datafilename));
                String csvSplitBy = ",";
                String line;
                MID rid = new MID();
                Heapfile f = null;
                int linecount = 0;
                boolean status = true;
                while((line = br.readLine()) != null){
                    String[] arryfields = line.split(csvSplitBy);
                    String rowLabel = arryfields[0];
                    String columnLabel = arryfields[1];
                    String value = arryfields[2];
                    String timeStamp = arryfields[3];

                    //fixed length record
                    DummyRecord rec = new DummyRecord(reclen);
                    rec.rowlabname = rowLabel;
                    rec.collabname = columnLabel;
                    rec.timestampname = Integer.parseInt(timeStamp);
                    rec.valuename = value;
                    System.out.println("Printing the Dummy record object fields in Main");

                    System.out.println(" Dummy row label: "+ rec.rowlabname + " col label: " + rec.collabname + " TS: " + rec.timestampname  + " val: " + rec.valuename);


                    try {
                      //  Map recMap = new Map(rec.toByteArray(),0, rec.getRecLength()) ;
                          Map recMap = new Map(rec.toByteArray(),0, rec.getRecLength()) ;
                          //  System.out.println(" RecMap created successfully ");
                            AttrType[] types = {new AttrType(0),new AttrType(0),new AttrType(1),new AttrType(0)};
                            types[0] = new AttrType(0) ;
                            types[1] = new AttrType(0) ;
                            types[3] = new AttrType(1) ;
                            types[2] = new AttrType(0) ;
                            short[] strSizes = { 0, 0,0,0};
                            strSizes[0] = (short) (rowLabel.length());
                            strSizes[1] = (short) (columnLabel.length()) ;
                            strSizes[3] = (short) (4) ;
                            strSizes[2] = (short) (value.length());
                          //  System.out.println(" types[0] = " + types[0]+ "types[1] = " + types[1]+ " types[2] = " + types[2] + " types[3] = " + types[3]);
                            recMap.setHdr((short) 4, types, strSizes) ;
                          /*  recMap.setFieldOffset((short)1, (short)0);
                            recMap.setFieldOffset((short)2, (short) (rec.rowlabname.length()));
                            recMap.setFieldOffset((short)3, (short) (rec.rowlabname.length() + rec.collabname.length()));
                            recMap.setFieldOffset((short)4, (short) (rec.rowlabname.length() + rec.collabname.length() + 4));
                            recMap.setFieldOffset((short)5, (short) (rec.rowlabname.length() + rec.collabname.length() + 4 + rec.valuename.length()));
                            */
                          /*   recMap.fldOffset[2] = 0;
                             recMap.fldOffset[1] = rec.rowlabname.length();
                             recMap.fldOffset[2] = rec.rowlabname.length() + rec.collabname.length();
                             recMap.fldOffset[3] = rec.rowlabname.length() + rec.collabname.length() + 4;
                             recMap.fldOffset[4] = rec.rowlabname.length() + rec.collabname.length() + 4 + rec.valuename.length;
                             */
                        rid = big.insertMap(recMap.getMapByteArray());
                      }
                      catch (Exception e) {
                        status = false;
                        System.err.println ("*** Error inserting record " + linecount + "\n");
                        e.printStackTrace();
                      }
                      linecount++;
                }
                

                Scan scan = null;

                if ( status == true ) {
                        System.out.println (" In batchInsert - Scan the records just inserted\n");
          
                        try {
                             scan = big.hf.openScan();
                          //   System.out.println (" In batchInsert - done with f.openScan() \n") ;
                        }
                        catch (Exception e) {
                              status = false;
                              System.err.println ("*** Error opening scan\n");
                              e.printStackTrace();
                        }
          
                        if ( status == true &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                       == SystemDefs.JavabaseBM.getNumBuffers() ) {
                            System.err.println ("*** The heap-file scan has not pinned the first page\n");
                            status = false;
                        }
                }
          
                if ( status == true ) {
                          int len, i = 0;
                          DummyRecord rec = null;
                          Map aMap = new Map();
          
                          boolean done = false;
                          while (!done) {
                              try {
                              //  System.out.println (" In batchInsert, before  scan.getNext(rid) , rid.pageNo.pid = " + rid.pageNo.pid );
          
                                aMap = scan.getNext(rid);
                                if (aMap == null) {
                                  done = true;
                                  break;
                                }
                              }
                              catch (Exception e) {
                                status = false;
                                e.printStackTrace();
                              }
          
                              if (status == true && !done) {
                                try {
                                //  System.out.println ("From Scan, getting next Map and converting in to DummyRecord \n");
                                  rec = new DummyRecord(aMap);
                                //  System.out.println ("From Scan, After converting in to DummyRecord \n");
                                }
                                catch (Exception e) {
                                  System.err.println (""+e);
                                  e.printStackTrace();
                                }
          
                                len = aMap.getLength();
                                if ( len != reclen ) {
                                  System.err.println ("*** Record " + i + " had unexpected length "
                                    + len + "\n");
                                  status = false;
                                  break;
                                }
                                else if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                                    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                                  System.err.println ("On record " + i + ":\n");
                                  System.err.println ("*** The heap-file scan has not left its " +
                                    "page pinned\n");
                                  status = false;
                                  break;
                                }
                                String name = ("record" + i );
                                  System.out.println("rec.row "+ i + " :" + rec.rowlabname);
                                  System.out.println("rec.col "+ i + " :" +  rec.collabname);
                                  System.out.println("rec.timestamp "+ i + " :" +  rec.timestampname);
                                  System.out.println("rec.value "+ i + " :" +  rec.valuename);
                                /*if( (rec.ival != i)
                                    || (rec.fval != (float)i*2.5)
                                    || (!name.equals(rec.name)) ) {
                                  System.err.println ("*** Record " + i
                                    + " differs from what we inserted\n");
                                  System.err.println ("rec.ival: "+ rec.ival
                                    + " should be " + i + "\n");
                                  System.err.println ("rec.fval: "+ rec.fval
                                    + " should be " + (i*2.5) + "\n");
                                  System.err.println ("rec.name: " + rec.name
                                    + " should be " + name + "\n");
                                  status = false;
                                  break;
                                }*/
                              }
                              ++i;
                          }//end of while not done
          
                          //If it gets here, then the scan should be completed
                          if (status == true) {
                              if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                                   != SystemDefs.JavabaseBM.getNumBuffers() ) {
                                    System.err.println ("*** The heap-file scan has not unpinned " +
                                            "its page after finishing\n");
                                    status = false;
                              }
                              else if ( i != (linecount) )
                                {
                                  status = false;
          
                                  System.err.println ("*** Scanned " + i + " records instead of "
                                     + linecount + "\n");
                                }
                          }
                }//end of  bigger status ok



                   //insert map
                //  InsertBTmap(datafilename);



                /******************/



                // putting this here for now
                /*bigt big = new bigt(bigTableName, type);
                BTreeFile btf = null;
                switch (type){
                    case DBType.type1:
                        //TODO need to research this one a little more
                        //no index
                        break;
                    case DBType.type2:
                        //one btree to index row labels
                        try {
                            btf = new BTreeFile("type2Idx", AttrType.attrString, big.getRowCnt(), 1);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }

                        mid = new MID();
                        String key = null;
                        Map temp = null;

                        try {
                            temp = Stream.getNext(mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                        // itterate through all the maps
                        while ( temp != null) {
                            m.mapCopy(temp);

                            try {
                                // the key for Type 2 is the row
                                key = m.getRowLabel();
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }

                            try {
                                // insert the key and the 'pointer' Map Id into btree index
                                btf.insert(new StringKey(key), mid);
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }

                            try {
                                // get next map
                                temp = Stream.getNext(mid);
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        // close the file scan
                        scan.closescan();

                        //BTreeIndex file created successfully

                        break;
                    case DBType.type3:
                        // one btree to index column labels
                        break;
                    case DBType.type4:
                        // one btree to index column label and row label (combined key)
                        // one btree to index time stamps
                        break;
                    case DBType.type5:
                        // one btree to index row label and value (combined key)
                        // one btree to index time stamps
                        break;
                    default:
*/
                //}
          //  }


          //  }
      /*  }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

    }//end of main
    public static boolean InsertBTmap(String datafileN)  {

      BufferedReader br = null;
      int linecount = 0;
       String line = "";
       String csvSplitBy = ",";
       int recleng2 = 64;

      System.out.println ("\n  Test 1: Insert and scan fixed-size records\n");
      boolean status = true;
      MID rid = new MID();
      Heapfile f = null;

      System.out.println ("  - Create a heap file\n");
      try {
        f = new Heapfile("file_1");
      }
      catch (Exception e) {
        status = false;
        System.err.println ("*** Could not create heap file\n");
        e.printStackTrace();
      }

      if ( status == true && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
  	 != SystemDefs.JavabaseBM.getNumBuffers() ) {
        System.err.println ("*** The heap file has left pages pinned\n");
        status = false;
      }

      if ( status == true ) {
        System.out.println ("  - Add " + linecount + " records to the file\n");


        //for (int i =0; (i < choice) && (status == true); i++) {
        try{
            br = new BufferedReader(new FileReader(datafileN));
            while((line = br.readLine()) != null){
                String[] arryfields = line.split(csvSplitBy);
                String rowLabel = arryfields[0];
                String columnLabel = arryfields[1];
                String value = arryfields[2];
                String timeStamp = arryfields[3];
                
                //System.out.println("row label: "+ rowLabel + " col label: " + columnLabel + " TS: " + timeStamp  + " val: " + value);


      	        //fixed length record
              	DummyRecord rec = new DummyRecord(recleng2);
              	rec.rowlabname = rowLabel;
              	rec.collabname = columnLabel;
              	rec.timestampname = Integer.parseInt(timeStamp);
                rec.valuename = value;
                System.out.println("Printing the Dummy record object fields in Main");

                System.out.println(" Dummy row label: "+ rec.rowlabname + " col label: " + rec.collabname + " TS: " + rec.timestampname  + " val: " + rec.valuename);

              	try {
                //  Map recMap = new Map(rec.toByteArray(),0, rec.getRecLength()) ;
                    Map recMap = new Map(rec.toByteArray(),0, rec.getRecLength()) ;
                    //  System.out.println(" RecMap created successfully ");
                      AttrType[] types = {new AttrType(0),new AttrType(0),new AttrType(1),new AttrType(0)};
                      types[0] = new AttrType(0) ;
                      types[1] = new AttrType(0) ;
                      types[3] = new AttrType(1) ;
                      types[2] = new AttrType(0) ;
                      short[] strSizes = { 0, 0,0,0};
                      strSizes[0] = (short) (rowLabel.length());
                      strSizes[1] = (short) (columnLabel.length()) ;
                      strSizes[3] = (short) (4) ;
                      strSizes[2] = (short) (value.length());
                    //  System.out.println(" types[0] = " + types[0]+ "types[1] = " + types[1]+ " types[2] = " + types[2] + " types[3] = " + types[3]);
                      recMap.setHdr((short) 4, types, strSizes) ;
                    /*  recMap.setFieldOffset((short)1, (short)0);
                      recMap.setFieldOffset((short)2, (short) (rec.rowlabname.length()));
                      recMap.setFieldOffset((short)3, (short) (rec.rowlabname.length() + rec.collabname.length()));
                      recMap.setFieldOffset((short)4, (short) (rec.rowlabname.length() + rec.collabname.length() + 4));
                      recMap.setFieldOffset((short)5, (short) (rec.rowlabname.length() + rec.collabname.length() + 4 + rec.valuename.length()));
                      */
                    /*   recMap.fldOffset[2] = 0;
                       recMap.fldOffset[1] = rec.rowlabname.length();
                       recMap.fldOffset[2] = rec.rowlabname.length() + rec.collabname.length();
                       recMap.fldOffset[3] = rec.rowlabname.length() + rec.collabname.length() + 4;
                       recMap.fldOffset[4] = rec.rowlabname.length() + rec.collabname.length() + 4 + rec.valuename.length;
                       */
              	  rid = f.insertMap(recMap.getMapByteArray());
              	}
              	catch (Exception e) {
              	  status = false;
              	  System.err.println ("*** Error inserting record " + linecount + "\n");
              	  e.printStackTrace();
              	}

              	if ( status == true && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
              	     != SystemDefs.JavabaseBM.getNumBuffers() ) {

              	  System.err.println ("*** Insertion left a page pinned\n");
              	  status = false;
              	}
      //  }
      linecount++;
    }//end of while loop
    System.out.println("After reading the file, LineCoiunt = "+ linecount);

  }// end of try
  catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
    }

        try {
          	if ( f.getMapCnt() != linecount ) {
          	  status = false;
          	  System.err.println ("*** File reports " + f.getMapCnt() +
          			      " records, not " + linecount + "\n");
          	}
        }
        catch (Exception e) {
          	status = false;
          	System.out.println (""+e);
          	e.printStackTrace();
        }
      } // if status okay

      // In general, a sequential scan won't be in the same order as the
      // insertions.  However, we're inserting fixed-length records here, and
      // in this case the scan must return the insertion order.

      Scan scan = null;

      if ( status == true ) {
              System.out.println (" In batchInsert - Scan the records just inserted\n");

              try {
        	         scan = f.openScan();
                //   System.out.println (" In batchInsert - done with f.openScan() \n") ;
              }
              catch (Exception e) {
                  	status = false;
                  	System.err.println ("*** Error opening scan\n");
                  	e.printStackTrace();
              }

              if ( status == true &&  SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
        	   == SystemDefs.JavabaseBM.getNumBuffers() ) {
                	System.err.println ("*** The heap-file scan has not pinned the first page\n");
                	status = false;
              }
      }

      if ( status == true ) {
                int len, i = 0;
                DummyRecord rec = null;
                Map aMap = new Map();

                boolean done = false;
                while (!done) {
                  	try {
                    //  System.out.println (" In batchInsert, before  scan.getNext(rid) , rid.pageNo.pid = " + rid.pageNo.pid );

                  	  aMap = scan.getNext(rid);
                  	  if (aMap == null) {
                  	    done = true;
                  	    break;
                  	  }
                  	}
                  	catch (Exception e) {
                  	  status = false;
                  	  e.printStackTrace();
                  	}

                  	if (status == true && !done) {
                  	  try {
                      //  System.out.println ("From Scan, getting next Map and converting in to DummyRecord \n");
                  	    rec = new DummyRecord(aMap);
                      //  System.out.println ("From Scan, After converting in to DummyRecord \n");
                  	  }
                  	  catch (Exception e) {
                  	    System.err.println (""+e);
                  	    e.printStackTrace();
                  	  }

                  	  len = aMap.getLength();
                  	  if ( len != recleng2 ) {
                  	    System.err.println ("*** Record " + i + " had unexpected length "
                  				+ len + "\n");
                  	    status = false;
                  	    break;
                  	  }
                  	  else if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                  		    == SystemDefs.JavabaseBM.getNumBuffers() ) {
                  	    System.err.println ("On record " + i + ":\n");
                  	    System.err.println ("*** The heap-file scan has not left its " +
                  				"page pinned\n");
                  	    status = false;
                  	    break;
                  	  }
                  	  String name = ("record" + i );
                        System.out.println("rec.row "+ i + " :" + rec.rowlabname);
                        System.out.println("rec.col "+ i + " :" +  rec.collabname);
                        System.out.println("rec.timestamp "+ i + " :" +  rec.timestampname);
                        System.out.println("rec.value "+ i + " :" +  rec.valuename);
                  	  /*if( (rec.ival != i)
                  	      || (rec.fval != (float)i*2.5)
                  	      || (!name.equals(rec.name)) ) {
                  	    System.err.println ("*** Record " + i
                  				+ " differs from what we inserted\n");
                  	    System.err.println ("rec.ival: "+ rec.ival
                  				+ " should be " + i + "\n");
                  	    System.err.println ("rec.fval: "+ rec.fval
                  				+ " should be " + (i*2.5) + "\n");
                  	    System.err.println ("rec.name: " + rec.name
                  				+ " should be " + name + "\n");
                  	    status = false;
                  	    break;
                  	  }*/
                  	}
                  	++i;
                }//end of while not done

                //If it gets here, then the scan should be completed
                if (status == true) {
                  	if ( SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                  	     != SystemDefs.JavabaseBM.getNumBuffers() ) {
                      	  System.err.println ("*** The heap-file scan has not unpinned " +
                      			      "its page after finishing\n");
                      	  status = false;
                  	}
                  	else if ( i != (linecount) )
                  	  {
                  	    status = false;

                  	    System.err.println ("*** Scanned " + i + " records instead of "
                  	       + linecount + "\n");
                  	  }
                }
      }//end of  bigger status ok

      if ( status == true )
          System.out.println ("  Test 1 completed successfully.\n");

      return status;
    }

}

class DummyRecord implements GlobalConst{

  //content of the record
  public String rowlabname;
  public String collabname;
  public int timestampname;
  public String valuename;

  //length under control
  private int reclen1;

  private byte[]  data;

  /** Default constructor
   */
  public DummyRecord() {}

  /** another constructor
   */
  public DummyRecord (int _reclen) {
    setRecLen (_reclen);
    data = new byte[_reclen];
  }

  /** constructor: convert a byte array to DummyRecord object.
   * @param arecord a byte array which represents the DummyRecord object
   */
  public DummyRecord(byte [] arecord)
    throws java.io.IOException {
//TODO fix the functions and create one mor function
  /*  setIntRec (arecord);
    setFloRec (arecord);
    setStrRec (arecord);
    */
    // Setting the 4 fields in the data object
    setRowLabelRec (arecord);
    setColumnLabelRec (arecord);
    setTimeStampRec (arecord);
    setValueRec (arecord);
    data = arecord;
    //setRecLen(name.length());s
    // Setting the record length = arecord getLength
    int RecordLength = rowlabname.length() + collabname.length() + valuename.length() + valuename.length();
  //setRecLen(data.length());
  setRecLen(RecordLength);
  }

  /** constructor: translate a tuple to a DummyRecord object
   *  it will make a copy of the data in the tuple
   * @param atuple: the input tuple
   */
  public DummyRecord(Map _atuple)
	throws java.io.IOException, FieldNumberOutOfBoundException{
    //  System.out.println (" the length of the map in dummy reccord is: " + _atuple.getLength());
    data = new byte[_atuple.getLength()];
    data = _atuple.getMapByteArray();
    setRecLen(_atuple.getLength());
    /*  String MapRowLabel = "";
      String MapColLabel = "";
      int MapTimeLabel = 0 ;
      String MapValueLabel = "" ;
      */
  //  System.out.println (" In the Dummy Record (Map) function \n");
    //  try {}
    rowlabname = _atuple.getRowLabel() ;
    //  }
      //catch(FieldNumberOutOfBoundException e)
      //{
      //  e.printStackTrace();
      //}
      //System.out.println (" In the Dummy Record (Map) function , Retrieved  rowlabname = " + rowlabname );
//      try{
      collabname = _atuple.getColumnLabel() ;
//      }catch(FieldNumberOutOfBoundException e)
//      {
//        e.printStackTrace();
//      }
//      System.out.println (" In the Dummy Record (Map) function , Retrieved  MapColLabel = " + MapColLabel );

//      try{
      timestampname = _atuple.getTimeStamp() ;
//      System.out.println (" In the Dummy Record (Map) function , Retrieved  MapTimeLabel = " + MapTimeLabel );

//      }catch(FieldNumberOutOfBoundException e)
//      {
//        e.printStackTrace();
//      }

//      try{
      valuename = _atuple.getValue() ;
//      }catch(FieldNumberOutOfBoundException e)
//      {
//        e.printStackTrace();
//      }
//      System.out.println (" In the Dummy Record (Map) function , Retrieved  MapValueLabel = " + MapValueLabel );


//    System.out.println(" Dummy Map Function : "+ MapRowLabel + " col label: " + MapColLabel + " TS: " + MapTimeLabel  + " val: " + MapValueLabel);

  //  setIntRec (data);
  //  setFloRec (data);
  //  setStrRec (data);
/* Commented out as it is not required
    setRowLabelRec (data);
    setColumnLabelRec (data);
    setTimeStampRec (data);
    setValueRec (data);
*/
  }

  /** convert this class objcet to a byte array
   *  this is used when you want to write this object to a byte array
   */
  public byte [] toByteArray()
    throws java.io.IOException {
    //    data = new byte[reclen];
  /*  Convert.setIntValue (ival, 0, data);
    Convert.setFloValue (fval, 4, data);
    Convert.setStrValue (name, 8, data);
    */
    int RL_Length = rowlabname.length();
    // int RL_Length = GlobalConst.STR_LEN;
    int CL_Length = collabname.length();
    int TS_Length = 4;
    int V_Length = valuename.length();
   System.out.println("In toByte Array : TotalLength : " + (16 + RL_Length + 2 + CL_Length + 2 + TS_Length +4 + V_Length + 2));
    setRecLen (MAP_LEN);
  //  setRecLen (16 + RL_Length + 2 + CL_Length + 2 + TS_Length +4 + V_Length + 2);
    // Convert.setStrValue (rowlabname, 16,data);
    // Convert.setStrValue (collabname, 16 + RL_Length + 2,data );
    // Convert.setStrValue (valuename, 16 + RL_Length + CL_Length + 4,data );
    // Convert.setIntValue (timestampname, 16 + RL_Length + CL_Length + V_Length + 4,data);
    Convert.setStrValue(rowlabname, MAPHEADER_LEN, data);
    Convert.setStrValue(collabname, MAPHEADER_LEN + RL_Length + 2, data);
    Convert.setIntValue(timestampname, MAPHEADER_LEN + RL_Length + CL_Length + 4, data);
    Convert.setStrValue(valuename, MAPHEADER_LEN + RL_Length + CL_Length + TS_Length + 4, data);
    // recln1 = RL_Length + CL_Length + TS_Length ;
  // System.out.println("In toByte Array : Data = " + data + " Record length : " + reclen1);

    return data;
  }

  /** get the integer value out of the byte array and set it to
   *  the int value of the DummyRecord object
   */
/*  public void setIntRec (byte[] _data)
    throws java.io.IOException {
    ival = Convert.getIntValue (0, _data);
  }

  /** get the float value out of the byte array and set it to
   *  the float value of the DummyRecord object
   */
/*  public void setFloRec (byte[] _data)
    throws java.io.IOException {
    fval = Convert.getFloValue (4, _data);
  }
*/
  /** get the String value out of the byte array and set it to
   *  the float value of the HTDummyRecorHT object
   */
/*  public void setStrRec (byte[] _data)
    throws java.io.IOException {
   // System.out.println("reclne= "+reclen);
   // System.out.println("data size "+_data.size());
    name = Convert.getStrValue (8, _data, reclen1-8);
  }
  */
  public void setRowLabelRec (byte[] _data)
    throws java.io.IOException {
   // System.out.println("reclne= "+reclen);
   // System.out.println("data size "+_data.size());
    // rowlabname = Convert.getStrValue (0, _data, 16);
    rowlabname = Convert.getStrValue(MAPHEADER_LEN, _data, STR_LEN);
  }
  public void setColumnLabelRec (byte[] _data)
    throws java.io.IOException {
   // System.out.println("reclne= "+reclen);
   // System.out.println("data size "+_data.size());
    // collabname = Convert.getStrValue (16, _data, 16);
    collabname = Convert.getStrValue(MAPHEADER_LEN + STR_LEN, _data, STR_LEN);
  }
  public void setTimeStampRec (byte[] _data)
    throws java.io.IOException {
    // timestampname = Convert.getIntValue (32, _data);
    timestampname = Convert.getIntValue(MAPHEADER_LEN + STR_LEN * 2, _data);
  }
  public void setValueRec (byte[] _data)
    throws java.io.IOException {
   // System.out.println("reclne= "+reclen);
   // System.out.println("data size "+_data.size());
    // valuename = Convert.getStrValue (36, _data, 16);
    valuename = Convert.getStrValue(MAPHEADER_LEN + STR_LEN * 2 + 4, _data, STR_LEN);
  }
  //Other access methods to the size of the String field and
  //the size of the record
  public void setRecLen (int size) {
    reclen1 = size;
  }

  public int getRecLength () {
    return reclen1;
  }
 }
