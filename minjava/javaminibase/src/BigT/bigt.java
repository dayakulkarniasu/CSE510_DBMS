package BigT;

import java.io.*;
import global.*;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;
import iterator.Iterator;

interface Tabletype {
    int TEMP = 0;
    int ORDINARY = 1;
}

/**
 * BigT.bigt, which creates and maintains all the relevant heapﬁles (and index
 * ﬁles of your choice to organize the data)
 */
public class bigt implements Tabletype, GlobalConst {

    public String name;
    public Heapfile hf;
    public int BTType;
    public String heapFileName;

    // Initialize the big table. A null name produces a temporary heapfile while
    // will be deleted
    // by the destructor. If the name already denotes a file, the file is opened;
    // otherwise, a new empty file
    // is created.
    // @param type an integer between 1 and 5 and the different types will
    // correspond
    // to different clustering and indexing strategies you will use for the
    // bigtable.
    public bigt(String name, int type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {

        System.out.println("\nbigt: NumberOfTables : " + SystemDefs.JavabaseDB.NumberOfTables);
        String inputTableName;
        inputTableName = name + "_" + String.valueOf(type);
        System.out.println("Input Table Name : " + name);
        if (SystemDefs.JavabaseDB.NumberOfTables == 0) {
            // this.name = name;
            this.name = name;
            heapFileName = inputTableName;
            hf = new Heapfile(inputTableName);
            BTType = type;
            SystemDefs.JavabaseDB.table[0] = this;
            SystemDefs.JavabaseDB.CurrentTableIndex = 0;
            SystemDefs.JavabaseDB.NumberOfTables = 1;
            System.out.println("bigt: Setting up DB");
            System.out.println("bigDB tablename: "
                    + SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].name + " Heapfile name = "
                    + SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].heapFileName);
            System.out.println("HeapFile Name : "
                    + SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].heapFileName + " BTType : "
                    + BTType + "\n");
        } else {
            boolean found = false;
            for (int i = 0; i < SystemDefs.JavabaseDB.NumberOfTables; i++) {
                if (name.equals(SystemDefs.JavabaseDB.table[i].name) && type == SystemDefs.JavabaseDB.table[i].BTType) {
                    SystemDefs.JavabaseDB.CurrentTableIndex = i;
                    found = true;
                    System.out.println("bigt: DB existing");
                    System.out.println("bigDB heapFileName : " + SystemDefs.JavabaseDB.table[i].heapFileName);
                }
            }

            if (found == false) {
                System.out.println("bigt: Could not find tablename, creating a new heapfile ");
                // this.name = name;
                this.name = name;
                this.heapFileName = inputTableName;
                hf = new Heapfile(inputTableName);
                BTType = type;
                SystemDefs.JavabaseDB.CurrentTableIndex = SystemDefs.JavabaseDB.NumberOfTables;
                SystemDefs.JavabaseDB.NumberOfTables++;
                SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex] = this;
                System.out.println("bigt: Setting up DB, this.name = " + this.name);
                System.out.println(
                        "bigDB tablename: " + SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].name
                                + " Heapfile name = "
                                + SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].heapFileName);
                System.out.println("Heap File Name : " + name + " BTType : " + BTType);
            }
        }
    } // end of constructor

    public bigt(int Type) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        this.name = "HeapFile_1";
        BTType = Type;
        this.heapFileName = "HeapFile_1";
    }

    public bigt() throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        this.name = "TEMP";
        this.heapFileName = this.name;
        this.hf = new Heapfile(null);
        this.BTType = -1;
        // String inputTableName ;
        // inputTableName = name + String.valueOf(1) ;
        // if (SystemDefs.JavabaseDB.NumberOfTables == 0) {
        // this.name = name;
        // heapFileName = inputTableName ;
        // hf = new Heapfile(heapFileName);
        // BTType = 1;
        // SystemDefs.JavabaseDB.table[0] = this;
        // SystemDefs.JavabaseDB.CurrentTableIndex = 0;
        // SystemDefs.JavabaseDB.NumberOfTables = 1;
        /// * System.out.println("bigt: Setting up DB");
        // System.out.println("bigDB tablename: " +
        // SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].name);
        // System.out.println("Heap File Name : " + name + " BTType : " + BTType);
        // */
        // } else {
        // boolean found = false;
        // int i;
        // for (i=0; i < SystemDefs.JavabaseDB.NumberOfTables; i++) {
        // if ( name.equals(SystemDefs.JavabaseDB.table[i].name)) {
        // SystemDefs.JavabaseDB.CurrentTableIndex = i ;
        // // this = SystemDefs.JavabaseDB.table[i] ;
        // found = true;
        /// * System.out.println("bigt: DB existing");
        // System.out.println("bigDB tablename: " +
        // SystemDefs.JavabaseDB.table[i].name);
        // */
        // }
        // }
        // if (found == false) {
        // System.out.println("bigt: Could not find tablename, creating a new heapfile
        // ");
        // this.name = name;
        // heapFileName = inputTableName ;
        // hf = new Heapfile(heapFileName);
        // BTType = 1;
        // SystemDefs.JavabaseDB.CurrentTableIndex =
        // SystemDefs.JavabaseDB.NumberOfTables;
        // SystemDefs.JavabaseDB.NumberOfTables++ ;
        // SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex] = this;
        /// * System.out.println("bigt: Setting up DB, this.name = " + this.name);
        // System.out.println("bigDB tablename: " +
        // SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].name);
        // System.out.println("Heap File Name : " + name + " BTType : " + BTType);
        // */
        // }
        // }
    } // end of constructor

    public bigt(Iterator am, String btName) throws Exception {
        this.name = btName;
        this.heapFileName = btName;
        this.hf = new Heapfile(btName);
        this.BTType = 1;
        int count = 0;
        Map test = new Map(GlobalConst.MAP_LEN);
        while ((test = am.get_next()) != null) {
            System.out.println("++++++++++++++++++++++");
            System.out.println("RL " + test.getRowLabel());
            System.out.println("CL " + test.getColumnLabel());
            System.out.println("V " + test.getValue());
            System.out.println("TS " + test.getTimeStamp());
            Map amap = new Map(test.getMapByteArray(), 0, GlobalConst.MAP_LEN);
            this.hf.insertMap(amap.getMapByteArray());
            count++;
        }
        // Were any records added?
        // if not, do not create bt
        if (count != 0) {
            SystemDefs.JavabaseDB.CurrentTableIndex = SystemDefs.JavabaseDB.NumberOfTables;
            SystemDefs.JavabaseDB.NumberOfTables++;
            SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex] = this;
        } else {
            this.hf.deleteFile();
        }

    } // end of constructor

    // Delete the bigtable from the database.
    public void deleteBigt() throws InvalidSlotNumberException, FileAlreadyDeletedException, InvalidTupleSizeException,
            HFBufMgrException, HFDiskMgrException, IOException {
        hf.deleteFile();
    }

    // Return number of maps in the bigtable.
    public int getMapCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException {
        return hf.getMapCnt();
    }

    // Return number of distinct row labels in the bigtable.
    public int getRowCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException, FieldNumberOutOfBoundException {
        return hf.getRowCnt();
    }

    // Return number of distinct column labels in the bigtable.
    public int getColumnCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFDiskMgrException,
            HFBufMgrException, IOException, FieldNumberOutOfBoundException {
        return hf.getColCnt();
    }

    // Insert map into the big table, return its Mid.
    // The insertMap() method ensures that there are at most three maps with
    // the same row and column labels, but different timestamps, in the bigtable.
    // When a fourth is inserted, the one with the oldest label is dropped from the
    // big table.
    public MID insertMap(byte[] mapPtr) throws InvalidSlotNumberException, InvalidTupleSizeException,
            SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        System.out.println("In bigt.java, big.hf.name: " + hf.getFileName());
        return SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex].hf.insertMap(mapPtr);
    }

    /*
     * Initialize a stream of maps where row label matching rowFilter, column label
     * matching columnFilter, and value label matching valueFilter. If any of the
     * ﬁlter are null strings, then that ﬁlter is not considered (e.g., if rowFilter
     * is null, then all row labels are OK). If orderType is: 1, then results are
     * ﬁrst ordered in row label, then column label, then time stamp · 2, then
     * results are ﬁrst ordered in column label, then row label, then time stamp ·
     * 3, then results are ﬁrst ordered in row label, then time stamp · 4, then
     * results are ﬁrst ordered in column label, then time stamp · 5, then results
     * are ordered in time stamp
     */
    public Stream openStream(int orderType, String rowFilter, String columnFilter, String valueFilter)
            throws InvalidTupleSizeException, IndexException, InvalidTypeException, UnknownIndexTypeException,
            IOException {
        return new Stream(null, orderType, rowFilter, columnFilter, valueFilter, 12);
    }
}
