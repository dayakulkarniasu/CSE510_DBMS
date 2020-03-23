package BigT;

import java.io.*;
import global.*;
import heap.*;
import index.IndexException;
import index.UnknownIndexTypeException;

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
    private int BTType;

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
        if (SystemDefs.JavabaseDB.table == null) {
            this.name = name;
            hf = new Heapfile(name);
            BTType = type;
            SystemDefs.JavabaseDB.table = this;
        } else {
            System.out.println("bigt: DB existing");
            System.out.println("bigDB name: " + SystemDefs.JavabaseDBName);
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
        return hf.insertMap(mapPtr);
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
        return new Stream(null, orderType, valueFilter, valueFilter, valueFilter);
    }
}
