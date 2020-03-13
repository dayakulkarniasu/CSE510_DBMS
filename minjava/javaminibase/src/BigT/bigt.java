package BigT;

import java.lang.*;
import java.util.*;
import java.util.stream.*;
import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

interface Tabletype {
    int TEMP = 0;
    int ORDINARY = 1;
}

/**
 * BigT.bigt, which creates and maintains all the relevant heapﬁles 
 *(and index ﬁles of your choice to organize the data)
 */
public class bigt implements Tabletype, GlobalConst{

    private Heapfile hf;
    private int BTType;

    // private int rowCnt;
    // private int columnCnt;
    // private int mapCnt;

    // Initialize the big table. A null name produces a temporary heapfile while will be deleted
    // by the destructor. If the name already denotes a file, the file is opened; otherwise, a new empty file
    // is created.
    // @param type an integer between 1 and 5 and the different types will correspond 
    //      to different clustering and indexing strategies you will use for the bigtable.
    bigt(String name, int type)
        throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException
    {
        hf = new Heapfile(name);
        BTType = type;
    } // end of constructor

    //Delete the bigtable from the database.
    public void deleteBigt()
        throws InvalidSlotNumberException,
            FileAlreadyDeletedException,
            InvalidMapSizeException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException
    {
        hf.deleteFile();
    }

    //Return number of maps in the bigtable.
    public int getMapCnt()
        throws InvalidSlotNumberException,
            InvalidMapSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException
    {
        return hf.getMapCnt();
    }

    //Return number of distinct row labels in the bigtable.
    public int getRowCnt()
        throws InvalidSlotNumberException, 
            InvalidMapSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException
    {
        return hf.getRowCnt();
    }

    //Return number of distinct column labels in the bigtable.
    public int getColumnCnt()
        throws InvalidSlotNumberException,
            InvalidMapSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException
    {
        return hf.getColCnt();
    }

    //Insert map into the big table, return its Mid. 
    //The insertMap() method ensures that there are at most three maps with 
    //the same row and column labels, but different timestamps, in the bigtable. 
    //When a fourth is inserted, the one with the oldest label is dropped from the big table.
    public MID insertMap(byte[] mapPtr)
        throws InvalidSlotNumberException,
            InvalidMapSizeException,
            SpaceNotAvailableException,
            HFException, HFBufMgrException,
            HFDiskMgrException,
            IOException
    {
        return hf.insertMap(mapPtr);
    }

    /*
    Initialize a stream of maps where row label matching rowFilter, 
    column label matching columnFilter, and value label matching valueFilter. 
    If any of the ﬁlter are null strings, then that ﬁlter is not considered 
    (e.g., if rowFilter is null, then all row labels are OK). 
    If orderType is:
    1, then results are ﬁrst ordered in row label, then column label, then time stamp · 
    2, then results are ﬁrst ordered in column label, then row label, then time stamp · 
    3, then results are ﬁrst ordered in row label, then time stamp · 
    4, then results are ﬁrst ordered in column label, then time stamp · 
    5, then results are ordered in time stamp
    */
    public Stream<Map> openStream(int orderType, String rowFilter, String columnFilter, String valueFilter)
    {
        return new Stream<Map>();
    }
}