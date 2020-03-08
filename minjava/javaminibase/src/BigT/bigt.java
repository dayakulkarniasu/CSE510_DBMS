package BigT;

/**
 * BigT.bigt, which creates and maintains all the relevant heapﬁles 
 *(and index ﬁles of your choice to organize the data)
 */
public class bigt{
    // Initialize the big table. 
    // type is an integer between 1 and 5 and the different types will correspond 
    // to different clustering and indexing strategies you will use for the bigtable.
    bigt(java.lang.String name, int type){}

    //Delete the bigtable from the database.
    void deleteBigt(){}

    //Return number of maps in the bigtable.
    int getMapCnt(){}

    //Return number of distinct row labels in the bigtable.
    int getRowCnt(){}

    //Return number of distinct column labels in the bigtable.
    int getColumnCnt(){}

    //Insert map into the big table, return its Mid. 
    //The insertMap() method ensures that there are at most three maps with 
    //the same row and column labels, but different timestamps, in the bigtable. 
    //When a fourth is inserted, the one with the oldest label is dropped from the big table.
    MID insertMap(byte[] mapPtr){}

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
    Stream openStream(int orderType, java.lang.String rowFilter, java.lang.String columnFilter, java.lang.String valueFilter){}
}