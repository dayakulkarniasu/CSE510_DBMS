/*  File RID.java   */

package global;

import java.io.*;

public class MID {
    // slot number
    public int slotNo;

    // page number
    public PageId pageNo = new PageId();

    // default constructor
    public MID() {}

    // constructor
    public MID(PageId pageno, int slotno)
    {
        pageNo = pageno;
        slotNo = slotno;
    }

    // make a copy of the given MID
    public void copyMid(MID mid)
    {
        pageNo = mid.pageNo;
        slotNo = mid.slotNo;
    }

    // write the mid into a byte array at offset
    // @param ary the specified byte array
    // @param offset the offset of the byte array to write
    // @exception java.io.IOException I/O errors
    public void writeToByteArray(byte[] ary, int offset) throws java.io.IOException
    {
        Convert.setIntValue(slotNo, offset, ary);
        Convert.setIntValue(pageNo.pid, offset+4, ary);
    }

    // compares two mid objects, i.e. this to the mid
    // @param mid MID object to be compared to
    // @return ture is they are equal, false if not
    public boolean equals(MID mid)
    {
        if((this.pageNo.pid == mid.pageNo.pid) && (this.slotNo == mid.slotNo))
            return true;
        else
            return false;
    }
}