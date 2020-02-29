/* File HFPageBigDB.java */

package heap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;



 /**
  * Define constant values for INVALID_SLOT and EMPTY_SLOT
  */

interface ConstSlot{
  int INVALID_SLOT =  -1;
  int EMPTY_SLOT = -1;
}

/** Class heap file page.
 * The design assumes that maps are kept compacted when
 * deletions are performed.
 */

public class HFPageBigDBBigDB extends Page
  implements ConstSlot, GlobalConst{


  public static final int SIZE_OF_SLOT = 4;
  public static final int DPFIXED =  4 * 2  + 3 * 4;

  public static final int SLOT_CNT = 0;
  public static final int USED_PTR = 2;
  public static final int FREE_SPACE = 4;
  public static final int TYPE = 6;
  public static final int PREV_PAGE = 8;
  public static final int NEXT_PAGE = 12;
  public static final int CUR_PAGE = 16;

  /* Warning:
     These items must all pack tight, (no padding) for
     the current implementation to work properly.
     Be careful when modifying this class.
  */

  /**
   * number of slots in use
   */
  private    short     slotCnt;

  /**
   * offset of first used byte by data maps in data[]
   */
  private    short     usedPtr;

  /**
   * number of bytes free in data[]
   */
  private    short     freeSpace;

  /**
   * an arbitrary value used by subclasses as needed
   */
  private    short     type;

  /**
   * backward pointer to data page
   */
  private    PageId   prevPage = new PageId();

  /**
   * forward pointer to data page
   */
  private   PageId    nextPage = new PageId();

  /**
   *  page number of this page
   */
  protected    PageId    curPage = new PageId();

  /**
   * Default constructor
   */

  public HFPageBigDB ()   {  }

  /**
   * Constructor of class HFPageBigDB
   * open a HFPageBigDB and make this HFpage piont to the given page
   * @param  page  the given page in Page type
   */

  public HFPageBigDB(Page page)
    {
      data = page.getpage();
    }

  /**
   * Constructor of class HFPageBigDB
   * open a existed hfpage
   * @param  apage   a page in buffer pool
   */

  public void openHFpageBigDB(Page apage)
    {
      data = apage.getpage();
    }

  /**
   * Constructor of class HFPageBigDB
   * initialize a new page
   * @param	pageNo	the page number of a new page to be initialized
   * @param	apage	the Page to be initialized
   * @see		Page
   * @exception IOException I/O errors
   */


  public void init(PageId pageNo, Page apage)
    throws IOException
    {
      data = apage.getpage();

      slotCnt = 0;                // no slots in use
      Convert.setShortValue (slotCnt, SLOT_CNT, data);

      curPage.pid = pageNo.pid;
      Convert.setIntValue (curPage.pid, CUR_PAGE, data);

      nextPage.pid = prevPage.pid = INVALID_PAGE;
      Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
      Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);

      usedPtr = (short) MAX_SPACE;  // offset in data array (grow backwards)
      Convert.setShortValue (usedPtr, USED_PTR, data);

      freeSpace = (short) (MAX_SPACE - DPFIXED);    // amount of space available
      Convert.setShortValue (freeSpace, FREE_SPACE, data);

    }

  /**
   * @return byte array
   */

  public byte [] getHFpageBigDBArray()
    {
      return data;
    }

  /**
   * Dump contents of a page
   * @exception IOException I/O errors
   */
  public void dumpPage()
    throws IOException
    {
      int i, n ;
      int length, offset;

      curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
      nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);
      usedPtr =  Convert.getShortValue (USED_PTR, data);
      freeSpace =  Convert.getShortValue (FREE_SPACE, data);
      slotCnt =  Convert.getShortValue (SLOT_CNT, data);

      System.out.println("dumpPage");
      System.out.println("curPage= " + curPage.pid);
      System.out.println("nextPage= " + nextPage.pid);
      System.out.println("usedPtr= " + usedPtr);
      System.out.println("freeSpace= " + freeSpace);
      System.out.println("slotCnt= " + slotCnt);

      for (i= 0, n=DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
        length =  Convert.getShortValue (n, data);
	offset =  Convert.getShortValue (n+2, data);
	System.out.println("slotNo " + i +" offset= " + offset);
        System.out.println("slotNo " + i +" length= " + length);
      }

    }

  /**
   * @return	PageId of previous page
   * @exception IOException I/O errors
   */
  public PageId getPrevPage()
    throws IOException
    {
      prevPage.pid =  Convert.getIntValue (PREV_PAGE, data);
      return prevPage;
    }

  /**
   * sets value of prevPage to pageNo
   * @param       pageNo  page number for previous page
   * @exception IOException I/O errors
   */
  public void setPrevPage(PageId pageNo)
    throws IOException
    {
      prevPage.pid = pageNo.pid;
      Convert.setIntValue (prevPage.pid, PREV_PAGE, data);
    }

  /**
   * @return     page number of next page
   * @exception IOException I/O errors
   */
  public PageId getNextPage()
    throws IOException
    {
      nextPage.pid =  Convert.getIntValue (NEXT_PAGE, data);
      return nextPage;
    }

  /**
   * sets value of nextPage to pageNo
   * @param	pageNo	page number for next page
   * @exception IOException I/O errors
   */
  public void setNextPage(PageId pageNo)
    throws IOException
    {
      nextPage.pid = pageNo.pid;
      Convert.setIntValue (nextPage.pid, NEXT_PAGE, data);
    }

  /**
   * @return 	page number of current page
   * @exception IOException I/O errors
   */
  public PageId getCurPage()
    throws IOException
    {
      curPage.pid =  Convert.getIntValue (CUR_PAGE, data);
      return curPage;
    }

  /**
   * sets value of curPage to pageNo
   * @param	pageNo	page number for current page
   * @exception IOException I/O errors
   */
  public void setCurPage(PageId pageNo)
    throws IOException
    {
      curPage.pid = pageNo.pid;
      Convert.setIntValue (curPage.pid, CUR_PAGE, data);
    }

  /**
   * @return 	the ype
   * @exception IOException I/O errors
   */
  public short getType()
    throws IOException
    {
      type =  Convert.getShortValue (TYPE, data);
      return type;
    }

  /**
   * sets value of type
   * @param	valtype     an arbitrary value
   * @exception IOException I/O errors
   */
  public void setType(short valtype)
    throws IOException
    {
      type = valtype;
      Convert.setShortValue (type, TYPE, data);
    }

  /**
   * @return 	slotCnt used in this page
   * @exception IOException I/O errors
   */
  public short getSlotCnt()
    throws IOException
    {
      slotCnt =  Convert.getShortValue (SLOT_CNT, data);
      return slotCnt;
    }

  /**
   * sets slot contents
   * @param       slotno  the slot number
   * @param 	length  length of map the slot contains
   * @param	offset  offset of map
   * @exception IOException I/O errors
   */
  public void setSlot(int slotno, int length, int offset)
    throws IOException
    {
      int position = DPFIXED + slotno * SIZE_OF_SLOT;
      Convert.setShortValue((short)length, position, data);
      Convert.setShortValue((short)offset, position+2, data);
    }

  /**
   * @param	slotno	slot number
   * @exception IOException I/O errors
   * @return	the length of map the given slot contains
   */
  public short getSlotLength(int slotno)
    throws IOException
    {
      int position = DPFIXED + slotno * SIZE_OF_SLOT;
      short val= Convert.getShortValue(position, data);
      return val;
    }

  /**
   * @param       slotno  slot number
   * @exception IOException I/O errors
   * @return      the offset of map the given slot contains
   */
  public short getSlotOffset(int slotno)
    throws IOException
    {
      int position = DPFIXED + slotno * SIZE_OF_SLOT;
      short val= Convert.getShortValue(position +2, data);
      return val;
    }


  /**
   * inserts a new map onto the page, returns MID of this map
   * @param	map 	a map to be inserted
   * @return	MID of map, null if sufficient space does not exist
   * @exception IOException I/O errors
   * in C++ Status insertMap(char *recPtr, int mapLen, MID& mid)
   */
  public MID insertMap ( byte [] map)
    throws IOException
    {
      MID mid = new MID();

      int mapLen = map.length;
      int spaceNeeded = mapLen + SIZE_OF_SLOT;

      // Start by checking if sufficient space exists.
      // This is an upper bound check. May not actually need a slot
      // if we can find an empty one.

      freeSpace = Convert.getShortValue (FREE_SPACE, data);
      if (spaceNeeded > freeSpace) {
        return null;

      } else {

	// look for an empty slot
	slotCnt = Convert.getShortValue (SLOT_CNT, data);
	int i;
	short length;
	for (i= 0; i < slotCnt; i++)
	  {
	    length = getSlotLength(i);
	    if (length == EMPTY_SLOT)
	      break;
	  }

	if(i == slotCnt)   //use a new slot
	  {
	    // adjust free space
	    freeSpace -= spaceNeeded;
	    Convert.setShortValue (freeSpace, FREE_SPACE, data);

	    slotCnt++;
	    Convert.setShortValue (slotCnt, SLOT_CNT, data);

	  }
	else {
	  // reusing an existing slot
	  freeSpace -= mapLen;
	  Convert.setShortValue (freeSpace, FREE_SPACE, data);
	}

	usedPtr = Convert.getShortValue (USED_PTR, data);
        usedPtr -= mapLen;    // adjust usedPtr
	Convert.setShortValue (usedPtr, USED_PTR, data);

	//insert the slot info onto the data page
	setSlot(i, mapLen, usedPtr);

	// insert data onto the data page
	System.arraycopy (map, 0, data, usedPtr, mapLen);
	curPage.pid = Convert.getIntValue (CUR_PAGE, data);
	mid.pageNo.pid = curPage.pid;
	mid.slotNo = i;
	return   mid ;
      }
    }

  /**
   * delete the map with the specified mid
   * @param	mid 	the map ID
   * @exception	InvalidSlotNumberException Invalid slot number
   * @exception IOException I/O errors
   * in C++ Status deleteMap(const MID& mid)
   */
  public void deleteMap ( MID mid )
    throws IOException,
	   InvalidSlotNumberException
    {
      int slotNo = mid.slotNo;
      short mapLen = getSlotLength (slotNo);
      slotCnt = Convert.getShortValue (SLOT_CNT, data);

      // first check if the map being deleted is actually valid
      if ((slotNo >= 0) && (slotNo < slotCnt) && (mapLen > 0))
	{
	  // The maps always need to be compacted, as they are
	  // not necessarily stored on the page in the order that
	  // they are listed in the slot index.

	  // offset of map being deleted
	  int offset = getSlotOffset(slotNo);
	  usedPtr = Convert.getShortValue (USED_PTR, data);
	  int newSpot= usedPtr + mapLen;
	  int size = offset - usedPtr;

	  // shift bytes to the right
	  System.arraycopy(data, usedPtr, data, newSpot, size);

	  // now need to adjust offsets of all valid slots that refer
	  // to the left of the map being removed. (by the size of the hole)

	  int i, n, chkoffset;
	  for (i = 0, n = DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
	    if ((getSlotLength(i) >= 0))
	      {
		chkoffset = getSlotOffset(i);
		if(chkoffset < offset)
		  {
		    chkoffset += mapLen;
		    Convert.setShortValue((short)chkoffset, n+2, data);
		  }
	      }
	  }

	  // move used Ptr forwar
	  usedPtr += mapLen;
	  Convert.setShortValue (usedPtr, USED_PTR, data);

	  // increase freespace by size of hole
	  freeSpace = Convert.getShortValue(FREE_SPACE, data);
	  freeSpace += mapLen;
	  Convert.setShortValue (freeSpace, FREE_SPACE, data);

	  setSlot(slotNo, EMPTY_SLOT, 0);  // mark slot free
	}
      else {
	throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
      }
    }

  /**
   * @return MID of first map on page, null if page contains no maps.
   * @exception  IOException I/O errors
   * in C++ Status firstMap(MID& firstRid)
   *
   */
  public MID firstMap()
    throws IOException
    {
      MID mid = new MID();
      // find the first non-empty slot


      slotCnt = Convert.getShortValue (SLOT_CNT, data);

      int i;
      short length;
      for (i= 0; i < slotCnt; i++)
	{
	  length = getSlotLength (i);
	  if (length != EMPTY_SLOT)
	    break;
	}

      if(i== slotCnt)
	return null;

      // found a non-empty slot

      mid.slotNo = i;
      curPage.pid= Convert.getIntValue(CUR_PAGE, data);
      mid.pageNo.pid = curPage.pid;

      return mid;
    }

  /**
   * @return MID of next map on the page, null if no more
   * maps exist on the page
   * @param 	curRid	current map ID
   * @exception  IOException I/O errors
   * in C++ Status nextRecord (MID curRid, MID& nextRid)
   */
  public MID nextRecord (MID curRid)
    throws IOException
    {
      MID mid = new MID();
      slotCnt = Convert.getShortValue (SLOT_CNT, data);

      int i=curRid.slotNo;
      short length;

      // find the next non-empty slot
      for (i++; i < slotCnt;  i++)
	{
	  length = getSlotLength(i);
	  if (length != EMPTY_SLOT)
	    break;
	}

      if(i >= slotCnt)
	return null;

      // found a non-empty slot

      mid.slotNo = i;
      curPage.pid = Convert.getIntValue(CUR_PAGE, data);
      mid.pageNo.pid = curPage.pid;

      return mid;
    }

  /**
   * copies out map with MID mid into map pointer.
   * <br>
   * Status getMap(MID mid, char *recPtr, int& mapLen)
   * @param	mid 	the map ID
   * @return 	a Map contains the map
   * @exception   InvalidSlotNumberException Invalid slot number
   * @exception  	IOException I/O errors
   * @see 	Map
   */
  public Map getMap ( MID mid )
    throws IOException,
	   InvalidSlotNumberException
    {
      short mapLen;
      short offset;
      byte []map;
      PageId pageNo = new PageId();
      pageNo.pid= mid.pageNo.pid;
      curPage.pid = Convert.getIntValue (CUR_PAGE, data);
      int slotNo = mid.slotNo;

      // length of map being returned
      mapLen = getSlotLength (slotNo);
      slotCnt = Convert.getShortValue (SLOT_CNT, data);
      if (( slotNo >=0) && (slotNo < slotCnt) && (mapLen >0)
	  && (pageNo.pid == curPage.pid))
	{
	  offset = getSlotOffset (slotNo);
	  map = new byte[mapLen];
	  System.arraycopy(data, offset, map, 0, mapLen);
	  Map map = new Map(map, 0, mapLen);
	  return map;
	}

      else {
        throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
      }


    }

  /**
   * returns a Map in a byte array[pageSize] with given MID mid.
   * <br>
   * in C++	Status returnMap(MID mid, char*& recPtr, int& mapLen)
   * @param       mid     the map ID
   * @return      a Map  with its length and offset in the byte array
   * @exception   InvalidSlotNumberException Invalid slot number
   * @exception   IOException I/O errors
   * @see 	Map
   */
  public Map returnMap ( MID mid )
    throws IOException,
	   InvalidSlotNumberException
    {
      short mapLen;
      short offset;
      PageId pageNo = new PageId();
      pageNo.pid = mid.pageNo.pid;

      curPage.pid = Convert.getIntValue (CUR_PAGE, data);
      int slotNo = mid.slotNo;

      // length of map being returned
      mapLen = getSlotLength (slotNo);
      slotCnt = Convert.getShortValue (SLOT_CNT, data);

      if (( slotNo >=0) && (slotNo < slotCnt) && (mapLen >0)
	  && (pageNo.pid == curPage.pid))
	{

	  offset = getSlotOffset (slotNo);
	  Map map = new Map(data, offset, mapLen);
	  return map;
	}

      else {
        throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
      }

    }

  /**
   * returns the amount of available space on the page.
   * @return  the amount of available space on the page
   * @exception  IOException I/O errors
   */
  public int available_space()
    throws IOException
    {
      freeSpace = Convert.getShortValue (FREE_SPACE, data);
      return (freeSpace - SIZE_OF_SLOT);
    }

  /**
   * Determining if the page is empty
   * @return true if the HFPageBigDB is has no maps in it, false otherwise
   * @exception  IOException I/O errors
   */
  public boolean empty()
    throws IOException
    {
      int i;
      short length;
      // look for an empty slot
      slotCnt = Convert.getShortValue (SLOT_CNT, data);

      for (i= 0; i < slotCnt; i++)
	{
	  length = getSlotLength(i);
	  if (length != EMPTY_SLOT)
	    return false;
	}

      return true;
    }

  /**
   * Compacts the slot directory on an HFPageBigDB.
   * WARNING -- this will probably lead to a change in the MIDs of
   * maps on the page.  You CAN'T DO THIS on most kinds of pages.
   * @exception  IOException I/O errors
   */
  protected void compact_slot_dir()
    throws IOException
    {
      int  current_scan_posn = 0;   // current scan position
      int  first_free_slot   = -1;   // An invalid position.
      boolean move = false;          // Move a map? -- initially false
      short length;
      short offset;

      slotCnt = Convert.getShortValue (SLOT_CNT, data);
      freeSpace = Convert.getShortValue (FREE_SPACE, data);

      while (current_scan_posn < slotCnt)
	{
	  length = getSlotLength (current_scan_posn);

	  if ((length == EMPTY_SLOT) && (move == false))
	    {
	      move = true;
	      first_free_slot = current_scan_posn;
	    }
	  else if ((length != EMPTY_SLOT) && (move == true))
	    {
	      offset = getSlotOffset (current_scan_posn);

	      // slot[first_free_slot].length = slot[current_scan_posn].length;
	      // slot[first_free_slot].offset = slot[current_scan_posn].offset;
	      setSlot ( first_free_slot, length, offset);

	      // Mark the current_scan_posn as empty
	      //  slot[current_scan_posn].length = EMPTY_SLOT;
	      setSlot (current_scan_posn, EMPTY_SLOT, 0);

	      // Now make the first_free_slot point to the next free slot.
	      first_free_slot++;

	      // slot[current_scan_posn].length == EMPTY_SLOT !!
	      while (getSlotLength (first_free_slot) != EMPTY_SLOT)
		{
		  first_free_slot++;
		}
	    }

	  current_scan_posn++;
	}

      if (move == true)
	{
	  // Adjust amount of free space on page and slotCnt
	  freeSpace += SIZE_OF_SLOT * (slotCnt - first_free_slot);
	  slotCnt = (short) first_free_slot;
	  Convert.setShortValue (freeSpace, FREE_SPACE, data);
	  Convert.setShortValue (slotCnt, SLOT_CNT, data);
	}
    }

}
