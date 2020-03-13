/*
 * @(#) BTIndexPage.java   98/05/14
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu)
 *
 */
package btree;

import java.io.*;
import global.*;

/**
 * BTFileScan implements a search/iterate interface to B+ tree index files
 * (class BTreeFile). It derives from abstract base class IndexFileScan.
 */
public class BTFileScan extends IndexFileScan implements GlobalConst {

  BTreeFile bfile;
  String treeFilename; // B+ tree we're scanning
  BTLeafPage leafPage; // leaf page containing current record
  MID curMid; // position in current leaf; note: this is
              // the MID of the key/MID pair within the
              // leaf page.
  boolean didfirst; // false only before getNext is called
  boolean deletedcurrent; // true after deleteCurrent is called (read
                          // by get_next, written by deleteCurrent).

  KeyClass endkey; // if NULL, then go all the way right
                   // else, stop when current record > this value.
                   // (that is, implement an inclusive range
                   // scan -- the only way to do a search for
                   // a single value).
  int keyType;
  int maxKeysize;

  /**
   * Iterate once (during a scan).
   * 
   * @return null if done; otherwise next KeyDataEntry
   * @exception ScanIteratorException iterator error
   */
  public KeyDataEntry get_next() throws ScanIteratorException {

    KeyDataEntry entry;
    PageId nextpage;
    try {
      if (leafPage == null)
        return null;

      if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
        didfirst = true;
        deletedcurrent = false;
        entry = leafPage.getCurrent(curMid);
      } else {
        entry = leafPage.getNext(curMid);
      }

      while (entry == null) {
        nextpage = leafPage.getNextPage();
        SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
        if (nextpage.pid == INVALID_PAGE) {
          leafPage = null;
          return null;
        }

        leafPage = new BTLeafPage(nextpage, keyType);

        entry = leafPage.getFirst(curMid);
      }

      if (endkey != null)
        if (BT.keyCompare(entry.key, endkey) > 0) {
          // went past right end of scan
          SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
          leafPage = null;
          return null;
        }

      return entry;
    } catch (Exception e) {
      e.printStackTrace();
      throw new ScanIteratorException();
    }
  }

  /**
   * Delete currently-being-scanned(i.e., just scanned) data entry.
   * 
   * @exception ScanDeleteException delete error when scan
   */
  public void delete_current() throws ScanDeleteException {

    KeyDataEntry entry;
    try {
      if (leafPage == null) {
        System.out.println("No Record to delete!");
        throw new ScanDeleteException();
      }

      if ((deletedcurrent == true) || (didfirst == false))
        return;

      entry = leafPage.getCurrent(curMid);
      SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
      bfile.Delete(entry.key, ((LeafData) entry.data).getData());
      leafPage = bfile.findRunStart(entry.key, curMid);

      deletedcurrent = true;
      return;
    } catch (Exception e) {
      e.printStackTrace();
      throw new ScanDeleteException();
    }
  }

  /**
   * max size of the key
   * 
   * @return the maxumum size of the key in BTFile
   */
  public int keysize() {
    return maxKeysize;
  }

  /**
   * destructor. unpin some pages if they are not unpinned already. and do some
   * clearing work.
   * 
   * @exception IOException                        error from the lower layer
   * @exception bufmgr.InvalidFrameNumberException error from the lower layer
   * @exception bufmgr.ReplacerException           error from the lower layer
   * @exception bufmgr.PageUnpinnedException       error from the lower layer
   * @exception bufmgr.HashEntryNotFoundException  error from the lower layer
   */
  public void DestroyBTreeFileScan() throws IOException, bufmgr.InvalidFrameNumberException, bufmgr.ReplacerException,
      bufmgr.PageUnpinnedException, bufmgr.HashEntryNotFoundException {
    if (leafPage != null) {
      SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
    }
    leafPage = null;
  }

}
