package BigT;

import java.io.*;
import bufmgr.*;
import global.*;

import java.util.*;
import java.lang.*;
import diskmgr.*;


public class bigt implements GlobalConst {


//create system def object
  public void bigt(String name, int type)
  {
    //dbpath, num of pages,buffer pool,replacement policy
    //convert int to string
    dbpath = "/tmp/"+nameRoot+System.getProperty("user.name")+"BIGTABLENAME." + type + "-db";
    logpath = "/tmp/"+nameRoot +System.getProperty("user.name")+"BIGTABLENAME." + type + "-log";
    SystemDefsBT sysdef = new SystemDefsBT( dbpath, 20,  100, "Clock" , type);


  }
  public void deleteBigt()
  {
    SystemDefsBT.JavabaseDB.destroyBigDB();

  }
  public int getMapCnt()
  {
    //the number of cells in one row for all columns
  }
  public int getRowCnt()
  {
    //indivisual label rows in bigTable
  }
  public int getColumnCnt()
  {
    //number of column labels in bigTable
  }




//add main method

}
