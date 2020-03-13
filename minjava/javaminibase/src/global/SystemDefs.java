package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public class SystemDefs {
  public static BufMgr	JavabaseBM;
  public static BigDB	JavabaseBigDB;
  public static Catalog	JavabaseCatalog;
  
  public static String  JavabaseBigDBName;
  public static String  JavabaseLogName;
  public static boolean MINIBASE_RESTART_FLAG = false;
  public static String	MINIBASE_BigDBNAME;
  
  public SystemDefs (){};
  
  public SystemDefs(String dbname, int num_pgs, int bufpoolsize,
		    String replacement_policy )
    {
      int logsize;
      
      String real_logname = new String(dbname);
      String real_dbname = new String(dbname);
      
      if (num_pgs == 0) {
	logsize = 500;
      }
      else {
	logsize = 3*num_pgs;
      }
      
      if (replacement_policy == null) {
	replacement_policy = new String("Clock");
      }
      
      init(real_dbname,real_logname, num_pgs, logsize,
	   bufpoolsize, replacement_policy);
    }
  
  
  public void init( String dbname, String logname,
		    int num_pgs, int maxlogsize,
		    int bufpoolsize, String replacement_policy )
    {
      
      boolean status = true;
      JavabaseBM = null;
      JavabaseBigDB = null;
      JavabaseBigDBName = null;
      JavabaseLogName = null;
      JavabaseCatalog = null;
      
      try {
	JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
	JavabaseBigDB = new BigDB();
/*
	JavabaseCatalog = new Catalog(); 
*/
      }
      catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }
      
      JavabaseBigDBName = new String(dbname);
      JavabaseLogName = new String(logname);
      MINIBASE_BigDBNAME = new String(JavabaseBigDBName);
      
      // create or open the DB

      if ((MINIBASE_RESTART_FLAG)||(num_pgs == 0)){//open an existing database
	try {
	  JavabaseBigDB.openDB(dbname);
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      } 
      else {
	try {
	  JavabaseBigDB.openDB(dbname, num_pgs);
	  JavabaseBM.flushAllPages();
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      }
    }
}
