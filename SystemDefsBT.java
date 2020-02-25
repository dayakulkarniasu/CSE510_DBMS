package global;

import bufmgr.*;
import diskmgr.*;
import catalog.*;

public class SystemDefsBT {
  public static BufMgr	JavabaseBM;
  public static DB	JavabaseDB;

  /*********
  public static BigTClass	JavabaseBIGDB;
  *********/



  public static Catalog	JavabaseCatalog;

  public static String  JavabaseDBName;
  public static String  JavabaseLogName;
  public static boolean MINIBASE_RESTART_FLAG = false;
  public static String	MINIBASE_DBNAME;

  public SystemDefsBT (){};

  public SystemDefsBT(String dbname, int num_pgs, int bufpoolsize,
		    String replacement_policy , int type)
    {
      int logsize;

      String real_logname = new String(dbname)+ "-log";
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
	   bufpoolsize, replacement_policy, type);
    }


  public void init( String dbname, String logname,
		    int num_pgs, int maxlogsize,
		    int bufpoolsize, String replacement_policy , int type)
    {

      boolean status = true;
      JavabaseBM = null;
      JavabaseBIGDB= null;
      //JavabaseDB = null;
      JavabaseBIGDBName = null;
      JavabaseBIGLogName = null;
      JavabaseBIGCatalog = null;

      try {
	JavabaseBM = new BufMgr(bufpoolsize, replacement_policy);
	//JavabaseDB = new DB();

  JavabaseBIGDB = new bigDB();
/*
	JavabaseCatalog = new Catalog();
*/
      }
      catch (Exception e) {
	System.err.println (""+e);
	e.printStackTrace();
	Runtime.getRuntime().exit(1);
      }

      JavabaseBIGDBName = new String(dbname);


      JavabaseBIGLogName = new String(logname);
      MINIBASE_DBNAME = new String(JavabaseBIGDBName);

      // create or open the DB

      if ((MINIBASE_RESTART_FLAG)||(num_pgs == 0)){//open an existing database
	try {
	  JavabaseBIGDB.openbigDB(dbname);
	}
	catch (Exception e) {
	  System.err.println (""+e);
	  e.printStackTrace();
	  Runtime.getRuntime().exit(1);
	}
      }
      else {
	try {
	  JavabaseDB.openbigDB(dbname, num_pgs, type );
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
