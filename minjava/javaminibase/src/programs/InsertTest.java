package programs;

import btree.*;
import global.*;
import BigT.*;
import heap.*;
import java.util.Scanner;

import java.io.*;

public class InsertTest implements GlobalConst{
    public static void main(String[] args) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
        boolean first = true;
        String filepath = "./";
        // String datafilename = args[0];
        // int type = Integer.parseInt(args[1]);
        // String bigTableName = args[2];

        String dbpath;
        String logpath;
    
        // dbpath = "/tmp/" + System.getProperty("user.name") + bigTableName + ".db";
        // logpath = "/tmp/" + System.getProperty("user.name") + bigTableName + ".log";
    
    
        // SystemDefs sysdef = new SystemDefs(dbpath, type, 8193, 100, "Clock");
        
        // String newdbpath;
        // String newlogpath;
        // String remove_logcmd;
        // String remove_dbcmd;
        // String remove_cmd = "/bin/rm -rf ";
    
        // newdbpath = dbpath;
        // newlogpath = logpath;
    
        // remove_logcmd = remove_cmd + logpath;
        // remove_dbcmd = remove_cmd + dbpath;
    
        // Commands here is very machine dependent. We assume
        // user are on UNIX system here
        // try {
        //   Runtime.getRuntime().exec(remove_logcmd);
        //   Runtime.getRuntime().exec(remove_dbcmd);
        // } catch (IOException e) {
        //   System.err.println("IO error: " + e);
        // }


        Scanner scanner = new Scanner(System.in);
        try{
            while(true){
                System.out.println("Input filename, type and bigtable name");
                String line = scanner.nextLine();
                first = false;
                String[] params = line.split(" ");
                if(params[0].equalsIgnoreCase("batchinsert"))
                {
                    String fname = params[1];
                    int dbtype = Integer.parseInt(params[2]);
                    String tablename = params[3];

                    if(first){
                        dbpath = "/tmp/" + System.getProperty("user.name") + fname + ".db";
                        logpath = "/tmp/" + System.getProperty("user.name") + tablename + ".log";
                        SystemDefs sysdef = new SystemDefs(dbpath, dbtype, 8193, 100, "Clock");

                        String newdbpath;
                        String newlogpath;
                        String remove_logcmd;
                        String remove_dbcmd;
                        String remove_cmd = "/bin/rm -rf ";
                    
                        newdbpath = dbpath;
                        newlogpath = logpath;
                    
                        remove_logcmd = remove_cmd + logpath;
                        remove_dbcmd = remove_cmd + dbpath;

                        try {
                            Runtime.getRuntime().exec(remove_logcmd);
                            Runtime.getRuntime().exec(remove_dbcmd);
                        } catch (IOException e) {
                            System.err.println("IO error: " + e);
                        }
                        first = false;
                    }
                    batchInsert.insertTable(fname);
                }
                else if(params[0].equalsIgnoreCase("query"))
                {
                    String btname = params[1];
                    int dbtype = Integer.parseInt(params[2]);
                    int ordertype = Integer.parseInt(params[3]);
                    String rowfilter = params[4];
                    String colfilter = params[5];
                    String valfilter = params[6];
                    int numbuf = Integer.parseInt(params[7]);
                }
                else{
                    if(line.equals("q")){
                        System.out.println("exiting");
                        scanner.close();
                    }
                    else{
                        batchInsert.insertTable(line);
                    }
                }
            }
        } catch(IllegalStateException e)
        {
            System.out.println("exiting");
        }


        

        // batchInsert.insertTable(datafilename + ".");

    
      }// end of main
}