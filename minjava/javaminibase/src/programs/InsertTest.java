package programs;

import btree.*;
import global.*;
import BigT.*;
import heap.*;
import java.util.Scanner;

import java.io.*;

public class InsertTest implements GlobalConst{
    public static void main(String[] args) throws Exception {
        boolean first = true;
        String dbpath;
        String logpath;

        SystemDefs sysdef = null;


        Scanner scanner = new Scanner(System.in);
        try{
            while(true){
                System.out.println("Input batchinsert or query");
                String line = scanner.nextLine();
                String[] params = line.split(" ");
                if(params[0].equalsIgnoreCase("batchinsert"))
                {
                    String fname = params[1];
                    int dbtype = Integer.parseInt(params[2]);
                    String tablename = params[3];

                    if(first){
                        dbpath = "/tmp/" + System.getProperty("user.name") + fname + ".db";
                        logpath = "/tmp/" + System.getProperty("user.name") + tablename + ".log";
                        sysdef = new SystemDefs(dbpath, dbtype, 10500, 100, "Clock");

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
                    batchInsert.insertTable(fname, tablename);
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

                    Query query = new Query(btname, dbtype, ordertype, rowfilter, colfilter, valfilter, numbuf);
                    query.runquery();
                }
                else{
                    if(line.equals("q")){
                        System.out.println("exiting");
                        scanner.close();
                    }
                }
            }
        } catch(IllegalStateException e)
        {
            System.out.println("exiting");
        }
      }// end of main
}