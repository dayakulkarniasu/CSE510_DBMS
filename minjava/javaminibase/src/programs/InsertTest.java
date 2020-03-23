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
        String filepath = "./";
        // String datafilename = args[0];
        // int type = Integer.parseInt(args[1]);
        // String bigTableName = args[2];

        String dbpath;
        String logpath;
    
        // dbpath = "/tmp/" + System.getProperty("user.name") + bigTableName + ".db";
        // logpath = "/tmp/" + System.getProperty("user.name") + bigTableName + ".log";
    
        SystemDefs sysdef = null;
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
            //String pattern = "\\*|(\\[.+\\])|\\w*";
            while(true){
                System.out.println("Input batchinsert or query");
                String line = scanner.nextLine();
                String[] params = line.split(" ");
                //Matcher m = Pattern.compile(pattern).matcher(line);
                //List<String> params = New ArrayList<>();
//                while(m.find()){
//                    params.add(m.group(0));
//                }
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


        

        // batchInsert.insertTable(datafilename + ".");

    
      }// end of main
}