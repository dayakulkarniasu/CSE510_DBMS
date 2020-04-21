package programs;

import btree.*;
import global.*;
import BigT.*;
import heap.*;
import iterator.RowJoin;

import java.util.Scanner;

import java.io.*;

public class InsertTest implements GlobalConst {
    public static void main(String[] args) throws Exception {
        boolean first = true;
        String dbpath;
        String logpath;

        SystemDefs sysdef = null;
        int recleng2 = MAP_LEN;
        boolean found_delete_flag = true;

        Scanner scanner = new Scanner(System.in);
        try {
            while (true) {
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                System.out.println("+");
                System.out.println("+\tCommands");
                System.out.println("+");
                System.out.println("+\tbatchinsert");
                System.out.println("+");
                System.out.println("+\tbatchinsert [DataFile] [OrderType] [BTName] [NumBuf]");
                System.out.println("+");
                System.out.println("+\tquery");
                System.out.println("+");
                System.out.println("+\tquery [BTName] [OrderBy] [RF] [CF] [VF] [NumBuf]");
                System.out.println("+");
                System.out.println("+\tmapinsert");
                System.out.println("+");
                System.out.println("+\tmapinsert [RL] [CL] [V] [TS] [Type] [BTName] [NumBuf]");
                System.out.println("+");
                System.out.println("+\tgetCounts");
                System.out.println("+");
                System.out.println("+\trowjoin");
                System.out.println("+\trowjoin [BT1] [BT2] [BTResult] [CF] [NumBuf]");
                System.out.println("+");
                System.out.println("+\tquit");
                System.out.println("+");
                System.out.println("+");
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                String line = scanner.nextLine();
                String[] params = line.split(" ");
                String fname = "";
                int dbtype;
                String tablename = "";
                int NumBuf = 0;
                String input_RL = "";
                String input_CL = "";
                String input_VL = "";
                int input_TS = 0, input_dbtype = 999;
                if (params[0].equalsIgnoreCase("batchinsert")) {
                    if (params.length == 3) {
                        fname = params[1];
                        dbtype = 999;
                        tablename = params[2];
                        NumBuf = Integer.parseInt(params[3]);
                    } else {
                        fname = params[1];
                        dbtype = Integer.parseInt(params[2]);
                        tablename = params[3];
                        NumBuf = Integer.parseInt(params[4]);
                    }
                    System.out.println("Input File name in Inserttest : " + fname + " IndexType : " + dbtype);
                    if (first) {
                        String[] strArr = fname.split("/");
                        String str = strArr[strArr.length - 1];
                        String _fname = str.substring(0, str.length() - 5);
                        dbpath = "/tmp/" + System.getProperty("user.name") + "_" + _fname + ".db";
                        logpath = "/tmp/" + System.getProperty("user.name") + "_" + tablename + ".log";
                        System.out.println("dbpath in Inserttest : " + dbpath);
                        sysdef = new SystemDefs(dbpath, dbtype, 10500, 100, "Clock");

                        // for (bigt _bigt : SystemDefs.JavabaseDB.table)
                        // System.out.println(_bigt.heapFileName);

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
                    batchInsert.insertTable(fname, tablename, dbtype, NumBuf);
                    System.out.println("NumberOfTables: " + SystemDefs.JavabaseDB.NumberOfTables);
                } else if (params[0].equalsIgnoreCase("query")) {
                    String btname = params[1];
                    int ordertype = Integer.parseInt(params[2]);
                    String rowfilter = params[3];
                    String colfilter = params[4];
                    String valfilter = params[5];
                    int numbuf = Integer.parseInt(params[6]);

                    Query query = new Query(btname, ordertype, rowfilter, colfilter, valfilter, numbuf);
                    query.runquery();
                } else if (params[0].equalsIgnoreCase("getCounts")) {
                    bigt big = null;
                    int i = 0;
                    for (i = 0; i < SystemDefs.JavabaseDB.NumberOfTables; i++) {
                        big = SystemDefs.JavabaseDB.table[i];
                        if (big == null) {
                            // big = new bigt(tablename, SystemDefs.JavabaseDB.dbType);
                            System.out.println("JavabaseDB name is Null : " + SystemDefs.JavabaseDB.table[i].name);
                            System.out.println("Exiting...... ");
                        } else {
                            big = SystemDefs.JavabaseDB.table[i];
                            System.out.println("Table Name : " + SystemDefs.JavabaseDB.table[i].name
                                    + " HeapFileName : " + SystemDefs.JavabaseDB.table[i].heapFileName
                                    + " Index Type : " + SystemDefs.JavabaseDB.table[i].BTType);
                            System.out.println("********** ");
                            System.out.println("No. Of Maps : " + big.getMapCnt());
                            System.out.println("No. Of Distinct RowLabels : " + big.getRowCnt());
                            System.out.println("No. Of Distinct ColumnLabels : " + big.getColumnCnt());
                            System.out.println("********** ");
                        }
                    }
                } else if (params[0].equalsIgnoreCase("mapinsert")) {
                    System.out.println("No OF parameters : " + params.length);
                    if (params.length == 7) {
                        input_RL = params[1];
                        input_CL = params[2];
                        input_VL = params[3];
                        input_TS = Integer.parseInt(params[4]);
                        input_dbtype = Integer.parseInt(params[5]);
                        tablename = params[6];
                    } else if (params.length == 6) {
                        input_RL = params[1];
                        input_CL = params[2];
                        input_VL = params[3];
                        input_TS = Integer.parseInt(params[4]);
                        input_dbtype = 999;
                        tablename = params[5];
                    } else {
                        System.out.println("Invalid number of Arguments.... ");
                        return;
                    }
                    if (first) {
                        dbpath = "/tmp/" + System.getProperty("user.name") + fname + ".db";
                        logpath = "/tmp/" + System.getProperty("user.name") + tablename + ".log";
                        System.out.println("dbpath in Inserttest : " + dbpath);
                        sysdef = new SystemDefs(dbpath, input_dbtype, 10500, 100, "Clock");
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
                    bigt big = null;
                    MID rid = null;
                    if (input_dbtype == 999) {
                        new bigt(tablename, 1);
                        big = SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex];
                    } else {
                        new bigt(tablename, input_dbtype);
                        big = SystemDefs.JavabaseDB.table[SystemDefs.JavabaseDB.CurrentTableIndex];
                    }
                    DummyRecord rec = new DummyRecord(recleng2);
                    rec.rowlabname = input_RL;
                    rec.collabname = input_CL;
                    rec.timestampname = input_TS;
                    rec.valuename = input_VL;
                    try {
                        Map recMap = new Map(rec.toByteArray(), 0, rec.getRecLength());
                        AttrType[] types = new AttrType[4];
                        types[0] = new AttrType(AttrType.attrString);
                        types[1] = new AttrType(AttrType.attrString);
                        types[2] = new AttrType(AttrType.attrString);
                        types[3] = new AttrType(AttrType.attrInteger);
                        short[] strSizes = new short[3];
                        strSizes[0] = (short) (STR_LEN);
                        strSizes[1] = (short) (STR_LEN);
                        strSizes[2] = (short) (STR_LEN);
                        recMap.setHdr((short) 4, types, strSizes);
                        found_delete_flag = batchInsert.found_delete(big.hf, input_RL, input_CL, input_TS, input_VL);
                        if (found_delete_flag /* found delete flag is false */ ) {
                            rid = big.insertMap(recMap.getMapByteArray());
                        }
                    } catch (Exception e) {
                        // status = false;
                        System.err.println("*** Error inserting Map ***  \n");
                        e.printStackTrace();
                    }
                    if (rid == null) {
                        System.out.println("Could not insert the Map.....");
                    }
                    /*
                     * batchInsert.insertTable(fname, tablename);
                     */
                } else if (params[0].equalsIgnoreCase("rowjoin")) {
                    String BTName1 = params[1];
                    String BTName2 = params[2];
                    String BTResult = params[3];
                    String ColumnFilter = params[4];
                    NumBuf = Integer.parseInt(params[5]);
                    bigt big = null;
                    for (int i = 0; i < SystemDefs.JavabaseDB.NumberOfTables; i++) {
                        if (BTName1.equals(SystemDefs.JavabaseDB.table[i].name)) {
                            big = SystemDefs.JavabaseDB.table[i];
                        }
                    }
                    Stream s = new Stream(big, 1, "*", ColumnFilter, "*", NumBuf);
                    RowJoin rj = new RowJoin(NumBuf, s, BTName2, ColumnFilter);
                    new bigt(rj, BTResult);
                    rj.close();
                } else {
                    if (params[0].equalsIgnoreCase("q") || params[0].equalsIgnoreCase("quit")) {
                        System.out.println("exiting");
                        scanner.close();
                    }
                }
            }
        } catch (IllegalStateException e) {
            System.out.println("exiting");
        }
    }// end of main
}