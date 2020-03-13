package programs;

import btree.*;
import global.*;
import BigT.*;
import heap.Tuple;

import java.io.*;
//import global.*;

public class batchInsert{
    public static void main(String[] args){
        String filepath = "./";
        String datafilename = args[0];
        int type = Integer.parseInt(args[1]);
        String bigTableName = args[2];
        //TODO System Defs
        //SystemDefsBigDB sysDefs = new SystemDefsBigDB();
        BufferedReader br = null;
        String line = "";
        String csvSplitBy = ",";
        try{
            br = new BufferedReader(new FileReader(datafilename));
            while((line = br.readLine()) != null){
                String[] map = line.split(csvSplitBy);
                String rowLabel = map[0];
                String columnLabel = map[1];
                String timeStamp = map[2];
                String value = map[3];
                System.out.println("row label: "+ rowLabel + " col label: " + columnLabel + " TS: " + timeStamp  + " val: " + value);

                //TODO Map
                Map m = new Map();
                //TODO insert into big table

                //TODO take big table, scan it and index based on type

                // TODO
                //At this point we're going to make indexes for our
                //maps. (The indexes should probabally be inside of the bigT constructor
                // but for now I am putting it here)
                // when we create an index we need to give it a name to find it
                // I think we should use the following names
                // Index File Nmes
                // type 1 - no index
                // type 2 - type2Idx
                // type 3 - type3Idx
                // type 4 - type4CombKeyIdx & type4TSIdx
                // type 5 - type5CombKeyIdx & type5TSIdx

                // putting this here for now
                bigt big = new bigt(bigTableName, type);
                BTreeFile btf = null;
                switch (type){
                    case DBType.type1:
                        //TODO need to research this one a little more
                        //no index
                        break;
                    case DBType.type2:
                        //one btree to index row labels
                        try {
                            btf = new BTreeFile("type2Idx", AttrType.attrString, big.getRowCnt(), 1/*delete*/);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            Runtime.getRuntime().exit(1);
                        }

                        mid = new MID();
                        String key = null;
                        Map temp = null;

                        try {
                            temp = Stream.getNext(mid);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                        // itterate through all the maps
                        while ( temp != null) {
                            m.mapCopy(temp);

                            try {
                                // the key for Type 2 is the row
                                key = m.getRowLabel();
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }

                            try {
                                // insert the key and the 'pointer' Map Id into btree index
                                btf.insert(new StringKey(key), mid);
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }

                            try {
                                // get next map
                                temp = Stream.getNext(mid);
                            }
                            catch (Exception e) {
                                e.printStackTrace();
                            }
                        }

                        // close the file scan
                        scan.closescan();

                        //BTreeIndex file created successfully

                        break;
                    case DBType.type3:
                        // one btree to index column labels
                        break;
                    case DBType.type4:
                        // one btree to index column label and row label (combined key)
                        // one btree to index time stamps
                        break;
                    case DBType.type5:
                        // one btree to index row label and value (combined key)
                        // one btree to index time stamps
                        break;
                    default:

                }
            }


            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
