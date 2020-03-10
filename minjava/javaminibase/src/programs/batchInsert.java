package programs;

import java.io.*;
import java.util.StringTokenizer;
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

                //TODO insert into big table

                //TODO take big table, scan it and index based on type

                /*TODO
                * switch(type):
                * case 1:
                * case 2: BTREE RowLabel
                *       BTreeFile btf = null;
                         btf = new BTreeFile("Name of the btree index file", AttrType.attrString, REC_LEN1   , 1/*delete);
                         mid = new MID();
                        String key = null;
                        Tuple temp = null;
                         while ( temp != null) {
                            m.tupleCopy(temp);
	                         key = m.getrowlabel();
      }

                 Case 3: BTREE Column label
                        ...
                 	     key = m.getcolumnlabel();

                 */


            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}