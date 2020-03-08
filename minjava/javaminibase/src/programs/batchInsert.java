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

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}