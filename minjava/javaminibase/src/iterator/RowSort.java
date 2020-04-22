package iterator;

import java.io.*;
import java.text.Format;

import BigT.*;
import global.*;
import heap.*;

public class RowSort {
    private Stream inStream;
    MapOrder sort_order;
    String columnName;
    int n_pages;
    
    public RowSort()
    {}

    public RowSort(Stream stream, MapOrder order, String colName, int np)
    {
        inStream = stream;
        sort_order = new MapOrder(order.mapOrder);
        columnName = colName;
        n_pages = np;

        Heapfile hf1 = null;
        Heapfile hf2 = null;
        try
        {
            hf1 = new Heapfile("rowsort-temp1");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            hf2 = new Heapfile("rowsort-temp2");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Map temp = new Map();
        MID mid = new MID();

        // insert maps to two heapfiles
        while((temp = stream.getNext()) != null)
        {
            String colLabel = null;
            try
            {
                colLabel = temp.getColumnLabel();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            if(!colLabel.equals(colName))
            {
                try
                {
                    mid = hf1.insertMap(temp.getMapByteArray());
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                try
                {
                    mid = hf2.insertMap(temp.getMapByteArray());
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

        AttrType[] attrType = MapSchema.MapAttrType();
        short[] attrSize = MapSchema.MapStrLengths();
        MapOrder sort_order = new MapOrder(MapOrder.Ascending);

        // sort the first heapfile by rowlabel, insert into heapfile rowsort_sorted1
        FileScan fscan = null;
        FldSpec[] schema = MapSchema.OutputMapSchema();

        try
        {
            fscan = new FileScan(hf1, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Sort sort = null;
        try
        {
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 1, sort_order, GlobalConst.STR_LEN, n_pages);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Heapfile hf_sorted1 = null;

        try
        {
            hf_sorted1 = new Heapfile("rowsort_sorted1");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            temp = sort.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        while(temp != null)
        {
            try
            {
                hf_sorted1.insertMap(temp.getMapByteArray());
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            try
            {
                temp = sort.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        // close heapfile
        try
        {
            sort.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // delete hf1
        try
        {
            hf1.deleteFile();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // sort hf2 by rowlabel
        try
        {
            fscan = new FileScan(hf2, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        sort = null;
        try
        {
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 1, sort_order, GlobalConst.STR_LEN, n_pages);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Heapfile hf_sorted2 = null;

        try
        {
            hf_sorted2 = new Heapfile("rowsort_sorted2");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            temp = sort.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        while(temp != null)
        {
            try
            {
                hf_sorted2.insertMap(temp.getMapByteArray());
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            try
            {
                temp = sort.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        // close heapfile
        try
        {
            sort.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // scan hf_sorted2, the row label instances with most recent values to hf_idx
        Heapfile hf_idx = null;

        try
        {
            hf_idx = new Heapfile("hf_idx");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        int max_ts = Integer.MIN_VALUE;
        Map curMap = new Map();
        String rowLabel = "";
        String curRowLabel = "";

        try
        {
            fscan = new FileScan(hf_idx, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        try
        {
            temp = fscan.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        while(temp != null)
        {
            try
            {
                curRowLabel = temp.getRowLabel();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            // beginning of a rowlabel, insert curmap into hf_idx.
            // assume rowlabel empty only at the first map
            if(curRowLabel != rowLabel)
            {
                if(!rowLabel.isEmpty())
                {
                    try
                    {
                        hf_idx.insertMap(curMap.getMapByteArray());
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
                rowLabel = curRowLabel;
                curMap = temp;
                try
                {
                    max_ts = curMap.getTimeStamp();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            else    // compare timestamps
            {
                try
                {
                    if(max_ts < temp.getTimeStamp())
                    {
                        max_ts = temp.getTimeStamp();
                        curMap = temp;
                    }
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }
        try
        {
            fscan.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        
    }

    public Map getNext()
    {
        return new Map();
    }
}