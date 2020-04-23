package iterator;

import java.io.*;
import java.text.Format;

import BigT.*;
import global.*;
import heap.*;

public class RowSort {
    private Stream inStream;
    private Heapfile _hf1;
    private Heapfile _hf2;
    private boolean hf1_empty = false;
    private FileScan _fscan;
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
                    mid = hf1.insertMap(temp.getMapByteSingleArray());
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
                    mid = hf2.insertMap(temp.getMapByteSingleArray());
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

        stream.close();

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
                mid = hf_sorted1.insertMap(temp.getMapByteSingleArray());
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
            fscan.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        _hf1 = hf_sorted1;

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
                mid = hf_sorted2.insertMap(temp.getMapByteSingleArray());
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
            fscan.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // scan hf_sorted2, the row label instances with most recent values to hf_idx
        Heapfile hf_idx = null;

        try
        {
            hf_idx = new Heapfile("rowsort_hf_idx");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Map curMap = null;
        String rowLabel = "";
        String curRowLabel = "";

        try
        {
            fscan = new FileScan(hf_sorted2, attrType, attrSize, (short) 4, 4, schema, null);
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
            if(!curRowLabel.equals(rowLabel))
            {
                System.out.println(rowLabel.isEmpty());
                if(!rowLabel.isEmpty())
                {
                    try
                    {
                        mid = hf_idx.insertMap(curMap.getMapByteSingleArray());
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
                rowLabel = curRowLabel;
                curMap = new Map(temp.getMapByteSingleArray(), 0, GlobalConst.MAP_LEN);
            }
            else    // compare timestamps
            {
                try
                {
                    if(curMap.getTimeStamp() <= temp.getTimeStamp())
                    {
                        curMap = new Map(temp.getMapByteSingleArray(), 0, GlobalConst.MAP_LEN);
                    }
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            try
            {
                temp = fscan.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        try
        {
            mid = hf_idx.insertMap(curMap.getMapByteSingleArray());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        fscan.close();
        
        Heapfile hf_merged2 = null;
        FileScan fscan2 = null;

        try
        {
            fscan = new FileScan(hf_idx, attrType, attrSize, (short) 4, 4, schema, null);
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 3, sort_order, GlobalConst.STR_LEN, n_pages);
            // temp = sort.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        try
        {
            fscan2 = new FileScan(hf_sorted2, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            hf_merged2 = new Heapfile("rowsort_merged2");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            curMap = sort.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        while(curMap != null)
        {
            try
            {
                while((temp = fscan2.get_next()) != null)
                {
                    if(temp.getRowLabel().equals(curMap.getRowLabel()))
                    {
                        System.out.println(temp.getRowLabel());
                        hf_merged2.insertMap(temp.getMapByteSingleArray());
                    }
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            fscan2.close();
            try
            {
                fscan2 = new FileScan(hf_sorted2, attrType, attrSize, (short) 4, 4, schema, null);
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            try
            {
                curMap = sort.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        fscan.close();

        // merge files
        Heapfile hf_out = null;
        try
        {
            hf_out = new Heapfile("rowsort_output");
            fscan = new FileScan(hf_sorted1, attrType, attrSize, (short) 4, 4, schema, null);
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
                hf_out.insertMap(temp.getMapByteSingleArray());
                temp = fscan.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        try
        {
            fscan = new FileScan(hf_merged2, attrType, attrSize, (short) 4, 4, schema, null);
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
                hf_out.insertMap(temp.getMapByteSingleArray());
                temp = fscan.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        fscan.close();

        try
        {
            hf_sorted1.deleteFile();
            hf_merged2.deleteFile();
            _fscan = new FileScan(hf_out, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    public Map getNext()
    {
        Map amap = new Map();
        try
        {
            amap = _fscan.get_next();
            // System.out.println(amap.getColumnLabel());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return amap;
    }

    public void close()
    {
        _fscan.close();

    }
}