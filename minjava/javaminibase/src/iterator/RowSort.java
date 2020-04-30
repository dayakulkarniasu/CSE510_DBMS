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
            hf2 = new Heapfile("rowsort-temp2");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Map temp = new Map();
        MID mid = new MID();
        String curRowLabel = "";
        String clusterRowLabel = "";

        try
        {
            temp = stream.getNext();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // insert maps to two heapfiles
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

            String colLabel = null;
            try
            {
                colLabel = temp.getColumnLabel();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            try
            {
                mid = hf1.insertMap(temp.getMapByteSingleArray());
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
            
            if(colLabel.equals(colName))
            {
                try
                {
                    clusterRowLabel = curRowLabel;
                    mid = hf2.insertMap(temp.getMapByteSingleArray());
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }

            try
            {
                temp = stream.getNext();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        stream.close();

        Heapfile hf3 = null;
        Heapfile hf4 = null;
        Heapfile hf_idx_temp = null;
        
        try
        {
            hf3 = new Heapfile("rowsort_other");
            hf4 = new Heapfile("rowsort_match");
            hf_idx_temp = new Heapfile("rowsort_idx_temp");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        AttrType[] attrType = MapSchema.MapAttrType();
        short[] attrSize = MapSchema.MapStrLengths();
        MapOrder sort_order = new MapOrder(MapOrder.Ascending);
        FldSpec[] schema = MapSchema.OutputMapSchema();

        FileScan fscan = null;
        FileScan fscan1 = null;
        FileScan fscan2 = null;

        try
        {
            fscan = new FileScan(hf2, attrType, attrSize, (short) 4, 4, schema, null);
            temp = fscan.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        int clusterTs = Integer.MIN_VALUE;
        int curTs = Integer.MIN_VALUE;
        Map ts_map = null;
        curRowLabel = "";
        clusterRowLabel = "";

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

            if(!curRowLabel.equals(clusterRowLabel))
            {
                // If new row, write the previous row.
                if(!clusterRowLabel.isEmpty())
                {
                    try
                    {
                        mid = hf_idx_temp.insertMap(ts_map.getMapByteSingleArray());
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
                clusterRowLabel = curRowLabel;
                ts_map = new Map(temp.getMapByteSingleArray(), 0, GlobalConst.MAP_LEN);
            }
            else    // compare timestamps
            {
                try
                {
                    if(ts_map.getTimeStamp() < temp.getTimeStamp())
                    {
                        ts_map= new Map(temp.getMapByteSingleArray(), 0, GlobalConst.MAP_LEN);
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
            mid = hf_idx_temp.insertMap(ts_map.getMapByteSingleArray());
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
       
        Heapfile hf_idx = null;
        fscan = null;
        Sort sort = null;
        
        try
        {
            hf_idx = new Heapfile("rowsort_hfidx");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        
        try
        {
            fscan = new FileScan(hf_idx_temp, attrType, attrSize, (short) 4, 4, schema, null);
            sort = new Sort(attrType, (short) 4, attrSize, fscan, 3, sort_order, GlobalConst.STR_LEN, n_pages);
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
                mid = hf_idx.insertMap(temp.getMapByteSingleArray());
                temp = sort.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }


        try
        {
            fscan1 = new FileScan(hf1, attrType, attrSize, (short) 4, 4, schema, null);
            fscan2 = new FileScan(hf_idx, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        try
        {
            temp = fscan1.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        Map temp_inner = new Map();
        curRowLabel = "";
        clusterRowLabel = "";
        boolean match = false;
        
        // Scan row-sorted heapfile (outer), and for each rowlabel scan index heapfile hf2.
        // If no match, insert into hf3 (other), otherwise insert into hf4 (match)
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

            // If new row, scan hf2.
            if(!curRowLabel.equals(clusterRowLabel))
            {
                match = false;
                try
                {
                    fscan2 = new FileScan(hf2, attrType, attrSize, (short) 4, 4, schema, null);
                    temp_inner = fscan2.get_next();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
                while(temp_inner != null)
                {
                    try
                    {
                        if(temp_inner.getRowLabel().equals(curRowLabel)) // found match
                        {
                            match = true;
                            break;
                        }
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                    try
                    {
                        temp_inner = fscan2.get_next();
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }

                fscan2.close();
                
                if(match)
                {
                    try
                    {
                        mid = hf4.insertMap(temp.getMapByteSingleArray());
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
                        mid = hf3.insertMap(temp.getMapByteSingleArray());
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
            }
            else if(match)
            {
                try
                {
                    mid = hf4.insertMap(temp.getMapByteSingleArray());
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
                    mid = hf3.insertMap(temp.getMapByteSingleArray());
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }

            clusterRowLabel = curRowLabel;
            try
            {
                temp = fscan1.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        // close heapfile
        try
        {
            fscan1.close();
            // hf1.deleteFile();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // Joining. Outer: hf2, inner: hf4. Output: hf5
        Heapfile hf5 = null;
        try
        {
            hf5 = new Heapfile("rowsort_match_sorted");
            fscan1 = new FileScan(hf_idx, attrType, attrSize, (short) 4, 4, schema, null);
            fscan2 = new FileScan(hf4, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        curRowLabel = "";
        clusterRowLabel = "";

        try
        {
            temp = fscan1.get_next();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        while(temp != null)
        {
            try
            {
                clusterRowLabel = temp.getRowLabel();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            try
            {
                fscan2 = new FileScan(hf4, attrType, attrSize, (short) 4, 4, schema, null);
                temp_inner = fscan2.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }

            match = false;

            while(temp_inner != null)
            {
                try
                {
                    curRowLabel = temp_inner.getRowLabel();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
                
                if(curRowLabel.equals(clusterRowLabel)) // match
                {
                    match = true;
                    try
                    {
                        mid = hf5.insertMap(temp_inner.getMapByteSingleArray());
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                    }
                }
                else if(match)
                {
                    break;
                }
                try
                {
                    temp_inner = fscan2.get_next();
                }
                catch(Exception e)
                {
                    e.printStackTrace();
                }
            }
            try
            {
                temp = fscan1.get_next();
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }

        // close heapfile
        try
        {
            fscan1.close();
            fscan2.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        // merge files
        fscan = null;
        Heapfile hf_out = null;
        try
        {
            hf_out = new Heapfile("rowsort_output");
            fscan = new FileScan(hf3, attrType, attrSize, (short) 4, 4, schema, null);
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
            fscan = new FileScan(hf5, attrType, attrSize, (short) 4, 4, schema, null);
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
            // hf2.deleteFile();
            // hf3.deleteFile();
            // hf4.deleteFile();
            // hf5.deleteFile();
            // hf_idx.deleteFile();
            // hf_idx_temp.deleteFile();
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