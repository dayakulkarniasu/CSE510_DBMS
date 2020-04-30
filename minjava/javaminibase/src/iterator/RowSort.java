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
        boolean written = false;

        // insert maps to two heapfiles
        while((temp = stream.getNext()) != null)
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
                written = false;
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
            
            if(colLabel.equals(colName) && !written)
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
                written = true;
            }
        }

        // stream.close();

        Heapfile hf3 = null;
        Heapfile hf4 = null;
        
        try
        {
            hf3 = new Heapfile("rowsort_other");
            hf4 = new Heapfile("rowsort_match");
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        AttrType[] attrType = MapSchema.MapAttrType();
        short[] attrSize = MapSchema.MapStrLengths();
        MapOrder sort_order = new MapOrder(MapOrder.Ascending);

        // sort the first heapfile by rowlabel, insert into heapfile rowsort_sorted1
        FileScan fscan1 = null;
        FileScan fscan2 = null;
        FldSpec[] schema = MapSchema.OutputMapSchema();

        try
        {
            fscan1 = new FileScan(hf1, attrType, attrSize, (short) 4, 4, schema, null);
            fscan2 = new FileScan(hf2, attrType, attrSize, (short) 4, 4, schema, null);
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
            fscan1 = new FileScan(hf2, attrType, attrSize, (short) 4, 4, schema, null);
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
                // mid = hf5.insertMap(temp.getMapByteSingleArray());
                clusterRowLabel = temp.getRowLabel();
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

        try
        {
            _fscan = new FileScan(hf5, attrType, attrSize, (short) 4, 4, schema, null);
        }
        catch(Exception e)
        {
            e.printStackTrace();
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
        FileScan fscan = null;
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
            // _fscan = new FileScan(hf_out, attrType, attrSize, (short) 4, 4, schema, null);
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