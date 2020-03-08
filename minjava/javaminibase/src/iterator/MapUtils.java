package iterator;


import BigT.Map;
import heap.*;
import global.*;
import java.io.*;
import java.lang.*;
import java.lang.reflect.Field;

/**
 *some useful method when processing Tuple
 */

public class MapUtils
{

    /**
     * This function compares a map with another map in respective field, and
     *  returns:
     *
     *    0        if the two are equal,
     *    1        if the tuple is greater,
     *   -1        if the tuple is smaller,
     *
     *@param    m1        one map.
     *@param    m2        another map.
     *@param    map_fld_no the field numbers in the tuples to be compared.
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     *@return   0        if the two are equal,
     *          1        if the tuple is greater,
     *         -1        if the tuple is smaller,
     */
    public static int CompareMapWithMap(Map m1, Map  m2, int map_fld_no)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        int m1_i, m2_i;
        String m1_s, m2_s;

        switch (map_fld_no)
        {
            case 1:                // Compare rows
                try {
                    m1_s = m1.getRowLabel();
                    m2_s = m2.getRowLabel();
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                if(m1_s.compareTo( m2_s)>0)return 1;
                if (m1_s.compareTo( m2_s)<0)return -1;
                return 0;

            case 2:                // Compare columns
                try {
                    m1_s = m1.getColumnLabel();
                    m2_s = m2.getColumnLabel();
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                if(m1_s.compareTo( m2_s)>0)return 1;
                if (m1_s.compareTo( m2_s)<0)return -1;
                return 0;

            case 3:                // Compare TS
                try {
                    m1_i = m1.getTimeStamp();
                    m2_i = m2.getTimeStamp();
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }

                // Now handle the special case that is posed by the max_values for strings...
                if (m1_i == m2_i) return  0;
                if (m1_i <  m2_i) return -1;
                if (m1_i >  m2_i) return  1;
            case 4:
                try {
                    m1_s = m1.getValue();
                    m2_s = m2.getValue();
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }

                // Now handle the special case that is posed by the max_values for strings...
                if(m1_s.compareTo( m2_s)>0)return 1;
                if (m1_s.compareTo( m2_s)<0)return -1;
                return 0;
            default:

                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }
    }



    /**
     * This function  compares  tuple1 with another tuple2 whose
     * field number is same as the tuple1
     *
     *@param    m1        one tuple
     *@param    value     another tuple.
     *@param    map_fld_no the field numbers in the tuples to be compared.
     *@return   0        if the two are equal,
     *          1        if the tuple is greater,
     *         -1        if the tuple is smaller,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static int CompareMapWithValue(Map  m1, int map_fld_no, Map value)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        return CompareMapWithMap(m1, value, map_fld_no);
    }

    /**
     *This function Compares two Tuple inn all fields
     * @param m1 the first map
     * @param m2 the second map
     * @param len the field numbers
     * @return  0        if the two are not equal,
     *          1        if the two are equal,
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */

    public static boolean Equal(Map m1, Map m2, int len)
            throws IOException,UnknowAttrType,MapUtilsException
    {
        int i;

        for (i = 1; i <= len; i++)
            if (CompareMapWithMap(m1, m2, i) != 0)
                return false;
        return true;
    }

    /**
     *get the string specified by the field number
     *@param map the tuple
     *@param map_fld_no the map's field number
     *@return the content of the field number
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static String Value(Map map, int map_fld_no)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {
        String temp;
        switch (map_fld_no){
            case 1:
                try{
                   temp = map.getRowLabel();
               }
               catch (FieldNumberOutOfBoundException e){
                   throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
               }
               break;
            case 2:
               try{
                   temp = map.getColumnLabel();
               }
               catch (FieldNumberOutOfBoundException e){
                   throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
               }
               break;
            case 3:
               try{
                   temp = Integer.toString(map.getTimeStamp());
               }
               catch (FieldNumberOutOfBoundException e){
                   throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
               }
               break;
            case 4:
               try{
                   temp = map.getValue();
               }
               catch (FieldNumberOutOfBoundException e){
                   throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
               }
               break;
            default:
                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");
        }
        return temp;
    }


    /**
     *set up a tuple in specified field from a tuple
     *@param value the map to be set
     *@param map the given map
     *@param map_fld_no the map's field number
     *@exception UnknowAttrType don't know the attribute type
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static void SetValue(Map value, Map  map, int map_fld_no)
            throws IOException,
            UnknowAttrType,
            MapUtilsException
    {

        switch (map_fld_no)
        {
            case 1:
                try {
                    value.setRowLabel(map.getRowLabel());
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            case 2:
                try {
                    value.setColumnLabel(map.getColumnLabel());
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            case 3:
                try {
                    value.setTimeStamp(map.getTimeStamp());
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            case 4:
                try {
                    value.setValue(map.getValue());
                }catch (FieldNumberOutOfBoundException e){
                    throw new MapUtilsException(e, "FieldNumberOutOfBoundException is caught by MapUtils.java");
                }
                break;
            default:
                throw new UnknowAttrType(null, "Don't know how to handle attrSymbol, attrNull");

        }

        return;
    }

    //TODO: Might need to come back to this
    /**
     *set up the Jtuple's attrtype, string size,field number for using join
     *@param Jtuple  reference to an actual tuple  - no memory has been malloced
     *@param res_attrs  attributes type of result tuple
     *@param in1  array of the attributes of the tuple (ok)
     *@param len_in1  num of attributes of in1
     *@param in2  array of the attributes of the tuple (ok)
     *@param len_in2  num of attributes of in2
     *@param t1_str_sizes shows the length of the string fields in S
     *@param t2_str_sizes shows the length of the string fields in R
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     */
    public static short[] setup_op_tuple(Tuple Jtuple, AttrType[] res_attrs,
                                         AttrType in1[], int len_in1, AttrType in2[],
                                         int len_in2, short t1_str_sizes[],
                                         short t2_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            MapUtilsException
    {
        short [] sizesT1 = new short [len_in1];
        short [] sizesT2 = new short [len_in2];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        for (count = 0, i = 0; i < len_in2; i++)
            if (in2[i].attrType == AttrType.attrString)
                sizesT2[i] = t2_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);
            else if (proj_list[i].relation.key == RelSpec.innerRel)
                res_attrs[i] = new AttrType(in2[proj_list[i].offset-1].attrType);
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
            else if (proj_list[i].relation.key == RelSpec.innerRel && in2[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT2[proj_list[i].offset-1];
        }
        try {
            Jtuple.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }


    /**
     *set up the Jtuple's attrtype, string size,field number for using project
     *@param Jtuple  reference to an actual tuple  - no memory has been malloced
     *@param res_attrs  attributes type of result tuple
     *@param in1  array of the attributes of the tuple (ok)
     *@param len_in1  num of attributes of in1
     *@param t1_str_sizes shows the length of the string fields in S
     *@param proj_list shows what input fields go where in the output tuple
     *@param nOutFlds number of outer relation fileds
     *@exception IOException some I/O fault
     *@exception MapUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */

    //TODO: Might need to come back to this
    public static short[] setup_op_tuple(Tuple Jtuple, AttrType res_attrs[],
                                         AttrType in1[], int len_in1,
                                         short t1_str_sizes[],
                                         FldSpec proj_list[], int nOutFlds)
            throws IOException,
            MapUtilsException,
            InvalidRelation
    {
        short [] sizesT1 = new short [len_in1];
        int i, count = 0;

        for (i = 0; i < len_in1; i++)
            if (in1[i].attrType == AttrType.attrString)
                sizesT1[i] = t1_str_sizes[count++];

        int n_strs = 0;
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer)
                res_attrs[i] = new AttrType(in1[proj_list[i].offset-1].attrType);

            else throw new InvalidRelation("Invalid relation -innerRel");
        }

        // Now construct the res_str_sizes array.
        for (i = 0; i < nOutFlds; i++)
        {
            if (proj_list[i].relation.key == RelSpec.outer
                    && in1[proj_list[i].offset-1].attrType == AttrType.attrString)
                n_strs++;
        }

        short[] res_str_sizes = new short [n_strs];
        count         = 0;
        for (i = 0; i < nOutFlds; i++) {
            if (proj_list[i].relation.key ==RelSpec.outer
                    && in1[proj_list[i].offset-1].attrType ==AttrType.attrString)
                res_str_sizes[count++] = sizesT1[proj_list[i].offset-1];
        }

        try {
            Jtuple.setHdr((short)nOutFlds, res_attrs, res_str_sizes);
        }catch (Exception e){
            throw new MapUtilsException(e,"setHdr() failed");
        }
        return res_str_sizes;
    }
}




