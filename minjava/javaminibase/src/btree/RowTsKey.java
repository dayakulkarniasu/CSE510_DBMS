package btree;

import global.*;

public class RowTsKey extends StringIntegerKey
{
    // class constructor, using a string and an int
    public RowTsKey(String s, int val)
    {
        super(s, val);
    }

    // class constructor, using a string and an integer
    public RowTsKey(String s, Integer val)
    {
        super(s, val.intValue());
    }

    // class constructor, using a stringinteger object
    public RowTsKey(StringInteger si)
    {
        super(si);
    }
}