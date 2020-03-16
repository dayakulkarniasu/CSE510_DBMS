package btree;

import global.*;

public class ColTsKey extends StringIntegerKey
{
    // class constructor, using a string and an int
    public ColTsKey(String s, int val)
    {
        super(s, val);
    }

    // class constructor, using a string and an integer
    public ColTsKey(String s, Integer val)
    {
        super(s, val.intValue());
    }

    // class constructor, using a stringinteger object
    public ColTsKey(StringInteger si)
    {
        super(si);
    }
}