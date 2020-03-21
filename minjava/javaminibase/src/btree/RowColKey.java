package btree;

import global.*;

public class RowColKey extends StringStringKey
{
    // class constructor, using two strings
    public RowColKey(String s1, String s2)
    {
        super(s1, s2);
    }

    // class constructor, using a stringstring object
    public RowColKey(StringString ss)
    {
        super(ss);
    }
}