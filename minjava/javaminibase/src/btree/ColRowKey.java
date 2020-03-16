package btree;

import global.*;

public class ColRowKey extends StringStringKey
{
    // class constructor, using two strings
    public ColRowKey(String s1, String s2)
    {
        super(s1, s2);
    }

    // class constructor, using a stringstring object
    public ColRowKey(StringString ss)
    {
        super(ss);
    }
}