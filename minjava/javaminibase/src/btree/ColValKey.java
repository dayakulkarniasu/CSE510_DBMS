package btree;

import global.*;

public class ColValKey extends StringStringKey
{
    // class constructor, using two strings
    public ColValKey(String s1, String s2)
    {
        super(s1, s2);
    }

    // class constructor, using a stringstring object
    public ColValKey(StringString ss)
    {
        super(ss);
    }
}