package btree;

import global.*;

public class ColRowTsKey extends StringStringIntegerKey
{
    // class constructor, using a stringstringinteger, encouraged
    public ColRowTsKey(StringStringInteger ssi)
    {
        super(ssi);
    }

    // class constructor, using two strings and an int
    public ColRowTsKey(String s1, String s2, int val)
    {
        super(s1, s2, val);
    }

    // class constructor, using two strings and an integer
    public ColRowTsKey(String s1, String s2, Integer val)
    {
        super(s1, s2, val.intValue());
    }

    // class constructor, using a stringstring and an int
    public ColRowTsKey(StringString ss, int val)
    {
        super(ss, val);
    }

    // class constructor, using a stringstring and an integer
    public ColRowTsKey(StringString ss, Integer val)
    {
        super(ss, val.intValue());
    }
}