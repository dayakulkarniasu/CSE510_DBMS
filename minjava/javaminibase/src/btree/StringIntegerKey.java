package btree;

import global.*;

public class StringIntegerKey extends KeyClass
{
    StringInteger key;

    public String toString()
    {
        return key.toString();
    }

    // class constructor, using a string and an int
    public StringIntegerKey(String s, int val)
    {
        key = new StringInteger(s, val);
    }

    // class constructor, using a string and an integer
    public StringIntegerKey(String s, Integer val)
    {
        key = new StringInteger(s, val.intValue());
    }

    // class constructor, using a stringinteger object
    public StringIntegerKey(StringInteger si)
    {
        key = new StringInteger(si);
    }

    // get a copy of the key
    public StringInteger getKey()
    {
        return new StringInteger(key);
    }

    // sets the stringstring key value with a string and an int
    public void setKey(String s, int val)
    {
        key = new StringInteger(s, val);
    }

    // sets the stringstring key value with a string and an integer
    public void setKey(String s, Integer val)
    {
        key = new StringInteger(s, val.intValue());
    }

    // sets the stringstring key value with a stringinteger
    public void setKey(StringInteger si)
    {
        key = new StringInteger(si);
    }
}