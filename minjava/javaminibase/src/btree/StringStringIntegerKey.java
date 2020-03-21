package btree;

import global.*;

public class StringStringIntegerKey extends KeyClass
{
    StringStringInteger key;

    public String toString()
    {
        return key.toString();
    }

    // class constructor, using a stringstringinteger, encouraged
    public StringStringIntegerKey(StringStringInteger ssi)
    {
        key = new StringStringInteger(ssi);
    }

    // class constructor, using two strings and an int
    public StringStringIntegerKey(String s1, String s2, int val)
    {
        key = new StringStringInteger(s1, s2, val);
    }

    // class constructor, using two strings and an integer
    public StringStringIntegerKey(String s1, String s2, Integer val)
    {
        key = new StringStringInteger(s1, s2, val.intValue());
    }

    // class constructor, using a stringstring and an int
    public StringStringIntegerKey(StringString ss, int val)
    {
        key = new StringStringInteger(ss, val);
    }

    // class constructor, using a stringstring and an integer
    public StringStringIntegerKey(StringString ss, Integer val)
    {
        key = new StringStringInteger(ss, val.intValue());
    }

    // get a copy of the key
    public StringStringInteger getKey()
    {
        return new StringStringInteger(key);
    }

    // sets the stringstring key value with a stringstringinteger
    public void setKey(StringStringInteger ssi)
    {
        key = new StringStringInteger(ssi);
    }

    // sets the stringstring key value with two strings and an int
    public void setKey(String s1, String s2, int val)
    {
        key = new StringStringInteger(s1, s2, val);
    }

    // sets the stringstring key value with two strings and an integer
    public void setKey(String s1, String s2, Integer val)
    {
        key = new StringStringInteger(s1, s2, val.intValue());
    }

    // sets the stringstring key value with a stringstring and an int
    public void setKey(StringString ss, int val)
    {
        key = new StringStringInteger(ss, val);
    }

    // sets the stringstring key value with a stringstring and an integer
    public void setKey(StringString ss, Integer val)
    {
        key = new StringStringInteger(ss, val.intValue());
    }
}