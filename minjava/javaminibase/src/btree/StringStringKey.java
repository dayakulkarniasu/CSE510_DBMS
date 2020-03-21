package btree;

import global.*;

public class StringStringKey extends KeyClass
{
    StringString key;

    public String toString()
    {
        return key.toString();
    }

    // class constructor, using two strings
    public StringStringKey(String s1, String s2)
    {
        key = new StringString(s1, s2);
    }

    // class constructor, using a stringstring object
    public StringStringKey(StringString ss)
    {
        key = new StringString(ss);
    }

    // get a copy of the key
    public StringString getKey()
    {
        return new StringString(key);
    }

    // sets the stringstring key value with two strings
    public void setKey(String s1, String s2)
    {
        key = new StringString(s1, s2);
    }

    // sets the stringstring key value with a stringstring
    public void setKey(StringString ss)
    {
        key = new StringString(ss);
    }
}