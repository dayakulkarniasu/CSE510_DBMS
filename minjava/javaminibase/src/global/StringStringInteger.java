package global;

import java.util.Comparator;

public class StringStringInteger implements Comparator<StringStringInteger>, Comparable<StringStringInteger>
{
    private short len1;
    private short len2;
    private StringString ss;
    private Integer value;

    // constructor, using two strings and an int
    public StringStringInteger(String s1, String s2, int val)
    {
        len1 = (short) s1.length();
        len2 = (short) s2.length();
        ss = new StringString(s1, s2);
        value = new Integer(val);
    }

    // constructor, using two strings and an integer
    public StringStringInteger(String s1, String s2, Integer val)
    {
        len1 = (short) s1.length();
        len2 = (short) s2.length();
        ss = new StringString(s1, s2);
        value = new Integer(val.intValue());
    }

    // constructor, using a stringstring and an int
    public StringStringInteger(StringString ss1, int val)
    {
        ss = new StringString(ss1);
        len1 = ss1.getStrLen()[0];
        len2 = ss1.getStrLen()[1];
        value = new Integer(val);
    }

    // constructor, using a stringstring and an integer
    public StringStringInteger(StringString ss1, Integer val)
    {
        ss = new StringString(ss1);
        len1 = ss1.getStrLen()[0];
        len2 = ss1.getStrLen()[1];
        value = new Integer(val.intValue());
    }

    // constructor, copying from another StringStringInteger
    public StringStringInteger(StringStringInteger ssi)
    {
        ss = new StringString(ssi.getStringString());
        len1 = ssi.len1;
        len2 = ssi.len2;
        value = new Integer(ssi.getInteger());
    }

    // get the stringstring
    public StringString getStringString()
    {
        return new StringString(ss);
    }

    // get the integer
    public Integer getInteger()
    {
        return new Integer(value.intValue());
    }

    // overriding toString method
    public String toString()
    {
        String[] str = ss.getStrings();
        return new String(str[0].concat(str[1]).concat(new String(value.toString())));
    }

    // return strings as a string array
    public String[] getStrings()
    {
        return ss.getStrings();
    }

    // get the strlen
    public short[] getStrLen()
    {
        return new short[]{len1, len2};
    }

    // overriding compareTo method
    public int compareTo(StringStringInteger ssi)
    {
        if(!this.ss.equals(ssi.ss))
        {
            return this.ss.compareTo(ssi.ss);
        }
        else
        {
            return this.value.compareTo(ssi.value);
        }
    }

    // implementing compare method
    public int compare(StringStringInteger ssi1, StringStringInteger ssi2)
    {
        return ssi1.compareTo(ssi2);
    }
}