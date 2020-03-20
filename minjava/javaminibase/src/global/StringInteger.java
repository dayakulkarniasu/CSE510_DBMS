package global;

import java.util.Comparator;

public class StringInteger implements Comparator<StringInteger>, Comparable<StringInteger>
{
    private short len;
    private String s;
    private Integer value;

    // constructor, using a string and an int
    public StringInteger(String str, int val)
    {
        len = (short) str.length();
        s = new String(str);
        value = new Integer(val);
    }

    // constructor, using a string and an integer
    public StringInteger(String str, Integer val)
    {
        len = (short) str.length();
        s = new String(str);
        value = new Integer(val.intValue());
    }

    // constructor, copying from a StringInteger
    public StringInteger(StringInteger si)
    {
        len = si.len;
        s = new String(si.s);
        value = new Integer(si.value.intValue());
    }

    public String toString()
    {
        return new String(s.concat(new String(value.toString())));
    }

    public String getString()
    {
        return new String(s);
    }

    public Integer getInteger()
    {
        return new Integer(value.intValue());
    }

    public short getStrLen()
    {
        return len;
    }

    public int compareTo(StringInteger si)
    {
        if(!this.s.equals(si.s))
        {
            return this.value.compareTo(si.value);
        }
        else
        {
            return this.s.compareTo(si.s);
        }
    }

    public int compare(StringInteger si1, StringInteger si2)
    {
        return si1.compareTo(si2);
    }
}