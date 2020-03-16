package global;

import java.util.Comparator;

public class StringString implements Comparator<StringString>, Comparable<StringString>
{
    private String s1;
    private String s2;

    // constructor using two strings
    public StringString(String str1, String str2)
    {
        s1 = new String(str1);
        s2 = new String(str2);
    }

    // constructor, copying another StringString
    public StringString(StringString ss)
    {
        s1 = new String(ss.s1);
        s2 = new String(ss.s2);
    }

    public String toString()
    {
        return new String(s1.concat(s2));
    }

    public String[] getStrings()
    {
        String[] strArry = new String[2];
        strArry[0] = new String(s1);
        strArry[1] = new String(s2);

        return strArry;
    }

    public int compareTo(StringString ss)
    {
        if(!this.s1.equals(ss.s1))
        {
            return this.s1.compareTo(ss.s1);
        }
        else
        {
            return this.s2.compareTo(ss.s2);
        }
    }

    public int compare(StringString ss1, StringString ss2)
    {
        return ss1.compareTo(ss2);
    }
}