package global;

/**
 * Enumeration class for AttrType
 *
 */

public class OrderType {

    public static final int type1 = 1;
    public static final int type2 = 2;
    public static final int type3 = 3;
    public static final int type4 = 4;
    public static final int type5 = 5;

    public int orderType;

    /**
     * orderType Constructor
     * <br>
     * An attribute type of String can be defined as
     * <ul>
     * <li>   AttrType attrType = new AttrType(AttrType.attrString);
     * </ul>
     * and subsequently used as
     * <ul>
     * <li>   if (attrType.attrType == AttrType.attrString) ....
     * </ul>
     *
     * @param _orderType The types of attributes available in this class
     */

    public OrderType(int _orderType) {
        orderType = _orderType;
    }

    public String toString() {

        switch (orderType) {
            case type1:
                return "type1";
            case type2:
                return "type2";
            case type3:
                return "type3";
            case type4:
                return "type4";
            case type5:
                return "type5";
        }
        return ("Unexpected AttrType " + orderType);
    }
}
