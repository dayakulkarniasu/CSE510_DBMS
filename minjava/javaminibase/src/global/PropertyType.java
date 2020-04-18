package global;

public class PropertyType {
    public static final String RowLabel = "RowLabel";
    public static final String ColumnLabel = "ColumnLabel";
    public static final String TimeStamp = "TimeStamp";
    public static final String Value = "Value";

    public String propertyType;

    /**
     * PropertyType Constructor
     * @param _propertyType The types of properties in a bigTable
     */

    public PropertyType(String _propertyType) {
        propertyType = _propertyType;
    }
}
