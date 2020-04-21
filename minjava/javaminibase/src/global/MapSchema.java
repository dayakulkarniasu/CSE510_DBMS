package global;

import iterator.FldSpec;
import iterator.RelSpec;

public class MapSchema {

    public static int MapFldCount() {
        return 4;
    }

    public static short[] MapStrLengths() {
        short[] attrSize = new short[3];
        attrSize[0] = GlobalConst.STR_LEN;
        attrSize[1] = GlobalConst.STR_LEN;
        attrSize[2] = GlobalConst.STR_LEN;
        return attrSize;
    }

    public static AttrType[] MapAttrType() {
        AttrType[] attrType = new AttrType[4];
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrString);
        attrType[2] = new AttrType(AttrType.attrString);
        attrType[3] = new AttrType(AttrType.attrInteger);
        return attrType;
    }

    public static FldSpec[] OutputMapSchema() {
        FldSpec[] schema = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        schema[0] = new FldSpec(rel, 1);
        schema[1] = new FldSpec(rel, 2);
        schema[2] = new FldSpec(rel, 3);
        schema[3] = new FldSpec(rel, 4);
        return schema;
    }
}
