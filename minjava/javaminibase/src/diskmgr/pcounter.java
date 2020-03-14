/* File pcounter.java */

package diskmgr;

import global.*;

public class PCounter implements GlobalConst {

  public static int rcounter;
  public static int wcounter;

  public static void initialize() {
    rcounter = 0;
    wcounter = 0;
  }

  public static void readIncrement() {
    rcounter++;
  }

  public static void writeIncrement() {
    wcounter++;
  }

}// end of DB class
