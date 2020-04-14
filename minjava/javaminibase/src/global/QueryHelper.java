package global;

import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryHelper {

    /**
     * Each filter can either be *, a single value, or a range [x,y]. This function
     * takes the filters and determines What kind of operation each filter is then
     * converts it into the appropriate CondExpr to be used with a FIleScan
     * 
     * @param rowFilter
     * @param columnFilter
     * @param valueFilter
     * @return the outFilter to be used with the FileScan
     */
    public static CondExpr[] queryFilter(String rowFilter, String columnFilter, String valueFilter) {
        List<CondExpr> query = new ArrayList<CondExpr>();
        // If RowFilter is a range, create a range expression
        if (isRangeQuery(rowFilter)) {
            String[] bounds = getBounds(rowFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 1)) {
                query.add(expr);
            }
        }
        // Else if rowFilter is an equality, create an identity expression
        else if (isEqualityQuery(rowFilter)) {
            query.add(equalityQuery(rowFilter, 1));
        }
        // If ColumnFilter is a range, create a range expression
        if (isRangeQuery(columnFilter)) {
            String[] bounds = getBounds(columnFilter);
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 2)) {
                query.add(expr);
            }
        }
        // Else if columnFilter is an equality, create an identity expression
        else if (isEqualityQuery(columnFilter)) {
            query.add(equalityQuery(columnFilter, 2));
        }

        // If ValueFilter is a range, create a range expression
        if (isRangeQuery(valueFilter)) {
            String[] bounds = getBounds(valueFilter);
            // System.out.println(isRangeQuery(valueFilter));
            System.out.println(bounds[0]+","+bounds[1]);// test output
            for (CondExpr expr : rangeQuery(bounds[0], bounds[1], 3)) {
                query.add(expr);
            }
        }
        // Else if valueFilter is an equality, create an identity expression
        else if (isEqualityQuery(valueFilter)) {
            query.add(equalityQuery(valueFilter, 3));
        }
        // convert results to the format required for scans
        return ConvertToArray(query);
    }

    /**
     * We Determine if a filter is a range by checking for the presence of the ','
     * character A range query would be in the format [x,y]
     * 
     * @param filter
     * @return
     */
    private static boolean isRangeQuery(java.lang.String filter) {
        return filter.contains(",");
    }

    /**
     * This filter check is context specific, it is always called after
     * isRangeQuery(). In those cases, the only other possible filter under
     * consideration is the single value. A single value will be anything not
     * containing '*'
     * 
     * @param filter
     * @return
     */
    private static boolean isEqualityQuery(java.lang.String filter) {
        return !filter.contains("*");
    }

    /**
     * Parses a range and returns the upper and lower bounds
     * 
     * @param filter [x,y] range
     * @return
     */
    private static String[] getBounds(java.lang.String filter) {
        return filter.replaceAll("(\\[|\\])", "").split(",");
    }

    /**
     * Converts a List of queries into the format required by FileScan, CondExpr[].
     * 
     * @param queries A List of Queries
     * @return
     */
    private static CondExpr[] ConvertToArray(List<CondExpr> queries) {
        // The CondExpr needs to terminate with a null CondExpr,
        // Hence we take the query size + 1
        CondExpr[] plan = new CondExpr[queries.size() + 1];
        for (int i = 0; i < queries.size(); i++) {
            plan[i] = queries.get(i);
        }
        plan[queries.size()] = null;
        return plan;
    }

    /**
     * Generates a CondExpr for an equality operation
     * 
     * @param Filter Single Value Filter
     * @param offSet The OffSet of the map attribute under consideration
     * @return
     */
    private static CondExpr equalityQuery(String Filter, int offSet) {
        // set up an equality expression
        CondExpr expr = new CondExpr();
        expr.op = new AttrOperator(AttrOperator.aopEQ);
        expr.type1 = new AttrType(AttrType.attrSymbol);
        expr.type2 = new AttrType(AttrType.attrString);
        expr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offSet);
        expr.operand2.string = Filter;
        expr.next = null;
        return expr;
    }

    /**
     * Generates a set of CondExpr needed for a range search operation
     * 
     * @param lowerBound String value of lower bound of range
     * @param upperBound String value of upper bound of range
     * @param offset     The OffSet of the map attribute under consideration
     * @return
     */
    private static CondExpr[] rangeQuery(java.lang.String lowerBound, java.lang.String upperBound, int offset) {
        // range scan
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopGE);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
        expr[0].operand2.string = lowerBound;
        expr[0].next = null;
        expr[1] = new CondExpr();
        expr[1].op = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
        expr[1].operand2.string = upperBound;
        expr[1].next = null;
        return expr;
    }

    public static CondExpr[] EqualityFilterByValue() {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[1] = new CondExpr();

        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 3);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

        expr[1] = null;
        return expr;
    }
}
