/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.xbib.jdbc.csv;

import org.xbib.io.DataReader;
import org.xbib.io.ListDataReader;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class implements the ResultSet interface for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @author Michael Maraya
 * @author Tomasz Skutnik
 * @author Chetan Gupta
 * @version $Id: CsvResultSet.java,v 1.56 2011/11/01 13:23:00 simoc Exp $
 */
public class CsvResultSet implements ResultSet {

    /**
     * Metadata for this ResultSet
     */
    private ResultSetMetaData resultSetMetaData;
    /**
     * Statement that produced this ResultSet
     */
    private CsvStatement statement;
    private int isScrollable = ResultSet.TYPE_SCROLL_SENSITIVE;
    /**
     * Helper class that performs the actual file reads
     */
    private DataReader reader;
    /**
     * Table referenced by the Statement
     */
    private String tableName;
    /**
     * Last column name index read
     */
    private int lastIndexRead = -1;
    private Expression whereClause;
    private List<Expression> groupByColumns;
    private List<Expression> distinctColumns;
    private Expression havingClause;
    private List<Object[]> orderByColumns;
    private List<Object[]> queryEnvironment;
    private List<AggregateFunction> aggregateFunctions;
    private Set<List<Object>> distinctValues;
    private Map<String, Object> recordEnvironment;
    private List<String> usedColumns;
    private String timeFormat;
    private String dateFormat;
    private String timeZone;
    private StringConverter converter;
    private List<Map<String, Object>> bufferedRecordEnvironments = null;
    private int currentRow;
    private boolean hitTail = false;
    private int maxRows;
    private int fetchSize;
    private int limit;
    private boolean isClosed = false;

    /**
     * Compares SQL ORDER BY expressions for two records.
     */
    public class OrderByComparator implements Comparator<Map<String, Object>> {

        public int compare(Map<String, Object> recordEnvironment1, Map<String, Object> recordEnvironment2) {
            int retval = 0;
            int i = 0;
            while (i < orderByColumns.size() && retval == 0) {
                Object[] o = orderByColumns.get(i);
                Integer direction = (Integer) o[0];
                Expression expr = (Expression) o[1];
                recordEnvironment = recordEnvironment1;
                Map<String, Object> objectEnvironment1 = updateRecordEnvironment(true);
                if (converter != null) {
                    objectEnvironment1.put("@STRINGCONVERTER", converter);
                }
                Comparable<Object> result1 = (Comparable<Object>) expr.eval(objectEnvironment1);
                recordEnvironment = recordEnvironment2;
                Map<String, Object> objectEnvironment2 = updateRecordEnvironment(true);
                if (converter != null) {
                    objectEnvironment2.put("@STRINGCONVERTER", converter);
                }
                Comparable<Object> result2 = (Comparable<Object>) expr.eval(objectEnvironment2);
                if (result1 == null) {
                    if (result2 == null) {
                        retval = 0;
                    } else {
                        retval = -1;
                    }
                } else if (result2 == null) {
                    retval = 1;
                } else {
                    retval = result1.compareTo(result2);
                }
                if (direction.intValue() < 0) {
                    retval = -retval;
                }
                i++;
            }
            return retval;
        }
    }

    protected CsvResultSet(CsvStatement statement,
                           DataReader reader,
                           String tableName,
                           List<Object[]> queryEnvironment,
                           boolean isDistinct,
                           int isScrollable,
                           Expression whereClause,
                           List<Expression> groupByColumns,
                           Expression havingClause,
                           List<Object[]> orderByColumns,
                           int sqlLimit,
                           int sqlOffset,
                           String columnTypes,
                           int skipLeadingLines) throws ClassNotFoundException, SQLException {
        this.statement = statement;
        maxRows = statement.getMaxRows();
        fetchSize = statement.getFetchSize();
        this.limit = sqlLimit;
        this.isScrollable = isScrollable;
        this.reader = reader;
        this.tableName = tableName;
        this.queryEnvironment = new ArrayList<Object[]>(queryEnvironment);
        this.aggregateFunctions = new ArrayList<AggregateFunction>();
        this.whereClause = whereClause;
        if (groupByColumns != null) {
            this.groupByColumns = new ArrayList<Expression>(groupByColumns);
        } else {
            this.groupByColumns = null;
        }
        this.havingClause = havingClause;
        if (orderByColumns != null) {
            this.orderByColumns = new ArrayList<Object[]>(orderByColumns);
        } else {
            this.orderByColumns = null;
        }
        if (isDistinct) {
            this.distinctValues = new HashSet<List<Object>>();
        }
        if (reader instanceof CsvReader || reader instanceof ListDataReader) {
            // timestampFormat = ((CsvConnection)statement.getConnection()).getTimestampFormat();
            timeFormat = ((CsvConnection) statement.getConnection()).getTimeFormat();
            dateFormat = ((CsvConnection) statement.getConnection()).getDateFormat();
            timeZone = ((CsvConnection) statement.getConnection()).getTimeZoneName();
            this.converter = new StringConverter(dateFormat, timeFormat, timeZone);
            if (reader instanceof CsvReader) {
                ((CsvReader) reader).setConverter(converter);
                if (!"".equals(columnTypes)) {
                    ((CsvReader) reader).setColumnTypes(columnTypes);
                }
            }
        }
        if (whereClause != null) {
            this.usedColumns = new LinkedList<String>(whereClause.usedColumns());
        } else {
            this.usedColumns = new LinkedList<String>();
        }

        String[] columnNames = reader.getColumnNames();

        String tableAlias = reader.getTableAlias();
        HashSet<String> allReaderColumns = new HashSet<String>();
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i].toUpperCase();
            allReaderColumns.add(columnName);
            if (tableAlias != null) {
                allReaderColumns.add(tableAlias + "." + columnName);
            }
        }

        /*
         * Replace any "select *" with the list of column names in that table.
         */
        for (int i = 0; i < this.queryEnvironment.size(); i++) {
            Object[] o = this.queryEnvironment.get(i);
            if (o[1] instanceof AsteriskExpression) {
                AsteriskExpression asteriskExpression = (AsteriskExpression) o[1];

                /*
                 * Check that any table alias is valid.
                 */
                String asterisk = asteriskExpression.toString();
                if (!(asterisk.equals("*") || (tableAlias != null && asterisk.equalsIgnoreCase(tableAlias + ".*")))) {
                    throw new SQLException("Invalid column name: " + asterisk);
                }
                this.queryEnvironment.remove(i);
                for (int j = 0; j < columnNames.length; j++) {
                    this.queryEnvironment.add(i + j, new Object[]{columnNames[j], new ColumnName(columnNames[j])});
                }
            }
        }

        /*
         * Replace any "group by 2" with the 2nd column in the query list.
         */
        if (this.groupByColumns != null) {
            for (int i = 0; i < this.groupByColumns.size(); i++) {
                Expression expression = this.groupByColumns.get(i);
                if (expression instanceof NumericConstant) {
                    NumericConstant n = (NumericConstant) expression;
                    if (!(n.value instanceof Integer)) {
                        throw new SQLException("Invalid GROUP BY column: " + n);
                    }
                    int index = n.value.intValue();

                    /*
                     * Column numbering in SQL starts at 1, not 0.
                     */
                    index--;

                    if (index < 0 || index >= this.queryEnvironment.size()) {
                        throw new SQLException("Invalid GROUP BY column: " + (index + 1));
                    }
                    Object[] q = this.queryEnvironment.get(index);
                    this.groupByColumns.set(i, (Expression) q[1]);
                }
            }
        }

        if (this.groupByColumns != null) {
            for (Expression expr : this.groupByColumns) {
                this.usedColumns.addAll(expr.usedColumns());
            }
            if (havingClause != null) {
                this.usedColumns.addAll(havingClause.usedColumns());
            }
        }

        /*
         * Replace any "order by 2" with the 2nd column in the query list.
         */
        if (this.orderByColumns != null) {
            for (Object[] o : this.orderByColumns) {
                Expression expression = (Expression) o[1];
                if (expression instanceof NumericConstant) {
                    NumericConstant n = (NumericConstant) expression;
                    if (!(n.value instanceof Integer)) {
                        throw new SQLException("Invalid ORDER BY column: " + n);
                    }
                    int index = n.value.intValue();

                    /*
                     * Column numbering in SQL starts at 1, not 0.
                     */
                    index--;

                    if (index < 0 || index >= this.queryEnvironment.size()) {
                        throw new SQLException("Invalid ORDER BY column: " + (index + 1));
                    }
                    Object[] q = this.queryEnvironment.get(index);
                    o[1] = q[1];
                }
            }
        }

        if (this.orderByColumns != null) {
            for (Object[] o : this.orderByColumns) {
                Expression expr = (Expression) o[1];
                this.usedColumns.addAll(expr.usedColumns());
            }
        }

        /*
         * Find any SQL aggregate functions so they can be evaluated separately.
         */
        for (int i = 0; i < this.queryEnvironment.size(); i++) {
            Object[] o = this.queryEnvironment.get(i);
            Expression expr = (Expression) o[1];
            List<AggregateFunction> exprAggregateFunctions = expr.aggregateFunctions();
            this.aggregateFunctions.addAll(exprAggregateFunctions);
            for (AggregateFunction aggregateFunction : exprAggregateFunctions) {
                this.usedColumns.addAll(aggregateFunction.aggregateColumns());
            }
        }

        if (aggregateFunctions.size() > 0 && this.groupByColumns == null) {
            /*
             * Check there is no mix of query columns and aggregate functions.
             */
            List<String> allUsedColumns = new LinkedList<String>();
            for (int i = 0; i < this.queryEnvironment.size(); i++) {
                Object[] o = this.queryEnvironment.get(i);
                if (o[1] != null) {
                    allUsedColumns.addAll(((Expression) o[1]).usedColumns());
                }
            }
            if (allUsedColumns.size() > 0 && aggregateFunctions.size() > 0) {
                throw new SQLException("Query columns cannot be combined with aggregate functions");
            }
        }
        if (whereClause != null && whereClause.aggregateFunctions().size() > 0) {
            throw new SQLException("Aggregate functions not allowed in WHERE clause");
        }

        if (!((CsvConnection) statement.getConnection()).isIndexedFiles()) {
            //TODO no check when indexedFiles=true because unit test TestCsvDriver.testFromNonExistingIndexedTable then fails.            /*
             // Check that each selected expression is valid, using only column names contained in the table.

            for (int i = 0; i < this.queryEnvironment.size(); i++) {
                Object[] o = this.queryEnvironment.get(i);
                if (o[1] != null) {
                    Expression expr = (Expression) o[1];
                    List<String> exprUsedColumns = expr.usedColumns();
                    for (Object usedColumn : exprUsedColumns) {
                        if (!allReaderColumns.contains(usedColumn)) {
                            throw new SQLException("Invalid column name: " + usedColumn);
                        }
                    }
                }
                //TODO selected column aliases are allowed in WHERE clause (although this is invalid SQL) and unit tested in TestCsvDriver.testFieldAsAlias so add all aliases to list too.
                allReaderColumns.add(o[0].toString());
            }
        }

        /*
         * Check that all columns used in the WHERE, GROUP BY, HAVING
         * and ORDER BY clauses do exist in the table.
         */
        if (!((CsvConnection) statement.getConnection()).isIndexedFiles()) {
            for (Object usedColumn : this.usedColumns) {
                if (!allReaderColumns.contains(usedColumn)) {
                    throw new SQLException("Invalid column name: " + usedColumn);
                }
            }

            checkGroupBy();

            if (this.orderByColumns != null) {
                for (Object[] o : this.orderByColumns) {
                    Expression expr = (Expression) o[1];
                    List<String> exprUsedColumns = new LinkedList<String>(expr.usedColumns());
                    for (AggregateFunction aggregatFunction : expr.aggregateFunctions()) {
                        exprUsedColumns.addAll(aggregatFunction.aggregateColumns());
                    }
                    if (exprUsedColumns.isEmpty()) {
                        /*
                         * Must order by something that contains at least one column, not 'foo' or 1+1.
                         */
                        throw new SQLException("Invalid ORDER BY column: " + expr.toString());
                    }
                }
            }
        }

        if (this.groupByColumns != null
                || this.orderByColumns != null || this.aggregateFunctions.size() > 0
                || this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            bufferedRecordEnvironments = new ArrayList<Map<String, Object>>();
            currentRow = 0;
        }

        if (this.groupByColumns != null) {
            /*
             * Read all rows and group them together based on GROUP BY expressions.
             */
            int savedMaxRows = maxRows;
            int savedLimit = limit;
            maxRows = 0;
            limit = -1;
            ArrayList<ArrayList<Object>> groupOrder = new ArrayList<ArrayList<Object>>();
            HashMap<ArrayList<Object>, ArrayList<Map<String, Object>>> groups = new HashMap<ArrayList<Object>, ArrayList<Map<String, Object>>>();
            try {
                while (next()) {
                    Map<String, Object> objectEnvironment = updateRecordEnvironment(true);
                    if (converter != null) {
                        objectEnvironment.put("@STRINGCONVERTER", converter);
                    }
                    ArrayList<Object> groupByKeys = new ArrayList<Object>(this.groupByColumns.size());
                    for (Expression expr : this.groupByColumns) {
                        groupByKeys.add(expr.eval(objectEnvironment));
                    }
                    ArrayList<Map<String, Object>> groupByValues = groups.get(groupByKeys);
                    if (groupByValues == null) {
                        groupByValues = new ArrayList<Map<String, Object>>();
                        groups.put(groupByKeys, groupByValues);
                        groupOrder.add(groupByKeys);
                    }
                    groupByValues.add(recordEnvironment);
                }
                bufferedRecordEnvironments.clear();
                for (ArrayList<Object> groupByKey : groupOrder) {
                    ArrayList<Map<String, Object>> values = groups.get(groupByKey);

                    /*
                     * Create a row in the ResultSet for each group with a
                     * reference to all the rows in that group so we can
                     * later calculate any aggregate functions for each group.
                     */
                    Map<String, Object> firstRow = new HashMap<String, Object>(values.get(0));
                    firstRow.put("@GROUPROWS", values);

                    if (this.havingClause == null || this.havingClause.isTrue(firstRow)) {
                        bufferedRecordEnvironments.add(firstRow);
                    }
                }

                if (this.orderByColumns != null) {
                    sortRows(sqlOffset);
                }
            } finally {
                maxRows = savedMaxRows;
                limit = savedLimit;
            }

            /*
             * Rewind back to before the row so we can read it.
             */
            currentRow = 0;
            recordEnvironment = null;
            updateRecordEnvironment(false);
            hitTail = true;

        } else if (this.aggregateFunctions.size() > 0) {
            /*
             * Read all rows, evaluating the aggregate functions for each row to
             * produce a single row result.
             */
            int savedMaxRows = maxRows;
            int savedLimit = limit;
            maxRows = 0;
            limit = -1;
            try {
                while (next()) {
                    for (Object o : this.aggregateFunctions) {
                        AggregateFunction func = (AggregateFunction) o;
                        func.processRow(recordEnvironment);
                    }
                }

                /*
                 * Create a single row ResultSet from the aggregate functions.
                 */
                bufferedRecordEnvironments.clear();
                if ((savedLimit < 0 || savedLimit > 0) && sqlOffset == 0) {
                    bufferedRecordEnvironments.add(new HashMap<String, Object>());
                }
            } finally {
                maxRows = savedMaxRows;
                limit = savedLimit;
            }

            /*
             * Rewind back to before the row so we can read it.
             */
            currentRow = 0;
            recordEnvironment = null;
            updateRecordEnvironment(false);
            hitTail = true;

        } else if (this.orderByColumns != null) {
            /*
             * Read all rows into memory and sort them based on SQL ORDER BY expressions.
             */
            int savedMaxRows = maxRows;
            int savedLimit = limit;
            maxRows = 0;
            limit = -1;
            try {
                while (next()) {
                    ;
                }
            } finally {
                maxRows = savedMaxRows;
                limit = savedLimit;
            }
            sortRows(sqlOffset);

            /*
             * Rewind back to before first row so we can now read them in sorted order.
             */
            currentRow = 0;
            recordEnvironment = null;
            updateRecordEnvironment(false);
        } else if (sqlOffset > 0) {

            int savedMaxRows = maxRows;
            int savedLimit = limit;
            maxRows = 0;
            limit = -1;

            /*
             * Skip the first n rows.
             */
            try {
                while (sqlOffset > 0) {
                    if (!next()) {
                        break;
                    }
                    sqlOffset--;
                }
            } finally {
                maxRows = savedMaxRows;
                limit = savedLimit;
                currentRow = 0;
                if (bufferedRecordEnvironments != null) {
                    bufferedRecordEnvironments.clear();
                }
            }
        }
    }

    /**
     * Check that all selected and ORDER BY columns also appear in any GROUP BY
     * clause.
     *
     * @throws SQLException
     */
    private void checkGroupBy() throws SQLException {
        if (this.groupByColumns != null) {
            for (Expression expr : this.groupByColumns) {
                List<String> exprUsedColumns = expr.usedColumns();
                if (exprUsedColumns.isEmpty()) {
                    /*
                     * Must group by something that contains at least one column, not 'foo' or 1+1.
                     */
                    throw new SQLException("Invalid GROUP BY column: " + expr.toString());
                }
            }
            ArrayList<String> groupingColumns = new ArrayList<String>();
            for (Expression expr : this.groupByColumns) {
                groupingColumns.addAll(expr.usedColumns());
            }
            ArrayList<String> queryEnvironmentColumns = new ArrayList<String>();
            for (int i = 0; i < this.queryEnvironment.size(); i++) {
                Object[] o = this.queryEnvironment.get(i);
                queryEnvironmentColumns.add(o[0].toString());
                if (o[1] != null) {
                    Expression expr = (Expression) o[1];
                    for (Object o2 : expr.usedColumns()) {
                        queryEnvironmentColumns.add(o2.toString());
                    }
                }
            }
            for (int i = 0; i < this.queryEnvironment.size(); i++) {
                Object[] o = this.queryEnvironment.get(i);
                if (!groupingColumns.contains(o[0])) {
                    if (o[1] != null) {
                        Expression expr = (Expression) o[1];
                        for (Object o2 : expr.usedColumns()) {
                            String columnName = o2.toString();
                            if (!groupingColumns.contains(columnName)) {
                                String tableAlias = this.reader.getTableAlias();
                                if (tableAlias == null || (!groupingColumns.contains(tableAlias + "." + columnName))) {
                                    /*
                                     * GROUP BY must include all queried columns.
                                     */
                                    throw new SQLException("Column not included in GROUP BY: " + columnName);
                                }
                            }
                        }
                    }
                }
            }
            if (this.havingClause != null) {
                for (String columnName : this.havingClause.usedColumns()) {
                    if (!queryEnvironmentColumns.contains(columnName)) {
                        throw new SQLException("Invalid HAVING column: " + columnName);
                    }
                }
            }
            if (this.orderByColumns != null) {
                for (Object[] o : this.orderByColumns) {
                    Expression expr = (Expression) o[1];
                    for (Object o2 : expr.usedColumns()) {
                        if (!queryEnvironmentColumns.contains(o2.toString())) {
                            throw new SQLException("ORDER BY column not included in GROUP BY: " + o2);
                        }
                    }
                }
            }

            /*
             * A query containing GROUP BY without any aggregate functions can be simplified
             * to a SELECT DISTINCT, avoiding the need to load all records into memory.
             */
            boolean hasAggregateFunctions = false;
            for (int i = 0; i < this.queryEnvironment.size(); i++) {
                Object[] o = this.queryEnvironment.get(i);
                Expression expr = (Expression) o[1];
                if (expr.aggregateFunctions().size() > 0) {
                    hasAggregateFunctions = true;
                }
            }
            if (this.havingClause != null && this.havingClause.aggregateFunctions().size() > 0) {
                hasAggregateFunctions = true;
            }
            if (!hasAggregateFunctions) {
                this.distinctValues = new HashSet<List<Object>>();
                this.distinctColumns = new ArrayList<Expression>(this.groupByColumns);
                this.groupByColumns = null;
            }
        }
    }

    private void sortRows(int sqlOffset) {
        Map<String, Object>[] allRows = new Map[bufferedRecordEnvironments.size()];
        for (int i = 0; i < allRows.length; i++) {
            allRows[i] = bufferedRecordEnvironments.get(i);
        }
        bufferedRecordEnvironments.clear();
        Arrays.sort(allRows, new OrderByComparator());
        int rowLimit = allRows.length;
        if (maxRows != 0 && maxRows < rowLimit) {
            rowLimit = maxRows;
        }
        if (limit >= 0 && sqlOffset + limit < rowLimit) {
            rowLimit = sqlOffset + limit;
        }

        for (int i = sqlOffset; i < rowLimit; i++) {
            bufferedRecordEnvironments.add(allRows[i]);
        }
    }

    private void checkOpen() throws SQLException {
        if (isClosed) {
            throw new SQLException("ResultSet is already closed");
        }
    }

    /**
     * Moves the cursor down one row from its current position. A
     * <code>ResultSet</code> cursor is initially positioned before the first
     * row; the first call to the method
     * <code>next</code> makes the first row the current row; the second call
     * makes the second row the current row, and so on.
     * <p/>
     * <P>If an input stream is open for the current row, a call to the method
     * <code>next</code> will implicitly close it. A
     * <code>ResultSet</code> object's warning chain is cleared when a new row
     * is read.
     *
     * @return <code>true</code> if the new current row is valid;
     *         <code>false</code> if there are no more rows
     * @throws SQLException if a database access error occurs
     */
    public boolean next() throws SQLException {

        checkOpen();

        if ((this.groupByColumns != null
                || this.aggregateFunctions.size() > 0
                || this.orderByColumns != null || this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE)
                && currentRow < bufferedRecordEnvironments.size()) {
            currentRow++;
            recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
            updateRecordEnvironment(true);
            return true;
        } else {
            boolean thereWasAnAnswer;
            if (maxRows != 0 && currentRow >= maxRows) {
                // Do not fetch any more rows, we have reached the row limit set by caller.
                thereWasAnAnswer = false;
            } else if (limit >= 0 && currentRow >= limit) {
                thereWasAnAnswer = false;
            } else if (hitTail) {
                thereWasAnAnswer = false;
            } else {
                thereWasAnAnswer = reader.next();
            }

            if (thereWasAnAnswer) {
                recordEnvironment = reader.getEnvironment();
            } else {
                recordEnvironment = null;
            }

            // We have a where clause or DISTINCT keyword, honor it
            if (whereClause != null || distinctValues != null) {
                Map<String, Object> objectEnvironment = updateRecordEnvironment(thereWasAnAnswer);
                while (thereWasAnAnswer) {
                    if (whereClause == null || whereClause.isTrue(objectEnvironment)) {
                        /*
                         * Check HAVING clause if no aggregate functions in query and
                         * it is being processed just like SELECT DISTINCT.
                         * In this case HAVING is exactly the same as a WHERE clause.
                         */
                        if (this.distinctColumns == null || this.havingClause == null || this.havingClause.isTrue(objectEnvironment)) {
                            if (distinctValues == null || addDistinctEnvironment(objectEnvironment)) {
                                break;
                            }
                        }
                    }
                    thereWasAnAnswer = reader.next();
                    if (thereWasAnAnswer) {
                        recordEnvironment = reader.getEnvironment();
                    } else {
                        recordEnvironment = null;
                    }
                    objectEnvironment = updateRecordEnvironment(thereWasAnAnswer);
                }
            }
            if (this.orderByColumns != null || this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
                if (thereWasAnAnswer) {
                    bufferedRecordEnvironments.add(reader.getEnvironment());
                    currentRow++;
                } else {
                    hitTail = true;
                    currentRow = bufferedRecordEnvironments.size() + 1;
                }
            } else {
                if (thereWasAnAnswer) {
                    currentRow++;
                } else {
                    hitTail = true;
                }
            }
            return thereWasAnAnswer;
        }
    }

    private Map<String, Object> updateRecordEnvironment(boolean thereWasAnAnswer) {
        HashMap<String, Object> objectEnvironment = new HashMap<String, Object>();
        if (!thereWasAnAnswer) {
            recordEnvironment = null;
            return objectEnvironment;
        }
        for (int i = 0; i < queryEnvironment.size(); i++) {
            Object[] o = queryEnvironment.get(i);
            String key = (String) o[0];
            Object value = ((Expression) o[1]).eval(recordEnvironment);
            objectEnvironment.put(key.toUpperCase(), value);
        }
        for (int i = 0; i < usedColumns.size(); i++) {
            String key = usedColumns.get(i);
            key = key.toUpperCase();
            if (!objectEnvironment.containsKey(key)) {
                objectEnvironment.put(key, recordEnvironment.get(key));
            }
        }

        /*
         * Always include any group of rows so we have assembled so we can evaluate
         * any aggregate functions.
         */
        String key = "@GROUPROWS";
        Object groupRows = recordEnvironment.get(key);
        if (groupRows != null) {
            objectEnvironment.put(key, groupRows);
        }

        /*
         * Always include the data type converter object so we can correctly
         * convert data types when evaluating expressions such as MYDATE > '2012-06-31'.
         */
        key = "@STRINGCONVERTER";
        Object stringConverter = recordEnvironment.get(key);
        if (stringConverter != null) {
            objectEnvironment.put(key, stringConverter);
        }
        return objectEnvironment;
    }

    private boolean addDistinctEnvironment(Map<String, Object> objectEnvironment) {
        boolean isDistinct;

        /*
         * Create list of query values for this row, either for a simple
         * GROUP BY statement, or for a SELECT DISTINCT.
         */
        ArrayList<Object> environment;
        if (this.distinctColumns != null) {
            environment = new ArrayList<Object>(distinctColumns.size());
            for (int i = 0; i < distinctColumns.size(); i++) {
                Object value = distinctColumns.get(i).eval(objectEnvironment);
                environment.add(value);
            }
        } else {
            environment = new ArrayList<Object>(queryEnvironment.size());
            for (int i = 0; i < queryEnvironment.size(); i++) {
                Object[] o = queryEnvironment.get(i);
                Object value = ((Expression) o[1]).eval(objectEnvironment);
                environment.add(value);
            }
        }

        /*
         * Has this list of values been read before for this query?
         */
        isDistinct = distinctValues.add(environment);
        return isDistinct;
    }

    /**
     * Releases this
     * <code>ResultSet</code> object's database and JDBC resources immediately
     * instead of waiting for this to happen when it is automatically closed.
     * <p/>
     * <P><B>Note:</B> A
     * <code>ResultSet</code> object is automatically closed by the
     * <code>Statement</code> object that generated it when that
     * <code>Statement</code> object is closed, re-executed, or is used to
     * retrieve the next result from a sequence of multiple results. A
     * <code>ResultSet</code> object is also automatically closed when it is
     * garbage collected.
     *
     * @throws SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        isClosed = true;
        reader.close();
    }

    /**
     * Reports whether the last column read had a value of SQL
     * <code>NULL</code>. Note that you must first call one of the getter
     * methods on a column to try to read its value and then call the method
     * <code>wasNull</code> to see if the value read was SQL
     * <code>NULL</code>.
     *
     * @return <code>true</code> if the last column value read was SQL
     *         <code>NULL</code> and <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean wasNull() throws SQLException {
        if (lastIndexRead >= 0) {
            return getString(lastIndexRead) == null;
        } else {
            throw new SQLException("No previous getter method called");
        }
    }

    //======================================================================
    // Methods for accessing results by column index
    //======================================================================

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>String</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public String getString(int columnIndex) throws SQLException {
        // perform pre-accessor method processing
        preAccessor(columnIndex);
        // use CsvReader.getColumn(String) to retrieve the column
        if (columnIndex < 1 || columnIndex > this.queryEnvironment.size()) {
            throw new SQLException("Column not found: invalid index: " + columnIndex);
        }
        Object[] o = queryEnvironment.get(columnIndex - 1);
        try {
            return ((Expression) o[1]).eval(recordEnvironment).toString();
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>boolean</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>false</code>
     * @throws SQLException if a database access error occurs
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        return converter.parseBoolean(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>byte</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public byte getByte(int columnIndex) throws SQLException {
        return converter.parseByte(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>short</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public short getShort(int columnIndex) throws SQLException {
        return converter.parseShort(getString(columnIndex));
    }

    /**
     * Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>int</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public int getInt(int columnIndex) throws SQLException {
        return converter.parseInt(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>long</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public long getLong(int columnIndex) throws SQLException {
        return converter.parseLong(getString(columnIndex));
    }

    /**
     * Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>float</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public float getFloat(int columnIndex) throws SQLException {
        return converter.parseFloat(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>double</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public double getDouble(int columnIndex) throws SQLException {
        return converter.parseDouble(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.BigDecimal</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param scale       the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     * @deprecated
     */
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException {
        // let getBigDecimal(int) handle this for now
        return getBigDecimal(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>byte</code> array in the Java programming language. The bytes
     * represent the raw values returned by the driver.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        return converter.parseBytes(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) getObject(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) getObject(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) getObject(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of ASCII characters. The value
     * can then be read in chunks from the stream. This method is particularly
     * suitable for retrieving large <char>LONGVARCHAR</char> values. The JDBC
     * driver will do any necessary conversion from the database format into
     * ASCII.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return
     * <code>0</code> when the method
     * <code>InputStream.available</code> is called whether there is data
     * available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a
     *         stream of one-byte ASCII characters; if the value is
     *         SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return converter.parseAsciiStream(getString(columnIndex));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as as a stream of two-byte Unicode
     * characters. The first byte is the high byte; the second byte is the low
     * byte.
     * <p/>
     * The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large
     * <code>LONGVARCHAR</code>values. The JDBC driver will do any necessary
     * conversion from the database format into Unicode.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return
     * <code>0</code> when the method
     * <code>InputStream.available</code> is called, whether there is data
     * available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a
     *         stream of two-byte Unicode characters; if the value is
     *         SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     * @deprecated use <code>getCharacterStream</code> in place of
     *             <code>getUnicodeStream</code>
     */
    @Deprecated
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        // delegate to getAsciiStream(int)
        return getAsciiStream(columnIndex);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a binary stream of uninterpreted bytes.
     * The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large
     * <code>LONGVARBINARY</code> values.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return
     * <code>0</code> when the method
     * <code>InputStream.available</code> is called whether there is data
     * available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a
     *         stream of uninterpreted bytes; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        // delegate to getAsciiStream(int)
        return getAsciiStream(columnIndex);
    }

    //======================================================================
    // Methods for accessing results by column name
    //======================================================================

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>String</code> in the Java programming language.
     *
     * @param columnName the SQL name (or alias) of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public String getString(String columnName) throws SQLException {
        return getString(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>boolean</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>false</code>
     * @throws SQLException if a database access error occurs
     */
    public boolean getBoolean(String columnName) throws SQLException {
        return getBoolean(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>byte</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public byte getByte(String columnName) throws SQLException {
        return getByte(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>short</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public short getShort(String columnName) throws SQLException {
        return getShort(findColumn(columnName));
    }

    /**
     * Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>int</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public int getInt(String columnName) throws SQLException {
        return getInt(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>long</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public long getLong(String columnName) throws SQLException {
        return getLong(findColumn(columnName));
    }

    /**
     * Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>float</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public float getFloat(String columnName) throws SQLException {
        return getFloat(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>double</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>0</code>
     * @throws SQLException if a database access error occurs
     */
    public double getDouble(String columnName) throws SQLException {
        return getDouble(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @param scale      the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     * @deprecated
     */
    @Deprecated
    public BigDecimal getBigDecimal(String columnName, int scale)
            throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>byte</code> array in the Java programming language. The bytes
     * represent the raw values returned by the driver.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public byte[] getBytes(String columnName) throws SQLException {
        return getBytes(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public Date getDate(String columnName) throws SQLException {
        return getDate(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public Time getTime(String columnName) throws SQLException {
        return getTime(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object.
     *
     * @param columnName the SQL name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     *         value returned is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public Timestamp getTimestamp(String columnName) throws SQLException {
        return getTimestamp(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of ASCII characters. The value
     * can then be read in chunks from the stream. This method is particularly
     * suitable for retrieving large
     * <code>LONGVARCHAR</code> values. The JDBC driver will do any necessary
     * conversion from the database format into ASCII.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return
     * <code>0</code> when the method
     * <code>available</code> is called whether there is data available or not.
     *
     * @param columnName the SQL name of the column
     * @return a Java input stream that delivers the database column value as a
     *         stream of one-byte ASCII characters. If the value is
     *         SQL <code>NULL</code>, the value returned is <code>null</code>.
     * @throws SQLException if a database access error occurs
     */
    public InputStream getAsciiStream(String columnName) throws SQLException {
        return getAsciiStream(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of two-byte Unicode characters.
     * The first byte is the high byte; the second byte is the low byte.
     * <p/>
     * The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large
     * <code>LONGVARCHAR</code> values. The JDBC technology-enabled driver will
     * do any necessary conversion from the database format into Unicode.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return
     * <code>0</code> when the method
     * <code>InputStream.available</code> is called, whether there is data
     * available or not.
     *
     * @param columnName the SQL name of the column
     * @return a Java input stream that delivers the database column value as a
     *         stream of two-byte Unicode characters. If the value is
     *         SQL <code>NULL</code>, the value returned is <code>null</code>.
     * @throws SQLException if a database access error occurs
     * @deprecated use <code>getCharacterStream</code> instead
     */
    @Deprecated
    public InputStream getUnicodeStream(String columnName) throws SQLException {
        return getUnicodeStream(findColumn(columnName));
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a stream of uninterpreted
     * <code>byte</code>s. The value can then be read in chunks from the stream.
     * This method is particularly suitable for retrieving large
     * <code>LONGVARBINARY</code> values.
     * <p/>
     * <P><B>Note:</B> All the data in the returned stream must be read prior to
     * getting the value of any other column. The next call to a getter method
     * implicitly closes the stream. Also, a stream may return
     * <code>0</code> when the method
     * <code>available</code> is called whether there is data available or not.
     *
     * @param columnName the SQL name of the column
     * @return a Java input stream that delivers the database column value as a
     *         stream of uninterpreted bytes; if the value is SQL <code>NULL</code>, the
     *         result is <code>null</code>
     * @throws SQLException if a database access error occurs
     */
    public InputStream getBinaryStream(String columnName) throws SQLException {
        return getBinaryStream(findColumn(columnName));
    }

    //=====================================================================
    // Advanced features:
    //=====================================================================

    /**
     * Retrieves the first warning reported by calls on this
     * <code>ResultSet</code> object. Subsequent warnings on this
     * <code>ResultSet</code> object will be chained to the
     * <code>SQLWarning</code> object that this method returns.
     * <p/>
     * <P>The warning chain is automatically cleared each time a new row is
     * read. This method may not be called on a
     * <code>ResultSet</code> object that has been closed; doing so will cause
     * an
     * <code>SQLException</code> to be thrown. <P> <B>Note:</B> This warning
     * chain only covers warnings caused by
     * <code>ResultSet</code> methods. Any warning caused by
     * <code>Statement</code> methods (such as reading OUT parameters) will be
     * chained on the
     * <code>Statement</code> object.
     *
     * @return the first <code>SQLWarning</code> object reported or
     *         <code>null</code> if there are none
     * @throws SQLException if a database access error occurs or this method
     *                      is called on a closed result set
     */
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    /**
     * Clears all warnings reported on this
     * <code>ResultSet</code> object. After this method is called, the method
     * <code>getWarnings</code> returns
     * <code>null</code> until a new warning is reported for this
     * <code>ResultSet</code> object.
     *
     * @throws SQLException if a database access error occurs
     */
    public void clearWarnings() throws SQLException {
    }

    /**
     * Retrieves the name of the SQL cursor used by this
     * <code>ResultSet</code> object.
     * <p/>
     * <P>In SQL, a result table is retrieved through a cursor that is named.
     * The current row of a result set can be updated or deleted using a
     * positioned update/delete statement that references the cursor name. To
     * insure that the cursor has the proper isolation level to support update,
     * the cursor's
     * <code>SELECT</code> statement should be of the form
     * <code>SELECT FOR UPDATE</code>. If
     * <code>FOR UPDATE</code> is omitted, the positioned updates may fail.
     * <p/>
     * <P>The JDBC API supports this SQL feature by providing the name of the
     * SQL cursor used by a
     * <code>ResultSet</code> object. The current row of a
     * <code>ResultSet</code> object is also the current row of this SQL cursor.
     * <p/>
     * <P><B>Note:</B> If positioned update is not supported, a
     * <code>SQLException</code> is thrown.
     *
     * @return the SQL name for this <code>ResultSet</code> object's cursor
     * @throws SQLException if a database access error occurs
     */
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getCursorName() unsupported");
    }

    /**
     * Retrieves the number, types and properties of this
     * <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     * @throws SQLException if a database access error occurs
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        if (resultSetMetaData == null) {
            String[] readerTypeNames = reader.getColumnTypes();
            String[] readerColumnNames = reader.getColumnNames();
            int[] readerColumnSizes = reader.getColumnSizes();
            String tableAlias = reader.getTableAlias();
            int columnCount = queryEnvironment.size();
            String[] columnNames = new String[columnCount];
            String[] columnLabels = new String[columnCount];
            int[] columnSizes = new int[columnCount];
            String[] typeNames = new String[columnCount];

            /*
             * Create a record containing dummy values.
             */
            HashMap<String, Object> env = new HashMap<String, Object>();
            for (int i = 0; i < readerTypeNames.length; i++) {
                Object literal = StringConverter.getLiteralForTypeName(readerTypeNames[i]);
                String columnName = readerColumnNames[i].toUpperCase();
                env.put(columnName, literal);
                if (tableAlias != null) {
                    env.put(tableAlias + "." + columnName, literal);
                }
            }
            if (converter != null) {
                env.put("@STRINGCONVERTER", converter);
            }

            for (int i = 0; i < columnCount; i++) {
                Object[] o = queryEnvironment.get(i);
                columnNames[i] = (String) o[0];
                columnLabels[i] = columnNames[i];

                /*
                 * Evaluate each expression to determine what data type it returns.
                 */
                Object result = null;
                try {
                    Expression expr = ((Expression) o[1]);

                    int columnSize = DataReader.DEFAULT_COLUMN_SIZE;
                    if (expr instanceof ColumnName) {
                        String usedColumn = expr.usedColumns().get(0);
                        for (int k = 0; k < readerColumnNames.length; k++) {
                            if (usedColumn.equalsIgnoreCase(readerColumnNames[k])) {
                                columnSize = readerColumnSizes[k];
                                break;
                            }
                        }
                    }
                    columnSizes[i] = columnSize;
                    result = expr.eval(env);
                } catch (NullPointerException e) {
                    /* Expression is invalid */
                    // TODO: should we throw an SQLException here?
                }
                if (result != null) {
                    typeNames[i] = StringConverter.getTypeNameForLiteral(result);
                } else {
                    typeNames[i] = "expression";
                }
            }
            resultSetMetaData = new CsvResultSetMetaData(tableName, columnNames, columnLabels, typeNames,
                    columnSizes);
        }
        return resultSetMetaData;
    }

    /**
     * <p>Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language.
     * <p/>
     * <p>This method will return the value of the given column as a Java
     * object. The type of the Java object will be the default Java object type
     * corresponding to the column's SQL type, following the mapping for
     * built-in types specified in the JDBC specification. If the value is an
     * SQL
     * <code>NULL</code>, the driver returns a Java
     * <code>null</code>.
     * <p/>
     * <p>This method may also be used to read datatabase-specific abstract data
     * types.
     * <p/>
     * In the JDBC 2.0 API, the behaviour of method
     * <code>getObject</code> is extended to materialize data of SQL
     * user-defined types. When a column contains a structured or distinct
     * value, the behaviour of this method is as if it were a call to:
     * <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.lang.Object</code> holding the column value
     * @throws SQLException if a database access error occurs
     */
    public Object getObject(int columnIndex) throws SQLException {
        Object[] o = queryEnvironment.get(columnIndex - 1);
        try {
            return ((Expression) o[1]).eval(recordEnvironment);
        } catch (NullPointerException e) {
            return null;
        }
    }

    /**
     * <p>Gets the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language.
     * <p/>
     * <p>This method will return the value of the given column as a Java
     * object. The type of the Java object will be the default Java object type
     * corresponding to the column's SQL type, following the mapping for
     * built-in types specified in the JDBC specification. If the value is an
     * SQL
     * <code>NULL</code>, the driver returns a Java
     * <code>null</code>. <P> This method may also be used to read
     * datatabase-specific abstract data types. <P> In the JDBC 2.0 API, the
     * behavior of the method
     * <code>getObject</code> is extended to materialize data of SQL
     * user-defined types. When a column contains a structured or distinct
     * value, the behavior of this method is as if it were a call to:
     * <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
     * @param columnName the SQL name of the column
     * @return a <code>java.lang.Object</code> holding the column value
     * @throws SQLException if a database access error occurs
     */
    public Object getObject(String columnName) throws SQLException {
        return getObject(findColumn(columnName));
    }

    //--------------------------JDBC 2.0-----------------------------------
    //---------------------------------------------------------------------
    // Getters and Setters
    //---------------------------------------------------------------------

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the column
     *         value; if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language.
     * @throws SQLException if a database access error occurs
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return (str == null) ? null : new StringReader(str);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     *
     * @param columnName the name of the column
     * @return a <code>java.io.Reader</code> object that contains the column
     *         value; if the value is SQL <code>NULL</code>, the value returned is
     *         <code>null</code> in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Reader getCharacterStream(String columnName) throws SQLException {
        String str = getString(columnName);
        return (str == null) ? null : new StringReader(str);
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> with full precision.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value (full precision); if the value is
     *         SQL <code>NULL</code>, the value returned is <code>null</code> in the
     *         Java programming language.
     * @throws SQLException if a database access error occurs
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        BigDecimal retval = null;
        String str = getString(columnIndex);
        if (str != null) {
            try {
                retval = new BigDecimal(str);
            } catch (NumberFormatException e) {
                throw new SQLException("Could not convert '" + str + "' to "
                        + "a java.math.BigDecimal object");
            }
        }
        return retval;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> with full precision.
     *
     * @param columnName the column name
     * @return the column value (full precision); if the value is
     *         SQL <code>NULL</code>, the value returned is <code>null</code> in the
     *         Java programming language.
     * @throws SQLException if a database access error occurs
     */
    public BigDecimal getBigDecimal(String columnName) throws SQLException {
        return getBigDecimal(findColumn(columnName));
    }

    //---------------------------------------------------------------------
    // Traversal/Positioning
    //---------------------------------------------------------------------

    /**
     * Retrieves whether the cursor is before the first row in this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is before the first row;
     *         <code>false</code> if the cursor is at any other position or the result
     *         set contains no rows
     * @throws SQLException if a database access error occurs
     */
    public boolean isBeforeFirst() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            return currentRow == 0;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.isBeforeFirst() unsupported");
        }
    }

    /**
     * Retrieves whether the cursor is after the last row in this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is after the last row;
     *         <code>false</code> if the cursor is at any other position or the result
     *         set contains no rows
     * @throws SQLException if a database access error occurs
     */
    public boolean isAfterLast() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            return currentRow == bufferedRecordEnvironments.size() + 1;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.isAfterLast() unsupported");
        }
    }

    /**
     * Retrieves whether the cursor is on the first row of this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on the first row;
     *         <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isFirst() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            return currentRow == 1;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.isFirst() unsupported");
        }
    }

    /**
     * Retrieves whether the cursor is on the last row of this
     * <code>ResultSet</code> object. Note: Calling the method
     * <code>isLast</code> may be expensive because the JDBC driver might need
     * to fetch ahead one row in order to determine whether the current row is
     * the last row in the result set.
     *
     * @return <code>true</code> if the cursor is on the last row;
     *         <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     */
    public boolean isLast() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            if (!hitTail && currentRow != 0) {
                next();
                previous();
            }
            return (currentRow == bufferedRecordEnvironments.size());
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.isLast() unsupported");
        }
    }

    /**
     * Moves the cursor to the front of this
     * <code>ResultSet</code> object, just before the first row. This method has
     * no effect if the result set contains no rows.
     *
     * @throws SQLException if a database access error occurs or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public void beforeFirst() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            first();
            previous();
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.beforeFirst() unsupported");
        }
    }

    /**
     * Moves the cursor to the end of this
     * <code>ResultSet</code> object, just after the last row. This method has
     * no effect if the result set contains no rows.
     *
     * @throws SQLException if a database access error occurs or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public void afterLast() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            while (next()) {
                ;
            }
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.afterLast() unsupported");
        }
    }

    /**
     * Moves the cursor to the first row in this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     *         <code>false</code> if there are no rows in the result set
     * @throws SQLException if a database access error occurs or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean first() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            currentRow = 0;
            boolean thereWasAnAnswer = next();
            updateRecordEnvironment(thereWasAnAnswer);
            return thereWasAnAnswer;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.first() unsupported");
        }
    }

    /**
     * Moves the cursor to the last row in this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     *         <code>false</code> if there are no rows in the result set
     * @throws SQLException if a database access error occurs or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean last() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            afterLast();
            previous();
            return (this.bufferedRecordEnvironments.size() != 0);
        } else {
            throw new UnsupportedOperationException("ResultSet.last() unsupported");
        }
    }

    /**
     * Retrieves the current row number. The first row is number 1, the second
     * number 2, and so on.
     *
     * @return the current row number; <code>0</code> if there is no current row
     * @throws SQLException if a database access error occurs
     */
    public int getRow() throws SQLException {
        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            return currentRow;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.getRow() unsupported");
        }
    }

    /**
     * Moves the cursor to the given row number in this
     * <code>ResultSet</code> object.
     * <p/>
     * <p>If the row number is positive, the cursor moves to the given row
     * number with respect to the beginning of the result set. The first row is
     * row 1, the second is row 2, and so on.
     * <p/>
     * <p>If the given row number is negative, the cursor moves to an absolute
     * row position with respect to the end of the result set. For example,
     * calling the method
     * <code>absolute(-1)</code> positions the cursor on the last row; calling
     * the method
     * <code>absolute(-2)</code> moves the cursor to the next-to-last row, and
     * so on.
     * <p/>
     * <p>An attempt to position the cursor beyond the first/last row in the
     * result set leaves the cursor before the first row or after the last row.
     * <p/>
     * <p><B>Note:</B> Calling
     * <code>absolute(1)</code> is the same as calling
     * <code>first()</code>. Calling
     * <code>absolute(-1)</code> is the same as calling
     * <code>last()</code>.
     *
     * @param row the number of the row to which the cursor should move. A
     *            positive number indicates the row number counting from the beginning of
     *            the result set; a negative number indicates the row number counting from
     *            the end of the result set
     * @return <code>true</code> if the cursor is on the result set;
     *         <code>false</code> otherwise
     * @throws SQLException if a database access error occurs, or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean absolute(int row) throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            boolean found;
            if (row < 0) {
                last();
                row = currentRow + row + 1;
            } else {
                // this is a no-op if we have already buffered enough lines.
                while ((bufferedRecordEnvironments.size() < row) && next()) {
                    ;
                }
            }
            if (row <= 0) {
                found = false;
                currentRow = 0;
            } else if (row > bufferedRecordEnvironments.size()) {
                found = false;
                currentRow = bufferedRecordEnvironments.size() + 1;
            } else {
                found = true;
                currentRow = row;
                recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
            }
            updateRecordEnvironment(found);
            return found;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.absolute() unsupported");
        }
    }

    /**
     * Moves the cursor a relative number of rows, either positive or negative.
     * Attempting to move beyond the first/last row in the result set positions
     * the cursor before/after the the first/last row. Calling
     * <code>relative(0)</code> is valid, but does not change the cursor
     * position.
     * <p/>
     * <p>Note: Calling the method
     * <code>relative(1)</code> is identical to calling the method
     * <code>next()</code> and calling the method
     * <code>relative(-1)</code> is identical to calling the method
     * <code>previous()</code>.
     *
     * @param rows an <code>int</code> specifying the number of rows to move
     *             from the current row; a positive number moves the cursor forward; a
     *             negative number moves the cursor backward
     * @return <code>true</code> if the cursor is on a row; <code>false</code>
     *         otherwise
     * @throws SQLException if a database access error occurs, there is no
     *                      current row, or the result set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean relative(int rows) throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            if (currentRow + rows >= 0) {
                return absolute(currentRow + rows);
            }
            currentRow = 0;
            updateRecordEnvironment(false);
            return false;
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.relative() unsupported");
        }
    }

    /**
     * Moves the cursor to the previous row in this
     * <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     *         <code>false</code> if it is off the result set
     * @throws SQLException if a database access error occurs or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code>
     */
    public boolean previous() throws SQLException {

        checkOpen();

        if (this.isScrollable == ResultSet.TYPE_SCROLL_SENSITIVE) {
            if (currentRow > 1) {
                currentRow--;
                recordEnvironment = bufferedRecordEnvironments.get(currentRow - 1);
                updateRecordEnvironment(true);
                return true;
            } else {
                currentRow = 0;
                recordEnvironment = null;
                updateRecordEnvironment(false);
                return false;
            }
        } else {
            throw new UnsupportedOperationException(
                    "ResultSet.previous() unsupported");
        }
    }

    //---------------------------------------------------------------------
    // Properties
    //---------------------------------------------------------------------

    /**
     * Gives a hint as to the direction in which the rows in this
     * <code>ResultSet</code> object will be processed. The initial value is
     * determined by the
     * <code>Statement</code> object that produced this
     * <code>ResultSet</code> object. The fetch direction may be changed at any
     * time.
     *
     * @param direction an <code>int</code> specifying the suggested fetch
     *                  direction; one of <code>ResultSet.FETCH_FORWARD</code>,
     *                  <code>ResultSet.FETCH_REVERSE</code>, or
     *                  <code>ResultSet.FETCH_UNKNOWN</code>
     * @throws SQLException if a database access error occurs or the result
     *                      set type is <code>TYPE_FORWARD_ONLY</code> and the fetch direction is
     *                      not <code>FETCH_FORWARD</code>
     */
    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.setFetchDirection(int) unsupported");
    }

    /**
     * Retrieves the fetch direction for this
     * <code>ResultSet</code> object.
     *
     * @return the current fetch direction for this <code>ResultSet</code>
     *         object
     * @throws SQLException if a database access error occurs
     * @see #setFetchDirection
     */
    public int getFetchDirection() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getFetchDirection() unsupported");
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should be
     * fetched from the database when more rows are needed for this
     * <code>ResultSet</code> object. If the fetch size specified is zero, the
     * JDBC driver ignores the value and is free to make its own best guess as
     * to what the fetch size should be. The default value is set by the
     * <code>Statement</code> object that created the result set. The fetch size
     * may be changed at any time.
     *
     * @param rows the number of rows to fetch
     * @throws SQLException if a database access error occurs or the
     *                      condition <code>0 <= rows <= this.getMaxRows()</code> is not satisfied
     */
    public void setFetchSize(int rows) throws SQLException {
        fetchSize = rows;
    }

    /**
     * Retrieves the fetch size for this
     * <code>ResultSet</code> object.
     *
     * @return the current fetch size for this <code>ResultSet</code> object
     * @throws SQLException if a database access error occurs
     * @see #setFetchSize
     */
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    /**
     * Retrieves the type of this
     * <code>ResultSet</code> object. The type is determined by the
     * <code>Statement</code> object that created the result set.
     *
     * @return <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *         <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>,        or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException if a database access error occurs
     */
    public int getType() throws SQLException {
        return isScrollable;
    }

    /**
     * Retrieves the concurrency mode of this
     * <code>ResultSet</code> object. The concurrency used is determined by the
     * <code>Statement</code> object that created the result set.
     *
     * @return the concurrency type, either
     *         <code>ResultSet.CONCUR_READ_ONLY</code>        or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @throws SQLException if a database access error occurs
     */
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    //---------------------------------------------------------------------
    // Updates
    //---------------------------------------------------------------------

    /**
     * Retrieves whether the current row has been updated. The value returned
     * depends on whether or not the result set can detect updates.
     *
     * @return <code>true</code> if both (1) the row has been visibly updated by
     *         the owner or another and (2) updates are detected
     * @throws SQLException if a database access error occurs
     * @see DatabaseMetaData#updatesAreDetected
     */
    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.rowUpdated() unsupported");
    }

    /**
     * Retrieves whether the current row has had an insertion. The value
     * returned depends on whether or not this
     * <code>ResultSet</code> object can detect visible inserts.
     *
     * @return <code>true</code> if a row has had an insertion and insertions
     *         are detected; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     * @see DatabaseMetaData#insertsAreDetected
     */
    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.rowInserted() unsupported");
    }

    /**
     * Retrieves whether a row has been deleted. A deleted row may leave a
     * visible "hole" in a result set. This method can be used to detect holes
     * in a result set. The value returned depends on whether or not this
     * <code>ResultSet</code> object can detect deletions.
     *
     * @return <code>true</code> if a row was deleted and deletions are
     *         detected; <code>false</code> otherwise
     * @throws SQLException if a database access error occurs
     * @see DatabaseMetaData#deletesAreDetected
     */
    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.rowDeleted() unsupported");
    }

    /**
     * Gives a nullable column a null value.
     * <p/>
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @throws SQLException if a database access error occurs
     */
    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateNull() unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>boolean</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateBoolean() unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>byte</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateByte() unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>short</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateShort() unsupported");
    }

    /**
     * Updates the designated column with an
     * <code>int</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateInt() unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>long</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateLong(int, long) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>float</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateFloat(int, float) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>double</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateDouble(int, double) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.math.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater
     * methods do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateBigDecimal(int, BigDecimal) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateString(int, String) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>byte</code> array value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods
     * do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateBytes(int, byte[]) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.Date</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods
     * do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateDate(int, Date) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods
     * do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateTime(int, Time) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater
     * methods do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateTimestamp(int, Timestamp) unsupported");
    }

    /**
     * Updates the designated column with an ascii stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException if a database access error occurs
     */
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateAsciiStream "
                + "(int, InputStream, int) unsupported");
    }

    /**
     * Updates the designated column with a binary stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException if a database access error occurs
     */
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateBinaryStream"
                + "(int, InputStream, int) unsupported");
    }

    /**
     * Updates the designated column with a character stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException if a database access error occurs
     */
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateCharacterStr"
                + "eam(int, Reader, int) unsupported");
    }

    /**
     * Updates the designated column with an
     * <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param scale       for <code>java.sql.Types.DECIMA</code>     *  or <code>java.sql.Types.NUMERIC</code> types, this is the number
     *                    of digits after the decimal point. For all other types this value will be
     *                    ignored.
     * @throws SQLException if a database access error occurs
     */
    public void updateObject(int columnIndex, Object x, int scale)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.udpateObject(int, Object) unsupported");
    }

    /**
     * Updates the designated column with an
     * <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateObject(int, Object, int) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>null</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @throws SQLException if a database access error occurs
     */
    public void updateNull(String columnName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateNull(String) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>boolean</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateBoolean(String columnName, boolean x)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateBoolean(String, boolean) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>byte</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateByte(String columnName, byte x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateByte(String, byte) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>short</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateShort(String columnName, short x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateShort(String, short) unsupported");
    }

    /**
     * Updates the designated column with an
     * <code>int</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateInt(String columnName, int x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateInt(String, int) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>long</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateLong(String columnName, long x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateLong(String, long) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>float	</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateFloat(String columnName, float x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateFloat(String, float) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>double</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateDouble(String columnName, double x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateDouble(String, double) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater
     * methods do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateBigDecimal(String columnName, BigDecimal x)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateBigDecimal(String, BigDecimal) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateString(String columnName, String x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateString(String, String) unsupported");
    }

    /**
     * Updates the designated column with a byte array value.
     * <p/>
     * The updater methods are used to update column values in the current row
     * or the insert row. The updater methods do not update the underlying
     * database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateBytes(String columnName, byte[] x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateBytes(String, byte[]) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.Date</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods
     * do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateDate(String columnName, Date x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateDate(String, Date) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods
     * do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateTime(String columnName, Time x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateTime(String, Time) unsupported");
    }

    /**
     * Updates the designated column with a
     * <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater
     * methods do not update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateTimestamp(String columnName, Timestamp x)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateTimestamp(String, Timestamp) unsupported");
    }

    /**
     * Updates the designated column with an ascii stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @param length     the length of the stream
     * @throws SQLException if a database access error occurs
     */
    public void updateAsciiStream(String columnName, InputStream x, int length)
            throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateAsciiStream"
                + "(String, InputStream, int) unsupported");
    }

    /**
     * Updates the designated column with a binary stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @param length     the length of the stream
     * @throws SQLException if a database access error occurs
     */
    public void updateBinaryStream(String columnName, InputStream x, int length)
            throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateBinaryStream"
                + "(String, InputStream, int) unsupported");
    }

    /**
     * Updates the designated column with a character stream value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param reader     the <code>java.io.Reader</code> object containing the new
     *                   column value
     * @param length     the length of the stream
     * @throws SQLException if a database access error occurs
     */
    public void updateCharacterStream(String columnName, Reader reader,
                                      int length) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateCharacterStr"
                + "eam(String, Reader, int) unsupported");
    }

    /**
     * Updates the designated column with an
     * <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @param scale      for <code>java.sql.Types.DECIMAL</code>     *  or <code>java.sql.Types.NUMERIC</code> types, this is the number
     *                   of digits after the decimal point. For all other types this value will be
     *                   ignored.
     * @throws SQLException if a database access error occurs
     */
    public void updateObject(String columnName, Object x, int scale)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateObject(String, Object, int) unsupported");
    }

    /**
     * Updates the designated column with an
     * <code>Object</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not
     * update the underlying database; instead the
     * <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName the name of the column
     * @param x          the new column value
     * @throws SQLException if a database access error occurs
     */
    public void updateObject(String columnName, Object x) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateObject(String, Object) unsupported");
    }

    /**
     * Inserts the contents of the insert row into this
     * <code>ResultSet</code> object and into the database. The cursor must be
     * on the insert row when this method is called.
     *
     * @throws SQLException if a database access error occurs, if this method
     *                      is called when the cursor is not on the insert row, or if not all of
     *                      non-nullable columns in the insert row have been given a value
     */
    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.insertRow() unsupported");
    }

    /**
     * Updates the underlying database with the new contents of the current row
     * of this
     * <code>ResultSet</code> object. This method cannot be called when the
     * cursor is on the insert row.
     *
     * @throws SQLException if a database access error occurs or if this
     *                      method is called when the cursor is on the insert row
     */
    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.updateRow() unsupported");
    }

    /**
     * Deletes the current row from this
     * <code>ResultSet</code> object and from the underlying database. This
     * method cannot be called when the cursor is on the insert row.
     *
     * @throws SQLException if a database access error occurs or if this
     *                      method is called when the cursor is on the insert row
     */
    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.deleteRow() unsupported");
    }

    /**
     * Refreshes the current row with its most recent value in the database.
     * This method cannot be called when the cursor is on the insert row.
     * <p/>
     * <P>The
     * <code>refreshRow</code> method provides a way for an application to
     * explicitly tell the JDBC driver to refetch a row(s) from the database. An
     * application may want to call
     * <code>refreshRow</code> when caching or prefetching is being done by the
     * JDBC driver to fetch the latest value of a row from the database. The
     * JDBC driver may actually refresh multiple rows at once if the fetch size
     * is greater than one.
     * <p/>
     * <P> All values are refetched subject to the transaction isolation level
     * and cursor sensitivity. If
     * <code>refreshRow</code> is called after calling an updater method, but
     * before calling the method
     * <code>updateRow</code>, then the updates made to the row are lost.
     * Calling the method
     * <code>refreshRow</code> frequently will likely slow performance.
     *
     * @throws SQLException if a database access error occurs or if this
     *                      method is called when the cursor is on the insert row
     */
    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.refreshRow() unsupported");
    }

    /**
     * Cancels the updates made to the current row in this
     * <code>ResultSet</code> object. This method may be called after calling an
     * updater method(s) and before calling the method
     * <code>updateRow</code> to roll back the updates made to a row. If no
     * updates have been made or
     * <code>updateRow</code> has already been called, this method has no
     * effect.
     *
     * @throws SQLException if a database access error occurs or if this
     *                      method is called when the cursor is on the insert row
     */
    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.cancelRowUpdates() unsupported");
    }

    /**
     * Moves the cursor to the insert row. The current cursor position is
     * remembered while the cursor is positioned on the insert row.
     * <p/>
     * The insert row is a special row associated with an updatable result set.
     * It is essentially a buffer where a new row may be constructed by calling
     * the updater methods prior to inserting the row into the result set.
     * <p/>
     * Only the updater, getter, and
     * <code>insertRow</code> methods may be called when the cursor is on the
     * insert row. All of the columns in a result set must be given a value each
     * time this method is called before calling
     * <code>insertRow</code>. An updater method must be called before a getter
     * method can be called on a column value.
     *
     * @throws SQLException if a database access error occurs or the result
     *                      set is not updatable
     */
    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.moveToInsertRow() unsupported");
    }

    /**
     * Moves the cursor to the remembered cursor position, usually the current
     * row. This method has no effect if the cursor is not on the insert row.
     *
     * @throws SQLException if a database access error occurs or the result
     *                      set is not updatable
     */
    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.moveToeCurrentRow() unsupported");
    }

    /**
     * Retrieves the
     * <code>Statement</code> object that produced this
     * <code>ResultSet</code> object. If the result set was generated some other
     * way, such as by a
     * <code>DatabaseMetaData</code> method, this method returns
     * <code>null</code>.
     *
     * @return the <code>Statment</code> object that produced      * this <code>ResultSet</code> object or <code>null</code> if the
     *         result set was produced some other way
     * @throws SQLException if a database access error occurs
     */
    public Statement getStatement() throws SQLException {
        return statement;
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language. If the value is an
     * SQL
     * <code>NULL</code>, the driver returns a Java
     * <code>null</code>. This method uses the given
     * <code>Map</code> object for the custom mapping of the SQL structured or
     * distinct type that is being retrieved.
     *
     * @param i   the first column is 1, the second is 2, ...
     * @param map a <code>java.util.Map</code> object that contains the mapping
     *            from SQL type names to classes in the Java programming language
     * @return an <code>Object</code> in the Java programming language
     *         representing the SQL value
     * @throws SQLException if a database access error occurs
     */
    public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getObject(int, Map) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>Ref</code> object in the Java programming language.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return a <code>Ref</code> object representing an SQL <code>REF</code>
     *         value
     * @throws SQLException if a database access error occurs
     */
    public Ref getRef(int i) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getRef(int) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>Blob</code> object in the Java programming language.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return a <code>Blob</code> object representing the SQL <code>BLOB</code>
     *         value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Blob getBlob(int i) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getBlob(int) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>Clob</code> object in the Java programming language.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return a <code>Clob</code> object representing the SQL <code>CLOB</code>
     *         value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Clob getClob(int i) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getClob(int) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>Array</code> object in the Java programming language.
     *
     * @param i the first column is 1, the second is 2, ...
     * @return an <code>Array</code> object representing the SQL
     *         <code>ARRAY</code> value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Array getArray(int i) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getArray(int) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>Object</code> in the Java programming language. If the value is an
     * SQL
     * <code>NULL</code>, the driver returns a Java
     * <code>null</code>. This method uses the specified
     * <code>Map</code> object for custom mapping if appropriate.
     *
     * @param colName the name of the column from which to retrieve the value
     * @param map     a <code>java.util.Map</code> object that contains the mapping
     *                from SQL type names to classes in the Java programming language
     * @return an <code>Object</code> representing the SQL value in the
     *         specified column
     * @throws SQLException if a database access error occurs
     */
    public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getObject(String, Map) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>Ref</code> object in the Java programming language.
     *
     * @param colName the column name
     * @return a <code>Ref</code> object representing the SQL <code>REF</code>
     *         value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Ref getRef(String colName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getRef(String) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>Blob</code> object in the Java programming language.
     *
     * @param colName the name of the column from which to retrieve the value
     * @return a <code>Blob</code> object representing the SQL <code>BLOB</code>
     *         value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Blob getBlob(String colName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getBlob(String) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>Clob</code> object in the Java programming language.
     *
     * @param colName the name of the column from which to retrieve the value
     * @return a <code>Clob</code> object representing the SQL <code>CLOB</code>
     *         value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Clob getClob(String colName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getClob(String) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as an
     * <code>Array</code> object in the Java programming language.
     *
     * @param colName the name of the column from which to retrieve the value
     * @return an <code>Array</code> object representing the SQL
     *         <code>ARRAY</code> value in the specified column
     * @throws SQLException if a database access error occurs
     */
    public Array getArray(String colName) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getArray(String) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language. This
     * method uses the given calendar to construct an appropriate millisecond
     * value for the date if the underlying database does not store timezone
     * information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object to use in
     *                    constructing the date
     * @return the column value as a <code>java.sql.Date</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getDate(int, Calendar) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Date</code> object in the Java programming language. This
     * method uses the given calendar to construct an appropriate millisecond
     * value for the date if the underlying database does not store timezone
     * information.
     *
     * @param columnName the SQL name of the column from which to retrieve the
     *                   value
     * @param cal        the <code>java.util.Calendar</code> object to use in
     *                   constructing the date
     * @return the column value as a <code>java.sql.Date</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Date getDate(String columnName, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getDate(String, Calendar) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language. This
     * method uses the given calendar to construct an appropriate millisecond
     * value for the time if the underlying database does not store timezone
     * information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object to use in
     *                    constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getTime(int, Calendar) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language. This
     * method uses the given calendar to construct an appropriate millisecond
     * value for the time if the underlying database does not store timezone
     * information.
     *
     * @param columnName the SQL name of the column
     * @param cal        the <code>java.util.Calendar</code> object to use in
     *                   constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Time getTime(String columnName, Calendar cal) throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getTime(String, Calendar) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Timestamp</code> object in the Java programming language.
     * This method uses the given calendar to construct an appropriate
     * millisecond value for the timestamp if the underlying database does not
     * store timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object to use in
     *                    constructing the timestamp
     * @return the column value as a <code>java.sql.Timestamp</code> object; if
     *         the value is SQL <code>NULL</code>, the value returned
     *         is <code>null</code> in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getTimestamp(int, Calendar) unsupported");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a
     * <code>java.sql.Time</code> object in the Java programming language. This
     * method uses the given calendar to construct an appropriate millisecond
     * value for the time if the underlying database does not store timezone
     * information.
     *
     * @param columnName the SQL name of the column
     * @param cal        the <code>java.util.Calendar</code> object to use in
     *                   constructing the time
     * @return the column value as a <code>java.sql.Time</code> object; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     *         in the Java programming language
     * @throws SQLException if a database access error occurs
     */
    public Timestamp getTimestamp(String columnName, Calendar cal)
            throws SQLException {
        throw new UnsupportedOperationException(
                "ResultSet.getTimestamp(String, Calendar) unsupported");
    }

    //---------------------------------------------------------------------
    // CSV JDBC private helper methods
    //---------------------------------------------------------------------

    /**
     * Perform pre-accessor method processing
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @throws SQLException if a database access error occurs
     */
    private void preAccessor(int columnIndex) throws SQLException {
        // set last read column index for wasNull()
        lastIndexRead = columnIndex;
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.getURL(int) unsupported");
    }

    public URL getURL(String columnName) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.getURL(String) unsupported");
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateRef(int,java.sql.Ref) unsupported");
    }

    public void updateRef(String columnName, Ref x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateRef(String,java.sql.Ref) unsupported");
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateBlob(int,java.sql.Blob) unsupported");
    }

    public void updateBlob(String columnName, Blob x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateBlob(String,java.sql.Blob) unsupported");
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateClob(int,java.sql.Clob) unsupported");
    }

    public void updateClob(String columnName, Clob x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateClob(String,java.sql.Clob) unsupported");
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateArray(int,java.sql.Array) unsupported");
    }

    public void updateArray(String columnName, Array x) throws SQLException {
        throw new UnsupportedOperationException("ResultSet.updateArray(String,java.sql.Array) unsupported");
    }

    public int getHoldability() throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getNString(int columnIndex) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public String getNString(String columnLabel) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBinaryStream(String columnLabel, InputStream x,
                                   long length) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateBlob(String columnLabel, InputStream inputStream,
                           long length) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateCharacterStream(String columnLabel, Reader reader,
                                      long length) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateClob(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNCharacterStream(String columnLabel, Reader reader,
                                       long length) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNString(int columnIndex, String string)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNString(String columnLabel, String string)
            throws SQLException {
        // TODO Auto-generated method stub
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return false;
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public int findColumn(String columnLabel) throws SQLException {
        if (columnLabel.equals("")) {
            throw new SQLException("Can't access columns with empty name by name");
        }
        for (int i = 0; i < this.queryEnvironment.size(); i++) {
            Object[] queryEnvEntry = this.queryEnvironment.get(i);
            if (((String) queryEnvEntry[0]).equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        return 0;
    }

    public NClob getNClob(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public NClob getNClob(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public RowId getRowId(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public RowId getRowId(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public SQLXML getSQLXML(int arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public SQLXML getSQLXML(String arg0) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    public void updateNClob(int arg0, NClob arg1) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateNClob(String arg0, NClob arg1) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateRowId(int arg0, RowId arg1) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateRowId(String arg0, RowId arg1) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
        // TODO Auto-generated method stub
    }

    public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
        // TODO Auto-generated method stub
    }

    public <T> T getObject(int parameterIndex, Class<T> type) {
        return null;
    }

    public <T> T getObject(String parameterName, Class<T> type) {
        return null;
    }

}
