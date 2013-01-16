/*
 *  CsvJdbc - a JDBC driver for CSV files
 *  Copyright (C) 2001  Jonathan Ackerman
 *  Copyright (C) 2008, 2011  Mario Frasca
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.xbib.jdbc.csv;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class implements the ResultSetMetaData interface for the CsvJdbc driver.
 *
 * @author Jonathan Ackerman
 * @author JD Evora
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class CsvResultSetMetaData implements ResultSetMetaData {

    private List<String> columnNames;
    private List<String> columnTypes;
    private String tableName;

    private final Map<String, Integer> types = new HashMap<String, Integer>() {

        {
            put("String", Types.VARCHAR);
            put("Boolean", Types.BOOLEAN);
            put("Byte", Types.TINYINT);
            put("Short", Types.SMALLINT);
            put("Int", Types.INTEGER);
            put("Integer", Types.INTEGER);
            put("Long", Types.BIGINT);
            put("Float", Types.FLOAT);
            put("Double", Types.DOUBLE);
            put("BigDecimal", Types.DECIMAL);
            put("Date", Types.DATE);
            put("Time", Types.TIME);
            put("Timestamp", Types.TIMESTAMP);
            put("Blob", Types.BLOB);
            put("Clob", Types.CLOB);
            put("expression", Types.BLOB);
        }
    };

    /**
     * Constructor for the CsvResultSetMetaData object
     *
     * @param tableName   Name of table
     * @param columnNames the column names
     * @param columnTypes Names of columns in table
     */
    CsvResultSetMetaData(String tableName, List<String> columnNames, List<String> columnTypes) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    /**
     * Returns the name of the catalog for the specified column. Returns "".
     *
     * @param column The column to get the catalog for
     * @return The catalog name (always "")
     * @throws SQLException Thrown if there is a problem
     */
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    public String getColumnClassName(int column) throws SQLException {
        throw new SQLException("Method not supported");
    }

    /**
     * Returns the number of columns in the table.
     *
     * @return The number of columns in the table
     * @throws SQLException Thrown if there is a a problem
     */
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    public int getColumnType(int column) throws SQLException {
        return types.get(columnTypes.get(column-1));
    }

    public String getColumnTypeName(int column) throws SQLException {
        return columnTypes.get(column-1);
    }

    /**
     * Returns the display column size for the specified column.
     *
     * @param column The column to get the size of
     * @return The size of the requested column
     * @throws SQLException Thrown if there is a problem.
     */
    public int getColumnDisplaySize(int column) throws SQLException {
        switch(getColumnType(column)) {
            case Types.VARCHAR:
            case Types.BIGINT:
                return 32;
            case Types.TINYINT:
                return 2;
            case Types.BOOLEAN:
                return 8;
            case Types.DOUBLE:
            case Types.INTEGER:
                return 16;
            default:
                return 32;
        }
    }

    /**
     * Returns a comment regarding the specified column
     *
     * @param column The column to get the label for
     * @return the label for the specified column
     * @throws SQLException Thrown if there is a problem
     */
    public String getColumnLabel(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    /**
     * Returns the name of the specified column
     *
     * @param column The column to get the name of
     * @return The name of the column
     * @throws SQLException Thrown if there is a problem
     */
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(column - 1);
    }
    /**
     * Gets the auto increment flag for the specified column.
     *
     * @param column The column to get the flag for
     * @return The autoIncrement flag (always false)
     * @throws SQLException Thrown if there is a problem
     */
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    /**
     * Returns the case sensitivity flag for the specified column
     *
     * @param column The column to return the flag for
     * @return The caseSensitive flag (always false)
     * @throws SQLException Thrown if there is a problem
     */
    public boolean isCaseSensitive(int column) throws SQLException {
        // all columns are uppercase
        return false;
    }

    /**
     * Returns the searchable flag for the specified column
     *
     * @param column the column to return the flag form
     * @return The searchable flag (always false)
     * @throws SQLException Thrown if there is a problem
     */
    public boolean isSearchable(int column) throws SQLException {
        // the implementation doesn't support the where clause
        return false;
    }

    /**
     * Returns the currency flag for the specified column
     *
     * @param column The column to get the flag for
     * @return The currency flag (always false)
     * @throws SQLException Thrown if there is a problem
     */
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    /**
     * Returns the nullable flag for the specified column
     *
     * @param column The column to return the flag for
     * @return The nullable flag (always unknown)
     * @throws SQLException Thrown if there is a problem
     */
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullableUnknown;
    }

    /**
     * Returns the signed flag for the specified column
     *
     * @param column The column to return the flag for
     * @return The signed flag (always false)
     * @throws SQLException Thrown if there is a problem
     */
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    public int getPrecision(int column) throws SQLException {
        // All the fields are text, should this throw an SQLException?
        return 0;
    }

    public int getScale(int column) throws SQLException {
        // All the fields are text, should this throw an SQLException?
        return 0;
    }

    public String getTableName(int column) throws SQLException {
        return tableName;
    }

    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    public boolean isWrapperFor(Class<?> arg0) throws SQLException {
        return false;
    }

    public <T> T unwrap(Class<T> arg0) throws SQLException {
        return null;
    }

}
