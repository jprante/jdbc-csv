package org.xbib.jdbc.csv.support;

import java.sql.SQLException;
import java.util.Map;

public interface DataReader {

    int DEFAULT_COLUMN_SIZE = 20;

    boolean next() throws SQLException;

    String[] getColumnNames() throws SQLException;

    Object getField(int i) throws SQLException;

    void close() throws SQLException;

    Map<String, Object> getEnvironment() throws SQLException;

    String[] getColumnTypes() throws SQLException;

    int[] getColumnSizes() throws SQLException;

    String getTableAlias();
}