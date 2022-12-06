package com.oz.db.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ClickhouseTableHelper {

    private static final Logger LOG = LoggerFactory.getLogger(ClickhouseTableHelper.class);

    private final Connection connection;
    private final String fullTableName;
    private final String database;
    private final String simpleTableName;
    private final QueryFactory queryFactory;

    private Map<String, String> dbColumns;


    public ClickhouseTableHelper(Connection connection, String fullTableName, String columnPrefix) {
        this.connection = connection;
        if (fullTableName.contains(".")) {
            String[] strs = fullTableName.split("\\.");
            this.fullTableName = fullTableName;
            this.database = strs[0];
            this.simpleTableName = strs[1];
        } else {
            this.database = "default";
            this.fullTableName = this.database + "." + fullTableName;
            this.simpleTableName = fullTableName;
        }
        queryFactory = new QueryFactory(columnPrefix);
    }

    public Map<String, String> getColumns() {
        try {
            loadColumns(connection);
        } catch (SQLException | ClassNotFoundException ex) {
            LOG.error("could not get columns", ex);
        }
        return dbColumns;
    }

    public int insert(Map<String, Object> map) {
        boolean addColumns = false;
        int cnt = 0;

        try {

            try {
                cnt = insertRow(map);
            } catch (SQLException ex) {
                //LOG.trace("Exception: {}", ex.getMessage());
                if (ex.getMessage().contains("No such column")) {
                    LOG.info("Need add columns to {} for {}", fullTableName, map);
                    addColumns = true;
                } else if (ex.getMessage().contains("Table "+fullTableName+" doesn't exist")) {
                    LOG.info("Need create table {} for {}", fullTableName, map);
                    addColumns = true;
                } else {
                    throw ex;
                }
            }
        } catch (SQLException ex) {
            LOG.error("could not insert", ex);
        }

        if (addColumns) {
            try {
                addColumns(map);
                cnt = insert(map);

            } catch (SQLException | ClassNotFoundException ex) {
                LOG.error("could not insert", ex);
            }
        }
        return cnt;
    }

    private int insertRow(Map<String, Object> map) throws SQLException {
        try (Statement statement = connection.createStatement()){
            List<String> columns = new ArrayList<>();
            List<String> values = new ArrayList<>();
            getListsColumnsAndValues(map, "", columns, values);
            String query = "insert into " + fullTableName
                    + " ( " + columns.stream().collect(Collectors.joining(","))
                    + " ) values ( "
                    + values.stream().map(x -> "'"+x+"'").collect(Collectors.joining(","))
                    + " )";
            LOG.trace("Query: {}", query);
            if (!statement.execute(query)) {
                LOG.trace("insert {} rows", statement.getUpdateCount());
                return statement.getUpdateCount();
            }
        }
        return 0;
    }

    public int insertBulk(List<Map<String, Object>> mapList) {
        boolean addColumns = false;
        int cnt = 0;

        try {

            try {
                cnt = insertBulkImpl(mapList);
            } catch (SQLException ex) {
                //LOG.trace("Exception: {}", ex.getMessage());
                if (ex.getMessage().contains("No such column")) {
                    LOG.info("Need add columns to {} for {}", fullTableName, mapList);
                    addColumns = true;
                } else if (ex.getMessage().contains("Table "+fullTableName+" doesn't exist")) {
                    LOG.info("Need create table {} for {}", fullTableName, mapList);
                    addColumns = true;
                } else {
                    throw ex;
                }
            }
        } catch (SQLException ex) {
            LOG.error("could not insert", ex);
        }

        if (addColumns) {
            try  {
                addColumns(mapList);
                cnt = insertBulkImpl(mapList);

            } catch (SQLException | ClassNotFoundException ex) {
                LOG.error("could not insert", ex);
            }
        }
        return cnt;
    }

    private int insertBulkImpl(List<Map<String, Object>> mapList) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            String query = queryFactory.createInsert(mapList, fullTableName);
            LOG.trace("Query: {}", query);
            if (!statement.execute(query)) {
                LOG.trace("insert {} rows", statement.getUpdateCount());
                return statement.getUpdateCount();
            }
        }
        return 0;
    }

    private void addColumns(Map<String, Object> map) throws SQLException, ClassNotFoundException {
        addColumns(map.keySet().stream());
    }

    private void addColumns(List<Map<String, Object>> mapList) throws SQLException, ClassNotFoundException {
        addColumns(mapList.stream().flatMap(x -> x.keySet().stream()));
    }

    private void addColumns(Stream<String> columns) throws SQLException, ClassNotFoundException {
        loadColumns(connection);
        Iterator<String> newColumns = columns
                .filter(x -> !dbColumns.containsKey(x))
                .iterator();
        while (newColumns.hasNext()) {
            String newColumn = newColumns.next();
            createColumn(newColumn);
            dbColumns.put(newColumn, newColumn);
        }
    }

    private static void getListsColumnsAndValues(Map<String, Object> map,
                                                 String postfix,
                                                 List<String> columns,
                                                 List<String> values) {
        map.forEach((k, v) -> {
            if(v instanceof List) {
                columns.add("tag_"+k);
                List<Map<String, Object>> vGroups = (List<Map<String, Object>>) v;
                values.add(String.valueOf(vGroups.size()));
                for(int i = 1; i <= vGroups.size(); i++) {
                    getListsColumnsAndValues(vGroups.get(i - 1), postfix + "_" + i, columns, values);
                }
            } else {
                columns.add("tag_" + k + postfix);
                values.add(v.toString());
            }
        });
    }

    private void createColumn(String newColumn) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            String query = "ALTER TABLE "+fullTableName+" ADD COLUMN "+newColumn+" String ";
            LOG.debug("Query: {}", query);
            statement.execute(query);
        }
    }

    private void loadColumns(Connection connection) throws SQLException, ClassNotFoundException {
        if(null != dbColumns) {
            return;
        }
        synchronized (this) {
            if(null != dbColumns) {
                return;
            }
            String query = "SELECT * FROM system.tables WHERE database = '"
                    + database + "' and name = '" + simpleTableName + "'";
            try (Statement statement = connection.createStatement()) {

                LOG.debug("Query: {}", query);
                try (ResultSet rs = statement.executeQuery(query)) {
                     if (null == rs || !rs.next()) {
                         createTableIfNotExists();
                    }
                }

                Map<String, String> columns = new HashMap<>();

                LOG.debug("Query: {}", query);
                try (ResultSet rs = statement.executeQuery(query)) {
                    while (rs.next()) {
                        String val = rs.getString("name");
                        String type = rs.getString("type");
                        columns.put(val, type);
                    }
                }
                dbColumns = columns;
            }
        }
    }

    public void createTableIfNotExists() throws SQLException {
        Statement statement = connection.createStatement();
        String query = "CREATE TABLE IF NOT EXISTS " + fullTableName +
                " ( date Date DEFAULT today() ) ENGINE = MergeTree(date, (date), 8192)";
        LOG.debug("Query: {}", query);
        statement.execute(query);
    }

    public void dropTableIfExists() {
        String query = "";
        try (Statement statement = connection.createStatement()) {
            query = "DROP TABLE IF EXISTS " + fullTableName;
            LOG.debug("Query: {}", query);
            statement.execute(query);
        } catch (SQLException ex) {
            LOG.error("could not execute " + query, ex);
        }
        dbColumns = null;
    }

    public String getDatabase() {
        return database;
    }

    public String getSimpleTableName() {
        return simpleTableName;
    }

    public String getFullTableName() {
        return fullTableName;
    }
}
