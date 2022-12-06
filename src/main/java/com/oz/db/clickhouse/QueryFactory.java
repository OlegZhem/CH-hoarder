package com.oz.db.clickhouse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class QueryFactory {

    private static final Logger LOG = LoggerFactory.getLogger(QueryFactory.class);

    private final String columnPrefix;

    public QueryFactory(String columnPrefix) {
        this.columnPrefix = columnPrefix;
    }

    public String createInsert(List<Map<String, Object>> dataListOfMap, String tableName) {
        List<String> columns = new ArrayList<>();
        List<Map<String, String>> valuesWithColumns = new ArrayList<>();
        getColumnsAndValuesForBulk(dataListOfMap, columns, valuesWithColumns);

        List<List<String>> fulfilledValuesList = getFulfilledValues(valuesWithColumns, columns);

        return generateInsert(fulfilledValuesList, columns, tableName);

    }

    public String generateInsert(List<List<String>> values,
                                 List<String> columns,
                                 String tableName) {
        String strValues = values.stream()
                .map(list -> list.stream()
                        .map(x -> "'" + x + "'")
                        .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.joining(""));
        String strColumns = String.join(",", columns);

        String query = "insert into " + tableName + " ( " + strColumns + " ) values " + strValues;
        return query;
    }

    public List<List<String>> getFulfilledValues(List<Map<String, String>> dataListOfMap,
                                                 List<String> columns) {
        List<List<String>> fulfilledValuesList = new ArrayList<>();

        for (Map<String, String> dataMap : dataListOfMap) {
            List<String> currentValues = new ArrayList<>();
            for (String columnName : columns) {
                if (dataMap.containsKey(columnName)) {
                    currentValues.add(dataMap.get(columnName));
                } else {
                    currentValues.add("");
                }
            }
            fulfilledValuesList.add(currentValues);
        }

        return fulfilledValuesList;
    }

    private void prepareColumnsAndValues(Map<String, Object> dataMap,
                                         String postfix,
                                         List<String> columns,
                                         Map<String, String> valuesWithColumns) {
        dataMap.forEach((k, v) -> {
            String columnName = columnPrefix + k + postfix;
            if (!columns.contains(columnName)) {
                columns.add(columnName);
            }
            String columnValue;
            if (v instanceof List) {
                List<Map<String, Object>> vGroups = (List<Map<String, Object>>) v;
                columnValue = String.valueOf(vGroups.size());
                for (int i = 1; i <= vGroups.size(); i++) {
                    prepareColumnsAndValues(vGroups.get(i - 1), postfix + "_" + i, columns, valuesWithColumns);
                }
            } else {
                columnValue = v.toString();
            }

            valuesWithColumns.put(columnName, columnValue);
        });
    }

    public void getColumnsAndValuesForBulk(List<Map<String, Object>> mapList,
                                           List<String> columns,
                                           List<Map<String, String>> values) {
        mapList.forEach(map -> {
            Map<String, String> singleMsgValues = new HashMap<>();
            prepareColumnsAndValues(map, "", columns, singleMsgValues);
            values.add(singleMsgValues);
        });
    }

    public Collection<String> getColumnsForBulk(List<Map<String, Object>> dataLitOfMap) {
        Set<String> columns = new HashSet<>();
        dataLitOfMap.forEach(map -> getColumnsForOneRow(map, "", columns));
        return columns;
    }

    private void getColumnsForOneRow(Map<String, Object> data, String postfix, Set<String> columns) {
        data.forEach((k, v) -> {
            if (v instanceof List) {
                columns.add(columnPrefix + k + postfix);
                List<Map<String, Object>> vGroups = (List<Map<String, Object>>) v;
                for (int i = 1; i <= vGroups.size(); i++) {
                    getColumnsForOneRow(vGroups.get(i - 1), postfix + "_" + i, columns);
                }
            } else {
                columns.add(columnPrefix + k + postfix);
            }
        });
    }

}
