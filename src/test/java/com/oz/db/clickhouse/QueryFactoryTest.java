package com.oz.db.clickhouse;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryFactoryTest {

    @Test
    public void getClickHouseQueryFromMsgsList() {
        List<Map<String, Object>> mapList = new ArrayList<>();
        mapList.add(new HashMap<>(Map.of("1", "v1_1", "2", "v2")));
        mapList.add(new HashMap<>(Map.of("1", "v1_2", "3", "v3")));
        mapList.add(new HashMap<>(Map.of("1", "v1_3", "4", "v4")));
        mapList.add(new HashMap<>(Map.of("1", "v1_4", "5", "v5")));
        QueryFactory queryFactory = new QueryFactory("col_");

        String result = queryFactory.createInsert(mapList, "testTableName");
        assertEquals("insert into testTableName ( col_1,col_2,col_3,col_4,col_5 ) values " +
                "('v1_1','v2','','','')" +
                "('v1_2','','v3','','')" +
                "('v1_3','','','v4','')" +
                "('v1_4','','','','v5')", result);
    }

    @Test
    public void getBulkListsColumnsAndValues_simple() {
        Map<String, Object> msgMap1 = Map.of("1", "v11", "2", "v12", "3", "v13");
        Map<String, Object> msgMap2 = Map.of("1", "v21", "2", "v22", "3", "v23");
        Map<String, Object> msgMap3 = Map.of("1", "v31", "2", "v32", "3", "v33");
        List<Map<String, Object>> mapList = List.of(msgMap1, msgMap2, msgMap3);
        QueryFactory queryFactory = new QueryFactory("col_");

        List<String> columns = new ArrayList<>();
        List<Map<String, String>> values = new ArrayList<>();
        queryFactory.getColumnsAndValuesForBulk(mapList, columns, values);

        assertEquals("[{col_1=v11, col_2=v12, col_3=v13}, " +
                "{col_1=v21, col_2=v22, col_3=v23}, " +
                "{col_1=v31, col_2=v32, col_3=v33}]", values.toString());
        assertColumns(columns, List.of("col_3","col_2","col_1"));
    }

    @Test
    public void getBulkListsColumnsAndValues_level1() {
        Map<String, Object> msgMap1 = Map.of("21", "v21", "22", "v22", "23", "v23");
        Map<String, Object> msgMap2 = Map.of("41", "v41", "42", "v42", "43", "v43");
        Map<String, Object> msgMap3 = Map.of("1", "v1", "2", List.of(msgMap1),
                "3", "v3", "4", List.of(msgMap2));
        List<Map<String, Object>> mapList = List.of(msgMap3);
        QueryFactory queryFactory = new QueryFactory("col_");

        List<String> columns = new ArrayList<>();
        List<Map<String, String>> values = new ArrayList<>();
        queryFactory.getColumnsAndValuesForBulk(mapList, columns, values);

        assertEquals(1, values.size());
        assertEquals("{col_1=v1, col_2=1, col_21_1=v21, col_22_1=v22, col_23_1=v23, " +
                        "col_3=v3, col_4=1, col_41_1=v41, col_42_1=v42, col_43_1=v43}",
                new TreeMap<>(values.get(0)).toString());
        assertColumns(columns, List.of("col_1",
                "col_2", "col_21_1", "col_22_1", "col_23_1",
                "col_3",
                "col_4", "col_41_1", "col_42_1", "col_43_1"));
    }


    @Test
    public void getBulkListsColumnsAndValues_level2() {
        Map<String, Object> msgMap22_1= Map.of("221", "v221_1", "222", "v222_1");
        Map<String, Object> msgMap22_2= Map.of("221", "v221_2", "222", "v222_2");
        Map<String, Object> msgMap2 = Map.of("21", "v21", "22", List.of(msgMap22_1, msgMap22_2), "23", "v23");
        Map<String, Object> msgMap = Map.of("1", "v1", "2", List.of(msgMap2), "3", "v3");
        List<Map<String, Object>> mapList = List.of(msgMap);
        QueryFactory queryFactory = new QueryFactory("col_");

        List<String> columns = new ArrayList<>();
        List<Map<String, String>> values = new ArrayList<>();
        queryFactory.getColumnsAndValuesForBulk(mapList, columns, values);

        assertEquals(1, values.size());
        assertEquals("{col_1=v1, col_2=1, col_21_1=v21, " +
                        "col_221_1_1=v221_1, col_221_1_2=v221_2, col_222_1_1=v222_1, col_222_1_2=v222_2, " +
                        "col_22_1=2, col_23_1=v23, col_3=v3}",
                new TreeMap<>(values.get(0)).toString());
        assertColumns(columns, List.of("col_1",
                "col_2",
                "col_21_1", "col_221_1_1", "col_221_1_2",
                "col_22_1", "col_222_1_1", "col_222_1_2",
                "col_23_1",
                "col_3" ));

    }

    @Test
    public void getBulkListsColumnsAndValues_level2_noSimple() {
        Map<String, Object> msgMap_l1_1_l2_2= Map.of("1001", "v_l1_1_l2_2");
        Map<String, Object> msgMap_l1_1_l2_1= Map.of("1002", "v_l1_1_l2_1");
        Map<String, Object> msgMap_l1_2_l2_2= Map.of("1001", "v_l1_2_l2_2");
        Map<String, Object> msgMap_l1_2_l2_1= Map.of("1002", "v_l1_2_l2_1");
        Map<String, Object> msgMap_l1_1 = Map.of("101", List.of(msgMap_l1_1_l2_1, msgMap_l1_1_l2_2));
        Map<String, Object> msgMap_l1_2 = Map.of("101", List.of(msgMap_l1_2_l2_1, msgMap_l1_2_l2_2));
        Map<String, Object> msgMap = Map.of("1", List.of(msgMap_l1_1, msgMap_l1_2));
        List<Map<String, Object>> mapList = List.of(msgMap);
        QueryFactory queryFactory = new QueryFactory("col_");

        List<String> columns = new ArrayList<>();
        List<Map<String, String>> values = new ArrayList<>();
        queryFactory.getColumnsAndValuesForBulk(mapList, columns, values);

        assertEquals(1, values.size());
        assertEquals("{col_1=2, " +
                        "col_1001_1_2=v_l1_1_l2_2, col_1001_2_2=v_l1_2_l2_2, " +
                        "col_1002_1_1=v_l1_1_l2_1, col_1002_2_1=v_l1_2_l2_1, " +
                        "col_101_1=2, col_101_2=2}",
                new TreeMap<>(values.get(0)).toString());
        assertColumns(columns, List.of("col_1", "col_1001_1_2", "col_1001_2_2", "col_1002_1_1",
                "col_1002_2_1", "col_101_1", "col_101_2"));
    }

    @Test
    public void generateBulkInsertForClickHouse_2() {
        List<String> columns = List.of("col_1", "col_2");
        List<List<String>> msgsMapsList = List.of( List.of("v11", "v12"),
                List.of("v12", "v22"), List.of("v13", "v32"));
        QueryFactory queryFactory = new QueryFactory("col_");

        String result = queryFactory.generateInsert(msgsMapsList ,columns,"testTableName");
        assertEquals("insert into testTableName ( col_1,col_2 ) " +
                "values ('v11','v12')('v12','v22')('v13','v32')", result);
    }

    @Test
    public void generateBulkInsertForClickHouse_4() {
        List<String> columns = List.of("col_1", "col_2", "col_3", "col_4");
        List<List<String>> msgsMapsList = List.of( List.of("v11", "v12", "v13", "v14"),
                List.of("v12", "v22", "v23","v24"), List.of("v13", "v32", "v33", "v34"));
        QueryFactory queryFactory = new QueryFactory("col_");

        String result = queryFactory.generateInsert(msgsMapsList ,columns,"testTableName");
        assertEquals("insert into testTableName ( col_1,col_2,col_3,col_4 ) " +
                "values ('v11','v12','v13','v14')('v12','v22','v23','v24')('v13','v32','v33','v34')", result);
    }

    @Test
    public void getFulfilledMsgValues_test() {
        List<String> columns = List.of("col_1", "col_2", "col_3",  "col_4");
        List<Map<String, String>> msgsMapsList = new ArrayList<>();
        msgsMapsList.add(Map.of( "col_1", "v11", "col_2", "v21"));
        msgsMapsList.add(Map.of( "col_4", "v42", "col_2", "v22"));
        msgsMapsList.add(Map.of( "col_3", "v33", "col_4", "v43"));
        QueryFactory queryFactory = new QueryFactory("col_");

        List<List<String>> result = queryFactory.getFulfilledValues(msgsMapsList, columns);

        assertEquals("[[v11, v21, , ], [, v22, , v42], [, , v33, v43]]", result.toString());
    }

    @Test
    public void getColumnsForBulk() {
        Map<String, Object> msgMap1 = Map.of("21", "v11", "22", "v12", "23", "v13");
        Map<String, Object> msgMap2 = Map.of("31", "v21", "32", "v22", "33", "v23");
        Map<String, Object> msgMap3 = Map.of("1", "v31", "2", List.of(msgMap1), "4", "v34", "3", List.of(msgMap2));
        QueryFactory queryFactory = new QueryFactory("col_");

        Collection<String> columns = queryFactory.getColumnsForBulk(List.of(msgMap3));

        assertColumns(columns, List.of("col_1", "col_2", "col_21_1", "col_22_1", "col_23_1",
                "col_3", "col_31_1", "col_32_1", "col_33_1", "col_4"));
    }

    @Test
    public void getColumnsForBulk_duplicateTags() {
        Map<String, Object> msgMap1 = Map.of("1", "v11", "2", "v12");
        Map<String, Object> msgMap2 = Map.of("2", "v22", "3", "v23");
        QueryFactory queryFactory = new QueryFactory("col_");

        Collection<String> columns = queryFactory.getColumnsForBulk(List.of(msgMap1, msgMap2));

        assertColumns(columns, List.of("col_1", "col_2", "col_3"));
    }

    @Test
    public void getColumnsForBulk_duplicateTags_L1() {
        Map<String, Object> msgMap1_1 = Map.of("20", "v20_1");
        Map<String, Object> msgMap1 = Map.of("1", "v11", "2", List.of(msgMap1_1));
        Map<String, Object> msgMap2_1 = Map.of("20", "v20_2");
        Map<String, Object> msgMap2 = Map.of("1", "v12", "2", List.of(msgMap2_1));
        QueryFactory queryFactory = new QueryFactory("col_");

        Collection<String> columns = queryFactory.getColumnsForBulk(List.of(msgMap1, msgMap2));

        assertColumns(columns, List.of("col_1", "col_2", "col_20_1"));
    }

    @Test
    public void getColumnsForBulk_duplicateTags_2gr_L1() {
        Map<String, Object> msgMap1 = Map.of("1", "v11",
                "2", List.of(Map.of("20", "v20_1")), "3", List.of(Map.of("30", "v30_1")));
        Map<String, Object> msgMap2 = Map.of("1", "v21",
                "2", List.of(Map.of("20", "v20_2")), "3", List.of(Map.of("31", "v30_2")));
        QueryFactory queryFactory = new QueryFactory("col_");

        Collection<String> columns = queryFactory.getColumnsForBulk(List.of(msgMap1, msgMap2));

        assertColumns(columns, List.of("col_1", "col_2", "col_20_1", "col_3", "col_30_1", "col_31_1"));
    }

    private void assertColumns(Collection<String> actual, Collection<String> expexted) {
        expexted.forEach(x -> assertTrue(actual.contains(x),
                String.format("Value %s absent in %s",x, actual)));
        actual.forEach(x -> assertTrue(expexted.contains(x),
                String.format("Unexpected value %s in %s",x, actual)));
        assertEquals(expexted.size(), actual.size(),
                String.format("Wrong number of items. expected %s actual %s}", expexted, actual));
    }

}