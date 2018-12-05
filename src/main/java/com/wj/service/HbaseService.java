package com.wj.service;

import java.util.List;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date 2018-11-12 09:30
 **/
public interface HbaseService {

    void createTable(String name, List<String> columnFamilys);
    boolean addColumnFamily(String name, String... columnFamilys);
    boolean addRow(String name, String rowKey, String columnFamily, String column, String value);
    String getRow(String name, String rowKey, String columnFamily, String column);
    String getResultScanner(String name);
}
