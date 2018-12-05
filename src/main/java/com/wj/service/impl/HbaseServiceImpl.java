package com.wj.service.impl;

import com.wj.service.HbaseService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author wangJun
 * @Description //TODO
 * @Date ${date} ${time}
 **/

@Service
public class HbaseServiceImpl implements HbaseService {

    @Autowired
    private Configuration configuration;

    public HbaseServiceImpl(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * 创建表
     * @param name 表名
     * @param columnFamilys 列族
     */
    @Override
    public void createTable(String name, List<String> columnFamilys) {
        if (columnFamilys == null||name == null) {
            return;
        }
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            HBaseAdmin hBaseAdmin = (HBaseAdmin)conn.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if (hBaseAdmin.tableExists(tableName)) {
                if (hBaseAdmin.isTableEnabled(tableName)) {
                    hBaseAdmin.disableTable(tableName);
                }
                hBaseAdmin.deleteTable(tableName);
            }
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(name));
            for (String columnFamily: columnFamilys) {
                hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
            hBaseAdmin.createTable(hTableDescriptor);
        }
        catch (Exception e) {
            throw new RuntimeException("create HBase table error", e);
        }
        finally {
            close(conn);
        }
    }

    /**
     * 表添加列族
     * @param name 表名
     * @param columnFamilys 列族
     * @return
     */
    @Override
    public boolean addColumnFamily(String name, String... columnFamilys) {
        if (name == null||columnFamilys == null) {
            return false;
        }
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            HBaseAdmin hBaseAdmin = (HBaseAdmin)conn.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if (hBaseAdmin.tableExists(tableName)) {
                if (hBaseAdmin.isTableEnabled(tableName)) {
                    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(name));
                    for (String columnFamily: columnFamilys) {
                        hBaseAdmin.addColumn(tableName, new HColumnDescriptor(columnFamily));
                    }
                    return true;
                }

            }
        }
        catch (Exception e) {
            throw new RuntimeException("add HBase table columnFamily error", e);
        }
        finally {
            close(conn);
        }
        return false;
    }

    /**
     * 添加一行数据
     * @param name 表名
     * @param rowKey 行标识
     * @param columnFamily 列族
     * @param column 列
     * @param value 值
     * @return
     */
    @Override
    public boolean addRow(String name, String rowKey, String columnFamily, String column, String value) {
        if (name == null||rowKey == null||columnFamily == null||column == null) {
            return false;
        }
        Connection conn = null;
        HTable table = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            HBaseAdmin hBaseAdmin = (HBaseAdmin)conn.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if (hBaseAdmin.tableExists(tableName)) {
                if (hBaseAdmin.isTableEnabled(tableName)) {
                    table = (HTable) conn.getTable(tableName);
                    Put put = new Put(Bytes.toBytes(rowKey));
                    // 在put对象中设置列族、列、值
                    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
                    table.put(put);
                    return true;
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("add HBase table row error", e);
        }
        finally {
            close(table);
            close(conn);
        }
        return false;
    }

    /**
     * 获取一行数据
     * @param name
     * @param rowKey
     * @param columnFamily
     * @param column
     * @return
     */
    @Override
    public String getRow(String name, String rowKey, String columnFamily, String column) {
        if (name == null||rowKey == null||columnFamily == null||column == null) {
            return null;
        }
        Connection conn = null;
        HTable table = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            HBaseAdmin hBaseAdmin = (HBaseAdmin)conn.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if (hBaseAdmin.tableExists(tableName)) {
                if (hBaseAdmin.isTableEnabled(tableName)) {
                    table = (HTable) conn.getTable(tableName);
                    Get get = new Get(Bytes.toBytes(rowKey));
                    get.addColumn(columnFamily.getBytes(), column.getBytes());
                    Result result = table.get(get);
                    if (result.containsColumn(columnFamily.getBytes(), column.getBytes())) {
                        byte[] arrValue = result.getValue(columnFamily.getBytes(), column.getBytes());
                        String value = new String(arrValue);
                        return value;
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("get HBase table column value error", e);
        }
        finally {
            close(table);
            close(conn);
        }
        return null;
    }

    /**
     * 表扫描
     * @param name
     * @return
     */
    @Override
    public String getResultScanner(String name) {
        if (name == null||name.length() == 0) {
            return null;
        }
        Connection conn = null;
        HTable table = null;
        ResultScanner resultScanner = null;
        try {
            conn  = ConnectionFactory.createConnection(configuration);
            HBaseAdmin hBaseAdmin = (HBaseAdmin)conn.getAdmin();
            TableName tableName = TableName.valueOf(name);
            if (hBaseAdmin.tableExists(tableName)) {
                if (hBaseAdmin.isTableEnabled(tableName)) {
                    table = (HTable)conn.getTable(tableName);
                    Scan scan=new Scan();
                    resultScanner = table.getScanner(scan);
                    Iterator<Result> iterator = resultScanner.iterator();
                    while (iterator.hasNext()) {
                        Result result = iterator.next();
                        byte[] rowArr = result.getRow();
                        System.out.println(new String(rowArr));
                    }
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("get HBase ResultScanner error", e);
        }
        finally {
            close(resultScanner);
            close(table);
            close(conn);
        }
        return null;
    }

    private void close(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        }
        catch (Exception e) {
            throw new RuntimeException("close HBase connection error", e);
        }
    }

    private void close(HTable table) {
        try {
            if (table != null) {
                table.close();
            }
        }
        catch (Exception e) {
            throw new RuntimeException("close HTable error", e);
        }
    }
    private void close(ResultScanner resultScanner) {
        try {
            if (resultScanner != null) {
                resultScanner.close();
            }
        }
        catch (Exception e) {
            throw new RuntimeException("close ResultScanner error", e);
        }
    }

    public static void main(String args[]) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop-common-2.2.0-bin-master");
        org.apache.hadoop.conf.Configuration configuration = new Configuration();
        //org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        //configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "10.50.20.171:2181");
        HbaseServiceImpl hbaseService = new HbaseServiceImpl(configuration);
        List<String> familyList = new ArrayList<>();
        familyList.add("column1");
        //hbaseService.createTable("TEST", familyList);
        //hbaseService.addColumnFamily("TEST", "column2");
        hbaseService.addRow("TEST", "r1", "column1", "name", "wangjun");
        //hbaseService.addRow("TEST", "r2", "column1", "age", "20");

        //String value = hbaseService.getRow("test", "r1", "column1", "name1");
        //System.out.println(value);
        hbaseService.getResultScanner("TEST");

    }
}
