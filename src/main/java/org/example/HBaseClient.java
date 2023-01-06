package org.example;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient implements AutoCloseable {
  private final Connection connection;

  public HBaseClient(String hbaseConfFilePath) {
    Configuration conf = loadHbaseConf(hbaseConfFilePath);
    try {
      System.out.println(new LogObj("CreatingConnection"));
      connection = ConnectionFactory.createConnection(conf);
      System.out.println(new LogObj("ConnectionCreated"));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a table
   */
  public void createTable(String tableName, String[] columnFamilies) throws Exception {
    Admin admin = connection.getAdmin();
    if (admin.tableExists(TableName.valueOf(tableName))) {
      System.out.println(new LogObj("TableAlreadyExists").add("status", "skippingCreation"));
    } else {
      List<ColumnFamilyDescriptor> cfds = new ArrayList<>();
      for (String columnFamily : columnFamilies) {
        ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
            .newBuilder(Bytes.toBytes(columnFamily)).build();
        cfds.add(cfd);
      }
      TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(
          TableName.valueOf(tableName)).setColumnFamilies(cfds).build();
      admin.createTable(tableDesc);
      System.out.println(new LogObj("CreatedTable").add("table", tableName));
    }
  }
  /**
   * Delete a table
   */
  public void deleteTable(String tableName) throws Exception {
    try {
      Admin admin = connection.getAdmin();
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
      System.out.println(new LogObj("DeletedTable").add("table", tableName));
    } catch (MasterNotRunningException | ZooKeeperConnectionException e) {
      e.printStackTrace();
    }
  }

  /**
   * Put (or insert) a row
   */
  public void addRow(String tableName, String rowKey, String family, String qualifier, String value) {
    try {
      Table table = connection.getTable(TableName.valueOf(tableName));

      // row creation
      Put row = new Put(Bytes.toBytes(rowKey));
      row.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

      table.put(row);
      System.out.println(new LogObj("InsertedRow").add("table", table)
          .add("rowKey", rowKey));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Delete a row
   */
  public void deleteRow(String tableName, String rowKey) throws IOException {
    Table table = connection.getTable(TableName.valueOf(tableName));
    List<Delete> list = new ArrayList<>();
    Delete del = new Delete(rowKey.getBytes());
    list.add(del);
    table.delete(list);
    System.out.println(new LogObj("DeletedRow").add("rowKey", rowKey));
  }

  /**
   * Get a row
   */
  public void printRow(String tableName, String rowKey) throws IOException {
    System.out.println(new LogObj("GettingTable"));
    Table table = connection.getTable(TableName.valueOf(tableName));
    System.out.println(new LogObj("GottenTable"));
    Get get = new Get(rowKey.getBytes());
    System.out.println(new LogObj("GettingRow"));
    Result rs = table.get(get);
    System.out.println(new LogObj("GotRow_GoingToPrint"));
    printRow(rs);
  }

  /**
   * Scan (or list) a table
   */
  public void printAllRows(String tableName) {
    try {
      Table table = connection.getTable(TableName.valueOf(tableName));
      Scan s = new Scan();
      ResultScanner ss = table.getScanner(s);
      for(Result r:ss){
        printRow(r);
      }
    } catch (IOException e){
      e.printStackTrace();
    }
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  private static void printRow(Result rs) {
    for(Cell cell : rs.listCells()){
      String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
      String family = Bytes.toString(CellUtil.cloneFamily(cell));
      String value = Bytes.toString(CellUtil.cloneValue(cell));
      String rowKey = Bytes.toString(CellUtil.cloneRow(cell));

      System.out.println(new LogObj("row")
          .add("rowKey", rowKey)
          .add("family", family)
          .add("qualifier", qualifier)
          .add("value", value));
    }
  }

  private Configuration loadHbaseConf(String hbaseSiteXmlPath) {
    Configuration hbaseConf = new Configuration();
    try {
      hbaseConf.addResource(new URL("file://" + hbaseSiteXmlPath));
      System.out.println(new LogObj("hbaseConfLoaded")
          .add("path", hbaseSiteXmlPath)
          .addWithQuotes("conf", hbaseConf));
    } catch (Exception ex) {
      System.out.println(new LogObj("FailedToLoadHbaseConf").add(ex));
      throw new RuntimeException(ex);
    }
    return hbaseConf;
  }
}
