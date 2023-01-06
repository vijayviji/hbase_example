package org.example;

public class App {
    public static void main(String[] agrs) {
        String confFilePath = "/Users/vsomasundaram/Code/__hbase/conf/hbase-site.xml";
        //String confFilePath = "/home/y/libexec/hbase/conf/hbase-site.xml";
        try(HBaseClient client = new HBaseClient(confFilePath)) {
            String tablename = "chakra:votes";
            String[] families = { "f" };

            System.out.println("===========printing one record========");
            client.printRow(tablename, "testing");
            System.out.println("===========done printing record========");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void testHBaseClient() {
        try {
            String tablename = "scores";
            String[] families = { "grade", "course" };
            HBaseClient client = new HBaseClient("/home/y/libexec/hbase/conf/hbase-site.xml");
            client.createTable(tablename, families);

            // Add record abc
            client.addRow(tablename, "abc", "grade", "", "5");
            client.addRow(tablename, "abc", "course", "", "90");
            client.addRow(tablename, "abc", "course", "math", "97");
            client.addRow(tablename, "abc", "course", "art", "87");
            // Add record def
            client.addRow(tablename, "def", "grade", "", "4");
            client.addRow(tablename, "def", "course", "math", "89");

            System.out.println("===========get one record========");
            client.printRow(tablename, "zkb");

            System.out.println("===========show all record========");
            client.printAllRows(tablename);

            System.out.println("===========del one record========");
            client.deleteRow(tablename, "baoniu");
            client.printAllRows(tablename);

            System.out.println("===========show all record========");
            client.printAllRows(tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
