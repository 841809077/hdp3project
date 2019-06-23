package com.hdp.project.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * @author liuyzh
 * @description: 远程连接HBase，执行表的增删改查操作
 * @Version: HBase 1.1.2
 * @date 2018/10/27 14:14
 */
public class hbaseTestCase {

    //用hbaseconfiguration初始化配置信息时会自动加载当前应用classpath下的hbase-site.xml
    private static Configuration conf = HBaseConfiguration.create();

    private Admin admin = null;

    private Connection connection = null;

    static {
        conf.set("hbase.zookeeper.quorum", "node71.xdata,node72.xdata,node73.xdata");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //HBase的Master
        conf.set("hbase.master","node71.xdata:16000");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\hadoop-2.7.3");
    }

    /**
     * @description: 连接HBase客户端
     */
    @Test
    @Before
    public void hbaseConnect() {
        try {
            // 获取hbase连接对象
            if (connection==null || connection.isClosed()){
                connection = ConnectionFactory.createConnection(conf);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 创建表空间
     */
    @Test
    public void createNameSpace() {
        String NameSpace = "testNameSpace3";
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            // 创建名字空间描述符
            NamespaceDescriptor nsd = NamespaceDescriptor.create(NameSpace).build();
            admin.createNamespace(nsd);
            System.out.println("表空间" + NameSpace + "创建成功");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 列出表空间
     */
    @Test
    public void listNameSpace() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
            NamespaceDescriptor[] ns = admin.listNamespaceDescriptors();
            System.out.println("表空间为：");
            for (NamespaceDescriptor n : ns) {
                System.out.println(n.getName());
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 删除表空间
     */
    @Test
    public void deleteNameSpace() {
        String NameSpace = "testNameSpace";
        try {
            admin = connection.getAdmin();
            admin.deleteNamespace(NameSpace);
            System.out.println("表空间" + NameSpace + "删除成功");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 创建HBase表
     */
    @Test
    public void createTable() {
        String tableName = "student";
        String[] cf1 = {"cf1", "cf2"};
        //HTD需要TableName类型的tableName，创建tablename对象描述表的名称信息
        TableName tbName = TableName.valueOf(tableName);
        //获取admin对象
        try {
            admin = connection.getAdmin();
            //判断表述否已存在，不存在则创建表
            if (admin.tableExists(tbName)) {
                System.err.println("表" + tableName + "已存在！");
                return;
            }
            //创建HTableDescriptor对象，描述表信息
            HTableDescriptor HTD = new HTableDescriptor(tbName);
            //添加表列簇信息
            for (String cf : cf1) {
                // 创建HColumnDescriptor对象添加表的详细的描述
                HColumnDescriptor HCD = new HColumnDescriptor(cf);
                HTD.addFamily(HCD);
            }
            //调用admin的createtable方法创建表
            admin.createTable(HTD);
            System.out.println("表" + tableName + "创建成功");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 删除hbase表，前提条件是将表解除占用
     */
    @Test
    public void deleteTable() {
        String tableName = "student";
        //通过tableName创建表名
        TableName tbName = TableName.valueOf(tableName);
        //获取admin对象
        try {
            admin = connection.getAdmin();
            //判断表是否存在，若存在就删除，不存在就退出
            if (admin.tableExists(tbName)) {
                //首先将表解除占用，否则无法删除
                admin.disableTable(tbName);
                //调用delete方法
                admin.deleteTable(tbName);
                System.out.println("表" + tableName + "已删除");
            } else {
                System.err.println("表" + tableName + "不存在！");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 获取所有hbase表
     */
    @Test
    public void getAllTables() {
        try {
            //获取admin对象
            admin = connection.getAdmin();
            if (admin != null) {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0) {
                    for (HTableDescriptor hTableDescriptor : allTable) {
                        System.out.println(hTableDescriptor.getNameAsString());
                    }
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 向表中插入数据
     */
    @Test
    public void putData() {
        TableName tableName = TableName.valueOf("testNameSpace:student");
        Table table = null;
        try {
            admin = connection.getAdmin();
            //判断表是否存在
            if (admin.tableExists(tableName)) {
                table = connection.getTable(tableName);
                Random random = new Random();
                List<Put> batPut = new ArrayList<Put>();
                for (int i = 0; i < 10; i++) {
                    //构建put的参数是rowkey
                    Put put = new Put(Bytes.toBytes("rowkey_" + i));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("username"), Bytes.toBytes("un" + i));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(random.nextInt(50))));
                    put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("birthday"), Bytes.toBytes("2018-" + (i + 1) + "-29"));
                    put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("phone"), Bytes.toBytes("电话" + i));
                    put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("email"), Bytes.toBytes("email_" + i));
                    batPut.add(put);
                }
                table.put(batPut);
                System.out.println(tableName + "表插入数据成功");
            } else {
                System.err.println(tableName + "表不存在");
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 方法一
     * @description: 查询数据：方法1：使用CellScanner类遍历数据表
     */
    @Test
    public void getData() {
        String tableName = "student";
        TableName tName = TableName.valueOf(tableName);
        // 连接指定表
        Table table = null;
        try {
            table = connection.getTable(tName);
            // 构建get对象
            List<Get> gets = new ArrayList<Get>();
            // 循环5次，取前5个测试数据
            for (int i = 0; i < 5; i++) {
                Get get = new Get(Bytes.toBytes("rowkey_" + i));
                gets.add(get);
            }
            // 获取结果集
            Result[] results = table.get(gets);
            for (Result result : results) {
                // 使用cell获取result里面的数据
                CellScanner cellScanner = result.cellScanner();
                while (cellScanner.advance()) {
                    Cell cell = cellScanner.current();
                    // 从单元格cell中把数据获取并输出
                    // 使用CellUtil工具类，从cell中把数据获取出来
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String quality = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String rowkey = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.println("rowkey：" + rowkey + ",columnfamily：" + family + ",quality：" + quality + ",value：" + value);
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 一个个地取，较麻烦
     * 方法二
     */
    @Test
    public void scanData1() {
        String tableName = "testNameSpace:student";
        TableName tName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(tName);
            Scan scan = new Scan();
            // 可设置rowkey查询范围
//            scan.setStartRow(Bytes.toBytes("rowkey_0"));
//            scan.setStopRow(Bytes.toBytes("rowkey_5"));
            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> it = rs.iterator();
            while (it.hasNext()) {
                Result r = it.next();
                byte[] username = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("username"));
                byte[] birthday = r.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("birthday"));
                System.out.println(Bytes.toString(username) + "," + Bytes.toString(birthday));
            }
            rs.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 通过getmap方法获取结果集，并循环遍历map获取数据
     * @description: 获取从rowkey0~rowkey_9的cf1的所有值
     * 方法三
     */
    @Test
    public void scanData2() {
        String tableName = "testNameSpace:student";
        TableName tName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(tName);
            Scan scan = new Scan();
            // 可设置rowkey查询范围
//            scan.setStartRow(Bytes.toBytes("rowkey_0"));
//            scan.setStopRow(Bytes.toBytes("rowkey_9"));
            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> it = rs.iterator();
            while (it.hasNext()) {
                Result r = it.next();
                Map<byte[], byte[]> map = r.getFamilyMap(Bytes.toBytes("cf1"));
                for (Map.Entry<byte[], byte[]> entrySet : map.entrySet()) {
                    String col = Bytes.toString(entrySet.getKey());
                    String val = Bytes.toString(entrySet.getValue());
                    System.out.print(col + ":" + val + ",");
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 遍历表中所有列族的所有数据
     * 方法四
     */
    @Test
    public void scan3() throws IOException {
        TableName tname = TableName.valueOf("testNameSpace:student");
        Table table = connection.getTable(tname);
        Scan scan = new Scan();
        // 可设置rowkey查询范围
//        scan.setStartRow(Bytes.toBytes("rowkey_0"));
//        scan.setStopRow(Bytes.toBytes("rowkey_9"));
        ResultScanner rs = table.getScanner(scan);
        Iterator<Result> it = rs.iterator();
        while (it.hasNext()) {
            Result r = it.next();
            //得到一行的所有map,key=f1,value=Map<Col,Map<Timestamp,value>>
            NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();
            //
            for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
                //得到列族
                String f = Bytes.toString(entry.getKey());
                Map<byte[], NavigableMap<Long, byte[]>> colDataMap = entry.getValue();
                for (Map.Entry<byte[], NavigableMap<Long, byte[]>> ets : colDataMap.entrySet()) {
                    String c = Bytes.toString(ets.getKey());
                    Map<Long, byte[]> tsValueMap = ets.getValue();
                    for (Map.Entry<Long, byte[]> e : tsValueMap.entrySet()) {
                        Long ts = e.getKey();
                        String value = Bytes.toString(e.getValue());
                        System.out.print(f + ":" + c + ":" + ts + "=" + value + ",");
                    }
                }
            }
            System.out.println();
        }
    }


    @Test
    public void updateData() {
        String tableName = "testNameSpace:student";
        String rowkey = "rowkey_0";
        String family = "cf1";
        String columnKey = "username";
        String updatedata = "zhangsan";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            // 将新数据添加到put中
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnKey), Bytes.toBytes(updatedata));
            // 将put写入hbase
            table.put(put);
            System.out.println("数据更新完成");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 删除指定表的某个列值
     */
    @Test
    public void deleteData() {
        String tableName = "student";
        TableName tName = TableName.valueOf(tableName);
        try {
            Table table = connection.getTable(tName);
            Delete del = new Delete(Bytes.toBytes("rowkey_0"));
            del.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
            table.delete(del);
            System.out.println("删除");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * @description: 删除某条rowkey
     */
    @Test
    public void deleteRow() {
        String tableName = "testNameSpace:student";
        String rowkey = "rowkey_4";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            // 通过行键删除一整行的数据
            Delete deleteRow = new Delete(Bytes.toBytes(rowkey));
            table.delete(deleteRow);
            System.out.println(rowkey + "已被删除");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }


    /**
     * @description: 关闭admin对象和connection对象
     */
    @Test
    @After
    public void closeConnect() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
