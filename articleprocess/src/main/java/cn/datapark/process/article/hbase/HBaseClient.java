package cn.datapark.process.article.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by eason on 16/1/20.
 */
public class HBaseClient {

    private static final Logger LOG = Logger.getLogger(HBaseClient.class);

    //map key的常量 rowkey ,列族,cell value
    public static final String ROWKEY = "rowkey";
    public static final String COLUMN_FAMILY = "columnfamily";
    public static final String COLUMN_NAME = "columnname";
    public static final String CELL_VALUE = "cellvalue";

    private Configuration hbaseConf = null;

    private Connection conn = null;

    synchronized public void initClient(String zookeeperQuorum, String zookeeperPort, String isClusterDisctributed) {
        if (conn != null) {
            LOG.info("HBaseClient already load,close before reinit");
            return;
        }
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperPort);
        hbaseConf.set("hbase.cluster.distributed", isClusterDisctributed);
        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure");//使用horton ambari安装的集群需要使用该设置
        try {
            conn = ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        conn = null;
        LOG.info("HBase Connection close");
    }


    public void createTable(String nameSpace, String tableName, String columnFamily) {


        Admin admin = null;
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        boolean isNameSpaceExist = false;
        try {
            NamespaceDescriptor npd = admin.getNamespaceDescriptor(nameSpace);
            isNameSpaceExist = true;
            LOG.info("Namespace " + nameSpace + " exists");
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {

            if (!isNameSpaceExist) {
                LOG.info("creating namespace " + nameSpace);
                admin.createNamespace(NamespaceDescriptor.create(nameSpace).build());
                LOG.info("Namespace " + nameSpace + "create success!");
            }

            String tableNameWithNameSpace = nameSpace + ":" + tableName;

            TableName tableNameObj = TableName.valueOf(tableNameWithNameSpace);
            if (admin.tableExists(tableNameObj)) {
                LOG.error("Table " + tableNameWithNameSpace + " exists!");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableNameWithNameSpace);
                tableDesc.addFamily(new HColumnDescriptor(columnFamily));
                admin.createTable(tableDesc);
                LOG.info("Create " + tableName + ":" + columnFamily + " success");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, String columnFamily) throws IOException {
        createTable("default", tableName, columnFamily);
    }

    public void createTable(String tableName) throws IOException {
        createTable("default", tableName, "d");
    }


    public boolean put(String nameSpace, String tableName, String columnFamily, String colName, String value, String rowKey) {

        boolean isPutSuc = false;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(nameSpace + ":" + tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(colName), Bytes.toBytes(value));
            table.put(put);
            isPutSuc = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isPutSuc;
    }


    public boolean batchPuts(String nameSpace, String tableName, List<Put> puts) {
        boolean isPutSuc = false;
        Table table = null;

        try {
            table = conn.getTable(TableName.valueOf(nameSpace + ":" + tableName));
            table.put(puts);
            isPutSuc = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return isPutSuc;
    }

    /**
     * 默认namespace为default
     *
     * @param tableName
     * @param valueList
     */
    public boolean batchPutMap(String tableName, List<Map<String, Object>> valueList) {
        return batchPutMap("default", tableName, valueList);
    }


    public boolean batchPutMap(String nameSpace, String tableName, List<Map<String, Object>> valueList) {

        List<Put> puts = new ArrayList<Put>();

        for (Map<String, Object> row : valueList) {
            Put put = new Put(row.get(ROWKEY).toString().getBytes());
            put.addColumn(Bytes.toBytes(row.get(COLUMN_FAMILY).toString()), Bytes.toBytes(row.get(COLUMN_NAME).toString()), Bytes.toBytes(row.get(CELL_VALUE).toString()));
            puts.add(put);
        }

        return batchPuts(nameSpace, tableName, puts);

    }


    public byte[] getCellValueByRowkey(String rowkey, String nameSpace, String tableName, String columnFamilyName, String ColumnName) {

        Table table = null;
        try {

            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));
            Get get = new Get(rowkey.getBytes());
            get.addFamily(columnFamilyName.getBytes());
            Result result = table.get(get);

            if (result != null) {
                byte[] b = result.getValue(columnFamilyName.getBytes(), ColumnName.getBytes());
                return b;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public byte[] getRowkeybyScanCompareOp(String scanPatten, String nameSpace, String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf((nameSpace + ":" + tableName).getBytes()));

            Scan scan = new Scan();
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(scanPatten));
            scan.setFilter(filter);
            ResultScanner scanner;
            scanner = table.getScanner(scan);

            for (Result res : scanner) {
                return res.getRow();
            }
        } catch (Exception e){
            e.fillInStackTrace();
        }
        finally {

            try {
                if (table != null)
                    table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;

    }

    public static void main(String arg[]) throws IOException {
        HBaseClient client = new HBaseClient();
//        client.initClient("192.168.0.112,192.168.0.113,192.168.0.114", "2181", "true");
        client.initClient("192.168.31.63,192.168.31.61,192.168.31.62", "2181", "true");
//        client.createTable("dpa", "test", "cf0");
//
//        client.put("dpa", "test", "cf0", "aaaa", "222222222", "RRRRRRRRR123");
//        byte[] r=client.getCellValueByRowkey("cn.busines20160320234914http://cn.businessoffashion.com/2015/05/south-korea-climbing-the-ranks-of-asias-a-league-2.html","dpa", "articles", "d", "src_url");
        byte[] r = client.getRowkeybyScanCompareOp(".*http://www.vogue.com.cn/people/movie/pic_151334b0e27cb092.html", "dpa", "articles");
        String result = new String(r);
        System.out.println(result);
        client.close();
    }
}
