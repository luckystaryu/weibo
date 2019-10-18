package com.zjpl.hbasedemo.utils;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;


import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 1.创建HBASE连接池
 * 2.创建HBASE表空间
 * 3.判断表是否存在
 * 4.创建表
 */
public class HBaseUtils {

    private static Configuration conf;
    private static Connection conn;
    private static HBaseUtils hBaseUtils;
    private static Properties prop;

    /**
     * 1.初始化HBASE
     */
    public void init(){
        conf = HBaseConfiguration.create();
        try {
            YmlUtil ymlUtil = new YmlUtil();
            String quorum =ymlUtil.getStrYmlValue("hbase.zookeeper.quorum");
            String hbasePort = ymlUtil.getStrYmlValue("hbase.zookeeper.property.clientPort");
           // String zkBasePath = prop.getProperty("hbase.zkBasePath");
            conf.set("hbase.zookeeper.quorum", quorum);
            conf.set("hbase.zookeeper.property.clientPort", hbasePort);
           // conf.set("hbase.rootdir",rootdir);
           // conf.set("zookeeper.znode.parent",zkBasePath);
            conf.set("hbase.client.pause","500");
            conf.set("hbase.client.retries.number","5");
            conf.set("hbase.rpc.timeout","9000");
            conf.set("hbase.client.operation.timeout","9000");
            conf.set("hbase.client.scanner.timeout.period","10000");
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public HBaseUtils() {
    }

    /**
     * 创建Hbase线程
     * @return
     */
    public static HBaseUtils getInstance(){
        if(hBaseUtils ==null){
            synchronized (HBaseUtils.class){
                if(hBaseUtils ==null){
                    hBaseUtils = new HBaseUtils();
                    hBaseUtils.init();
                }
            }
        }
        return hBaseUtils;
    }

    /**
     * 获取HBASE连接信息
     * @return
     */
    public Connection getConn(){
        if(conn == null || conn.isClosed()){
            try {
                System.out.println("hbase.zookeepeer.quorum===========>"+conf.get("hbase.zookeepeer.quorum"));
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * 获取HBASE配置信息
     * @return
     */
    public Configuration getConfiguration(){
        return conf;
    }
   // @SuppressWarnings("resource")
    public boolean createTableBySplitKeys(String tableName, List<String> columnFamily,byte[][] splitKeys,boolean forceDeleteIfExist){
      Admin admin =null;
      TableName tableNameEntity = TableName.valueOf(tableName);
        try {
            admin = getConn().getAdmin();
            if(StringUtils.isBlank(tableName) || columnFamily ==null
            || columnFamily.size() <0){

            }
            //判断表是否存在
            if(admin.tableExists(tableNameEntity)){
                if(forceDeleteIfExist){
                    admin.disableTable(tableNameEntity);
                    admin.deleteTable(tableNameEntity);
                }else{
                    return true;
                }
            }
            HTableDescriptor tableDescriptor =new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                tableDescriptor.addFamily(new HColumnDescriptor(cf));
            }
            admin.createTable(tableDescriptor,splitKeys);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 批量插入
     * @param tableName
     * @param columnFamilyName
     * @param rowKey
     * @param objectList
     * @throws IOException
     */
    public void saveOrUpdateObjectList2HBase(String tableName,String columnFamilyName,String rowKey,List<Object> objectList) throws IOException {
        conn = getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> putList = new ArrayList<Put>();
        objectList.parallelStream().forEach(e->{
            PropertyDescriptor[] pds = PropertyUtils.getPropertyDescriptors(e.getClass());
            BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(e);
            for (PropertyDescriptor propertyDescriptor : pds) {
                String properName = propertyDescriptor.getName();
                if("class".equals(properName) || ("ROW".equals(properName)))
                    continue;
                System.out.println("properName = ["+properName+"]");
                String value =(String) beanWrapper.getPropertyValue(properName);
                System.out.println("value =["+value+"]");
                if(!StringUtils.isBlank(value)){
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(columnFamilyName),Bytes.toBytes(properName),Bytes.toBytes(value));
                    putList.add(put);
                }

            }

            try {
                BufferedMutator mutator = null;
                TableName tName = TableName.valueOf(tableName);
                BufferedMutatorParams params = new BufferedMutatorParams(tName);
                params.writeBufferSize(5*1024*1024); //可以自己设定阈值 5M 达到5M则提交一次
                mutator = conn.getBufferedMutator(params);
                mutator.mutate(putList); //数据达到5M时会自动提交一次
                mutator.flush();
            } catch (IOException ex) {
                ex.printStackTrace();
            }finally {
                try {
                    table.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    /**
     * 批量插入
     * @param tableName
     * @param columnFamilyName
     * @param rowKey
     * @param object
     * @throws IOException
     */
    public void saveOrUpdateObject2HBase(String tableName,String columnFamilyName,String rowKey,Object object) throws IOException {
        conn = getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> putList = new ArrayList<Put>();
        Put put = null;
        PropertyDescriptor[] pds = PropertyUtils.getPropertyDescriptors(object.getClass());
        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(object);
        for (PropertyDescriptor propertyDescriptor : pds) {
            String properName = propertyDescriptor.getName();
            if("class".equals(properName) ||("ROW".equals(properName)))
                continue;
            System.out.println("properName = ["+properName+"]");
            String value =(String)beanWrapper.getPropertyValue(properName);
            if(!StringUtils.isBlank(value)){
                put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamilyName),
                        Bytes.toBytes(properName),
                        Bytes.toBytes(value));
                putList.add(put);
            }
        }

        try {
            BufferedMutator mutator =null;
            TableName tName = TableName.valueOf(tableName);
            BufferedMutatorParams params = new BufferedMutatorParams(tName);
            params.writeBufferSize(5*1024*1024);//可以自己设定阈值 5M 达到5M则提交一次
            mutator = conn.getBufferedMutator(params);
            mutator.mutate(putList); //数据量达到5M时会自动提交一次
            mutator.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            table.close();
        }
    }

    /**
     * 根据rowkey List 批量查询
     * @param rowkeyList
     * @param tableName
     * @return
     */
    public List<String> qurryTableBatch(List<String> rowkeyList,String tableName) throws IOException {
        List<Get> getList = new ArrayList();
        List dataList = new ArrayList();
        Table table = getConn().getTable(TableName.valueOf(tableName));
        for (String rowkey : rowkeyList) {
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList);
        for (Result result : results) {
            //对返回的结果集进行操作
            for (Cell cell : result.rawCells()) {
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                dataList.add(value);
            }
        }
        return dataList;
    }
    public List<Object> queryDataObjByRowkeyList(List<String> rowkeyList,String tableName,Class objClass) throws Exception {
        List<Get> getList = new ArrayList<>();
        List dataList = new ArrayList();
        Table table = getConn().getTable(TableName.valueOf(tableName));
        for (String rowkey : rowkeyList) {
            //先把rowkey 加到get里，再把get装list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results = table.get(getList); //重点在这，直接查getList<Get>
        List<Object> dataResult = new ArrayList<>();
        for (Result result : results) {
            //对返回的结果集进行操作
            Object obj = objClass.newInstance();
            Object objResult = convertHbaseResult2Obj(result,obj);
            dataResult.add(objResult);
        }
        return dataResult;
    }

    private Object convertHbaseResult2Obj(Result result, Object obj) throws InvocationTargetException, IllegalAccessException {
        String qualifier =null;
        String value =null;
        String rowkey =null;
        for (Cell cell : result.listCells()) {
            Bytes.toString(CellUtil.cloneFamily(cell));
            rowkey = Bytes.toString(CellUtil.cloneRow(cell));
            qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            value = Bytes.toString(CellUtil.cloneValue(cell));
            if(!StringUtils.isBlank(value)){
                BeanUtils.setProperty(obj,qualifier,value);
                BeanUtils.setProperty(obj,"rowkey",rowkey);
            }
        }
        return  obj;
    }

    /**
     * 单条插入数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param data
     * @throws IOException
     */
    public void putData(String tableName,String rowkey,String columnFamily,String column,String data) throws IOException {
        Table table = getConn().getTable(TableName.valueOf(tableName));
        try{
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
            table.put(put);
        }finally {
            table.close();
        }
    }

    ThreadLocal<List<Put>> threadLocal = new ThreadLocal<List<Put>>();

    /**
     * 批量添加记录到HBase表，同一线程要保证对相同表进行添加操作
     * @param tableName
     * @param rowkey
     * @param cf
     * @param column
     * @param value
     */
    public void bulkput(String tableName,String rowkey,String cf,String column,String value) throws IOException {
        HTable table = (HTable) getConn().getTable(TableName.valueOf(tableName));
        try{
            List<Put> list = threadLocal.get();
            if(list ==null){
                list = new ArrayList<Put>();
            }
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
            list.add(put);
            if(list.size() >= 500){
                //超过500条数据，批量提交

                System.out.println("begin to batch put list");
                table.put(list);
                list.clear();
            }else{
                threadLocal.set(list);
            }
        }finally {
            table.close();
        }

    }

    /**
     * 起始值和终止值查询
     * @param tableName
     * @param startKey
     * @param stopKey
     * @param num
     * @return
     * @throws IOException
     */
    public List<Result> getStartEndRow(final String tableName,final String startKey,final String stopKey,final int num) throws IOException {
        List<Result> list = new ArrayList<>();
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        if(num >0){
//            //过滤获取的条数
//            Filter filterNum = new PageFilter(num); //每页展示条数
//            fl.addFilter(filterNum);
//        }
        //过滤器的添加
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startKey));
        scan.setStopRow(Bytes.toBytes(stopKey));
        scan.setFilter(fl); //为查询设置过滤器的list
        HTable table = (HTable) getConn().getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            list.add(result);
        }
        return list;
    }

    /**
     * 前缀查询
     * @param tableName
     * @param prefixStr
     * @param num
     * @return
     * @throws IOException
     */
    public List<Result> getPrefixRow(final String tableName,final String prefixStr,int num) throws IOException {
        List<Result> list = new ArrayList<>();
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter prefixFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes(prefixStr)));
        if(num>0){
            //过滤获取的条数
            Filter filterNum = new PageFilter(num);//每页展示条数
            fl.addFilter(filterNum);
        }
        //过滤器的添加
        fl.addFilter(prefixFilter);
        Scan scan = new Scan();
        scan.setFilter(fl); //为查询设置过滤器的list
        HTable table = (HTable) getConn().getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            list.add(result);
        }
        return list;
    }

    /**
     * 单条查询GET
     * @param tableName
     * @param rowkey
     * @return
     * @throws IOException
     */
    public Result getDataByRowKey(String tableName,String rowkey) throws IOException {
        HTable table = (HTable) getConn().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);
        return result;
    }

    /**
     * 查询全量数据
     * @param tableName
     * @return
     * @throws IOException
     */
    public List<Result> getAllData(String tableName) throws IOException {
        HTable table = (HTable) getConn().getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        List<Result> list = new ArrayList<>();
        for (Result result : resultScanner) {
            list.add(result);
        }
        return list;
    }

    /**
     * 根据rowKey删除数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void delDataByRowKey(String tableName,String rowKey) throws IOException {
        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
    }

    /**
     * 创建非预分区表
     * @param tableName
     * @param columnFamily
     */
    public boolean createTable(String tableName,List<String> columnFamily,Integer maxVersion,Boolean forceDeleteIfExist) throws IOException {
        Admin admin =null;
        TableName tableNameEntity = TableName.valueOf(tableName);
        try {
            admin = getConn().getAdmin();
            if(StringUtils.isBlank(tableName) || columnFamily ==null
                    || columnFamily.size() <0){

            }
            //判断表是否存在
            if(admin.tableExists(tableNameEntity)){
                if(forceDeleteIfExist){
                    admin.disableTable(tableNameEntity);
                    admin.deleteTable(tableNameEntity);
                }else{
                    return true;
                }
            }
            HTableDescriptor tableDescriptor =new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setMaxVersions(maxVersion);
                tableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    //大表的count非常慢，通过这种方式计算行数
    public static long rowCount(String tableName){
        long rowCount = 0;
       // @SuppressWarnings("resource")
        AggregationClient aggregationClient = new AggregationClient(conf);
        Scan scan = new Scan();
        try {
            rowCount =aggregationClient.rowCount(TableName.valueOf(tableName),
                    new LongColumnInterpreter(),scan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return rowCount;
    }
}
