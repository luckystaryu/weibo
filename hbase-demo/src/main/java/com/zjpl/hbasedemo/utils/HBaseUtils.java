package com.zjpl.hbasedemo.utils;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.io.IOException;
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
    private static String springApplicationXML="application.yml";

    /**
     * 1.初始化HBASE
     */
    public void init(){
        conf = HBaseConfiguration.create();
        prop = new Properties();
        try {
            String hbaseConfigFile =null;
            //String env =
            prop.load(HBaseUtils.class.getResourceAsStream(hbaseConfigFile));
            String rootdir = prop.getProperty("hbase.rootDir");
            String quorum =prop.getProperty("hbase.zkQuorum");
            String zkBasePath = prop.getProperty("hbase.zkBasePath");
            conf.set("hbase.zookeepeer.quorum",quorum);
            conf.set("hbase.rootdir",rootdir);
            conf.set("zookeeper.znode.parent",zkBasePath);
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
    @SuppressWarnings("resource")
    public boolean createTableBySplitKeys(String tableName, List<String> columnFamily,byte[][] splitKeys,boolean forceDeleteIfExist){
      Admin admin =null;
      TableName tableNameEntity = TableName.valueOf(tableName);
        try {
            admin = getConn().getAdmin();
            if(StringUtils.isEmpty(tableName) || columnFamily ==null
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
                if(!StringUtils.isEmpty(value)){
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
            if(!StringUtils.isEmpty(value)){
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
}
