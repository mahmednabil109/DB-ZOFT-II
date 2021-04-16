import java.io.Serializable;
import java.util.*;


public class DBApp implements DBAppInterface{

    @Override
    public void init() {
            
    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        // TODO Auto-generated method stub
        return null;
    }

    public static void main (String args[]){
        
        // !tmp test
        
        // Tuple t = new Tuple(1, new Integer(10), new String("Hamada"));
        // System.out.println(t);
        // System.out.println(t.getPrimeKey());
        // PageInfo p = new PageInfo();
        // System.out.println(p instanceof Serializable);
        // System.out.println("Hello Zoft!");

        // // check how to test for the types
        // Object i = new Integer(10);
        // Class o = i.getClass();
        // Object j = o.cast(i);
        // System.out.println(j.getClass());
        // System.out.println((i.getClass().toString()).split(" ")[1]);


        // test the comareto function in the Tuple calss
        Tuple t1 = new Tuple(1, new Integer(100), new String("athis is the first Tuple")),
            t2 = new Tuple(1, new Integer(100), new String("bthis is the first Tuple"));
        System.out.println(t1.compareTo(t2));

        int data[] = new int[]{10,102,45,123,5123,12};
        Vector<Tuple> page = new Vector<Tuple>();
        for(int d : data){
            page.add(new Tuple(0, new Integer(d)));
        }
        System.out.println(page);
        Collections.sort(page);
        System.out.println(page);

    }

}
