import java.util.*;
import java.util.stream.Stream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URISyntaxException;
import java.nio.file.*;

public class DBApp implements DBAppInterface {

    // vector to hold table objects
    private Vector<Table> tables;
    // path to realtions folder
    Path pathToRelations;
    // max number of rows per page
    static int maxPerPage;
    // TODO to be added Max number of keys in Index_Buckets


    // static block to init GLOBAL_PARAMS
    static {
        try {
            Path ptc = Paths.get(Resources.getResourcePath(), "DBApp.config");
            maxPerPage = Integer.parseInt(Files.readAllLines(ptc).get(0).split("=")[1].trim());
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
            System.out.println("[ERROR] somthing habben when reading DBApp.config");
        }
    }

    public DBApp() {
        init();
    }

    @Override
    public void init() {
        // check for the table directory
        try{
            pathToRelations = Paths.get(Resources.getResourcePath(), "tables");
            if(!Files.exists(pathToRelations)){
                Files.createDirectories(pathToRelations);
            }
        }catch(IOException | URISyntaxException e){
            System.out.println("[ERROR] someting habbens when checking for the tables directory");
            e.printStackTrace();
        }

        // TODO need to be tested
        // check for saved tables and load them if any
        try {
            Stream<Path> tablesPaths = Files.walk(pathToRelations).filter(Files::isRegularFile);
            Iterator itr = tablesPaths.iterator();
            while(itr.hasNext()){
                // deserialize all the save table objects
                FileInputStream file = new FileInputStream(pathToRelations.toString());
                ObjectInputStream in = new ObjectInputStream(file);
                Table t = (Table) in.readObject();
                this.tables.add(t);
            }
            tablesPaths.close();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        Table t = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);
        this.tables.add(t);
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException {
        // TODO to be added later
    }

    @Override
    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {

    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        this._getTable(tableName).delete(columnNameValue);
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    private Table _getTable(String tableName) {
        for (Table t : tables)
            if (t.name.equals(tableName))
                return t;
        return null;
    }

    public static void main(String args[]) {

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
        // Tuple t1 = new Tuple(1, new Integer(100), new String("athis is the first
        // Tuple")),
        // t2 = new Tuple(1, new Integer(100), new String("bthis is the first Tuple"));
        // System.out.println(t1.compareTo(t2));

        // int data[] = new int[]{10,102,45,123,5123,12};
        // Vector<Tuple> page = new Vector<Tuple>();
        // for(int d : data){
        //     Hashtable<String, Object> htb = new Hashtable<String, Object>();
        //     htb.put("id", d);
        //     page.add(new Tuple("id", htb));
        // }
        // System.out.println(page);
        // Collections.sort(page);
        // System.out.println(page);

        // DBApp p = new DBApp();
        // System.out.printf("[DONE] %d \n", DBApp.maxPerPage);
        System.out.println(DBApp.class.getResource(""));
    }

}
