import java.util.*;
import java.util.stream.Stream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 
 * 
 * Please Read the README.md File
 * 
 * 
 * 
 **/

public class DBApp implements DBAppInterface {


    // GLOBAL OPTIONS FOR THE DB
    public static boolean ALLOW_DUBLICATES = false;
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
        tables = new Vector<Table>();
    }

    @Override
    public void init() {
        // check for the table directory
        try {
            pathToRelations = Paths.get(Resources.getResourcePath(), "data");
            if (!Files.exists(pathToRelations)) {
                Files.createDirectories(pathToRelations);
            }
        } catch (IOException | URISyntaxException e) {
            System.out.println("[ERROR] someting habbens when checking for the tables directory");
            e.printStackTrace();
        }

        // TODO need to be tested
        // check for saved tables and load them if any
        try {
            Stream<Path> tablesPaths = Files.walk(pathToRelations).filter(Files::isRegularFile);
            Iterator itr = tablesPaths.iterator();
            while (itr.hasNext()) {
                // deserialize all the save table objects
                FileInputStream file = new FileInputStream(itr.next().toString());
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
        
        Table t = this._getTable(tableName);
        if (t == null) {
            throw new DBAppException();
        } else {
            t.insert(colNameValue);
        }
    }

    @Override
    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        Table t = this._getTable(tableName);
        if (t == null) {
            throw new DBAppException();
        } else {
            // System.out.printf("[LOG] this is the %s table %s and it has %d pages %s\n", t.name, t.toString(),
            //         t.buckets.size(), t.buckets.toString());
            t.update(clusteringKeyValue, columnNameValue);
        }
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Table table = this._getTable(tableName);
        if (table == null) {
            System.out.printf("[ERROR] something habbens when openning table %s", tableName);
            throw new DBAppException();
        }
        table.delete(columnNameValue);
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        return null;
    }

    // FOR [DEBUGING]
    // public void printAllSizes() {
    //     System.out.printf("[NUM] of tables is %d\n", this.tables.size());
    //     for (Table t : this.tables)
    //         System.out.printf("%s %s %s\n", t.name, t.toString(), t.buckets.toString());
    // }

    // TODO convert to private
    public Table _getTable(String tableName) {
        for (Table t : tables){
            if (t.name.equals(tableName)) {
                // System.out.printf("[LOG] this is the %s table %s and it has %d pages %s\n", t.name, t.toString(),
                //         t.buckets.size(), t.buckets.toString());

                return t;
            }
        }
        return null;
    }

    public static void main(String args[]) {

        // !tmp test

        
    }

}
