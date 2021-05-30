import java.util.*;
import java.util.stream.Stream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URISyntaxException;
import java.nio.file.*;

/**
 * 
 * 
 * Please Read the README.md File
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
    static int maxPerIndexPage;
    // static block to init GLOBAL_PARAMS
    static {
        try {
            Path ptc = Paths.get(Resources.getResourcePath(), "DBApp.config");
            List<String> lines = Files.readAllLines(ptc);
            Hashtable<String, Integer> configs = new Hashtable<>();
            for (String line : lines) {
                String nameValue[] = line.split("=");
                configs.put(nameValue[0], Integer.parseInt(nameValue[1]));
            }
            if (!configs.containsKey("MaximumRowsCountinPage") || !configs.containsKey("MaximumKeysCountinIndexBucket"))
                throw new DBAppException();
            maxPerPage = configs.get("MaximumRowsCountinPage");
            maxPerIndexPage = configs.get("MaximumKeysCountinIndexBucket");
        } catch (IOException | URISyntaxException | DBAppException e) {
            System.out.println("[ERROR] somthing habben when reading DBApp.config");
        }
    }

    public DBApp() {
        tables = new Vector<Table>();
    }

    @Override
    public void init() {
        // check for the metadata file
        try {
            Path pathToMetaData = Paths.get(Resources.getResourcePath(), "metadata.csv");
            if (!Files.exists(pathToMetaData)) {
                Files.createFile(pathToMetaData);
            }
        } catch (IOException | URISyntaxException e) {
            System.out.println("[ERROR] someting habbens when checking for the metadata.csv file");
            e.printStackTrace();
        }

        // check for the data directory & tables directory
        try {
            Path pathToData = Paths.get(Resources.getResourcePath(), "data");
            pathToRelations = Paths.get(pathToData.toString(), ".tables");

            if (!Files.exists(pathToData)) {
                Files.createDirectories(pathToData);
                Files.createDirectories(pathToRelations);
            } else {
                if (!Files.exists(pathToRelations)) {
                    Files.createDirectories(pathToRelations);
                }
            }
        } catch (IOException | URISyntaxException e) {
            System.out.println("[ERROR] someting habbens when checking for the tables directory");
            e.printStackTrace();
        }

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
            System.out.printf("[ERROR] something habbens when openning table %s", tableName);
            throw new DBAppException();
        } else {
            t.update(clusteringKeyValue, columnNameValue);
        }
    }

    @Override
    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Table table = this._getTable(tableName);
        if (table == null) {
            System.out.printf("[ERROR] something habbens when openning table %s\n", tableName);
            throw new DBAppException();
        }
        table.delete(columnNameValue);
    }

    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException, ClassNotFoundException {
        Table table = this._getTable(tableName);
        if(table == null){
            System.out.printf("[ERROR] something habbens when openning table %s\n", tableName);
            throw  new DBAppException();
        }
        table.createIndex(columnNames);
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {

        return null;
    }

    // ? public to ease the test rahter than using the reflection api
    public Table _getTable(String tableName) {
        for (Table t : tables) {
            if (t.name.equals(tableName)) {
                return t;
            }
        }
        return null;
    }

}
