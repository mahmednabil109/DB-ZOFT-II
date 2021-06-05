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
    public static boolean ALLOW_DUBLICATES = true;
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
                configs.put(nameValue[0].trim(), Integer.parseInt(nameValue[1].trim()));
            }
            if (!configs.containsKey("MaximumRowsCountinPage") || !configs.containsKey("MaximumKeysCountinIndexBucket"))
                throw new DBAppException();
            maxPerPage = configs.get("MaximumRowsCountinPage");
            maxPerIndexPage = configs.get("MaximumKeysCountinIndexBucket");
        } catch (IOException | URISyntaxException | DBAppException e) {
            System.out.println("[ERROR] somthing habben when reading DBApp.config");
            e.printStackTrace();
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
        // !D
        System.out.println("[LOG] Start creating the index");
        Table table = this._getTable(tableName);
        if (table == null) {
            System.out.printf("[ERROR] something habbens when openning table %s\n", tableName);
            throw new DBAppException();
        }
        table.createIndex(columnNames);
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
        Table table = this._getTable(sqlTerms[0]._strTableName);
        if (table == null) {
            System.out.printf("[ERROR] something habbens when openning table %s\n", sqlTerms[0]._strTableName);
            throw new DBAppException();
        }
        if(sqlTerms.length!=1 && sqlTerms.length != 0){
            Object[] all = mergeStatement(sqlTerms, arrayOperators);
            Vector<Object> allPost = convertToPost(all);
            allPost = doingAllAnd(allPost, table);
            allPost = doingAllOr(allPost, table);
            allPost = doingAllXor(allPost, table);
            System.out.println("ALLPOST: " + allPost);
            return ((HashSet<Tuple>) allPost.get(0)).iterator();
        }
        else if(sqlTerms.length != 0){
            Vector<SQLTerm> sqlTerm=new Vector<>();
            sqlTerm.add(sqlTerms[0]);
            Vector<Object> result=table.getBestIndex(sqlTerm);
            if(result.size()!=0){
                Index index=(Index) result.get(0);
                return index.search(sqlTerm).iterator();
            }
            else{
                return table.search(sqlTerm).iterator();
            }
        }
        else{
            return table.retrieveALl().iterator();
        }
    }

    public static Object[] mergeStatement(SQLTerm[] sqlTerms, String[] arrayOperators) {
        Object[] all = new Object[sqlTerms.length + arrayOperators.length];
        for (int i = 0; i < all.length; i++) {
            if (i % 2 == 0) {
                all[i] = sqlTerms[i / 2];
            } else {
                all[i] = arrayOperators[i / 2];
            }
        }
        return all;
    }

    public static Vector<Object> convertToPost(Object[] all) {
        Vector<Object> allPost = new Vector<Object>();
        Stack<Object> opr = new Stack<Object>();
        for (int i = 0; i < all.length; i++) {
            if (i % 2 == 0) {
                allPost.add(all[i]);
            } else {
                if (opr.empty()) {
                    opr.add(all[i]);
                } else {
                    while ((!opr.isEmpty()))
                        if (compareOpr((String) opr.peek(), (String) all[i])) {
                            allPost.add(opr.pop());
                        } else {
                            break;
                        }
                    opr.add(all[i]);
                }
            }
        }
        while (opr.size() != 0) {
            allPost.add(opr.pop());
        }
        return allPost;
    }

    public static Vector<Object> doingAllOr(Vector<Object> allPost, Table table) throws DBAppException {
        Vector<SQLTerm> searchTerms = new Vector<SQLTerm>();
        int firstOrIndex = getNextOr(allPost, 0);
        int nextOr = getNextOr(allPost, firstOrIndex), currentOr = 0;
        while (firstOrIndex != currentOr) {
            Vector<HashSet<Tuple>> results = new Vector<HashSet<Tuple>>();
            currentOr = firstOrIndex;
            if (firstOrIndex != 0) {
                if (allPost.get(firstOrIndex - 2) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(firstOrIndex - 2));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(firstOrIndex - 2));
                }
                if (allPost.get(firstOrIndex - 1) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(firstOrIndex - 1));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(firstOrIndex - 1));
                }
            }
            while (nextOr == currentOr + 2) {
                if (allPost.get(currentOr + 1) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(currentOr + 1));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(currentOr + 1));
                }
                currentOr = nextOr;
                nextOr = getNextOr(allPost, currentOr);
            }
            while (searchTerms.size() != 0) {
                Vector<Object> bestIndex = table.getBestIndex(searchTerms);
                if (bestIndex.size() != 0) {
                    Index index = (Index) bestIndex.get(0);
                    HashSet<String> maxCon = (HashSet<String>) bestIndex.get(1);
                    Vector<SQLTerm> maxVector = new Vector<SQLTerm>();
                    for (SQLTerm searchTerm : searchTerms) {
                        if (maxCon.contains(searchTerm._strColumnName))
                            maxVector.add(searchTerm);
                    }
                    for (SQLTerm o : maxVector) {
                        searchTerms.remove(o);
                    }
                    // result ==null || {} break
                    HashSet<Tuple> result = index.search(maxVector);
                    results.add(result);
                } else {
                    HashSet<Tuple> result = table.search(searchTerms);
                    searchTerms = new Vector<>();
                    results.add(result);
                }
            }
            while (results.size() != 0 && results.size() != 1) {
                results.add(mergeAll(results.remove(0), results.remove(results.size() - 1)));
            }
            for (int i = currentOr; i >= firstOrIndex - 2; i--) {
                allPost.remove(i);
            }
            if (results.size() != 0)
                allPost.add(firstOrIndex - 2, results.get(0));
            firstOrIndex = getNextOr(allPost, firstOrIndex);
            currentOr = getNextAnd(allPost, firstOrIndex);
            nextOr = getNextOr(allPost, firstOrIndex);
        }
        return allPost;
    }

    public static Vector<Object> doingAllXor(Vector<Object> allPost, Table table) throws DBAppException {
        Vector<SQLTerm> searchTerms = new Vector<SQLTerm>();
        int firstXorIndex = getNextXor(allPost, 0);
        int nextXor = getNextXor(allPost, firstXorIndex), currentXor = 0;
        while (firstXorIndex != currentXor) {
            Vector<HashSet<Tuple>> results = new Vector<HashSet<Tuple>>();
            currentXor = firstXorIndex;
            if (firstXorIndex != 0) {
                if (allPost.get(firstXorIndex - 2) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(firstXorIndex - 2));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(firstXorIndex - 2));
                }
                if (allPost.get(firstXorIndex - 1) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(firstXorIndex - 1));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(firstXorIndex - 1));
                }
            }
            while (nextXor == currentXor + 2) {
                if (allPost.get(currentXor + 1) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(currentXor + 1));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(currentXor + 1));
                }
                currentXor = nextXor;
                nextXor = getNextXor(allPost, currentXor);
            }
            while (searchTerms.size() != 0) {
                Vector<Object> bestIndex = table.getBestIndex(searchTerms);
                if (bestIndex.size() != 0) {
                    Index index = (Index) bestIndex.get(0);
                    HashSet<String> maxCon = (HashSet<String>) bestIndex.get(1);
                    Vector<SQLTerm> maxVector = new Vector<SQLTerm>();
                    for (SQLTerm searchTerm : searchTerms) {
                        if (maxCon.contains(searchTerm._strColumnName))
                            maxVector.add(searchTerm);
                    }
                    for (SQLTerm o : maxVector) {
                        searchTerms.remove(o);
                    }
                    // result ==null || {} break
                    HashSet<Tuple> result = index.search(maxVector);
                    results.add(result);
                } else {
                    HashSet<Tuple> result = table.search(searchTerms);
                    searchTerms = new Vector<>();
                    results.add(result);
                }
            }
            while (results.size() != 0 && results.size() != 1) {
                results.add(xorAll(results.remove(0), results.remove(results.size() - 1)));
            }
            for (int i = currentXor; i >= firstXorIndex - 2; i--) {
                allPost.remove(i);
            }
            if (results.size() != 0)
                allPost.add(firstXorIndex - 2, results.get(0));
            firstXorIndex = getNextXor(allPost, firstXorIndex);
            currentXor = getNextAnd(allPost, firstXorIndex);
            nextXor = getNextXor(allPost, firstXorIndex);
        }
        return allPost;
    }

    public static Vector<Object> doingAllAnd(Vector<Object> allPost, Table table) throws DBAppException {
        Vector<SQLTerm> searchTerms = new Vector<SQLTerm>();
        int firstAndIndex = getNextAnd(allPost, 0);
        int nextAnd = getNextAnd(allPost, firstAndIndex), currentAnd = 0;
        // System.out.println(firstAndIndex);
        while (firstAndIndex != currentAnd) {
            Vector<HashSet<Tuple>> results = new Vector<HashSet<Tuple>>();
            currentAnd = firstAndIndex;
            if (firstAndIndex != 0) {
                if (allPost.get(firstAndIndex - 2) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(firstAndIndex - 2));
                    // System.out.println(searchTerms);
                } else {
                    results.add((HashSet<Tuple>) allPost.get(firstAndIndex - 2));
                }
                if (allPost.get(firstAndIndex - 1) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(firstAndIndex - 1));
                } else {
                    System.out.println("allpost: " + allPost);
                    results.add((HashSet<Tuple>) allPost.get(firstAndIndex - 1));
                }
            }
            while (nextAnd == currentAnd + 2) {
                if (allPost.get(currentAnd + 1) instanceof SQLTerm) {
                    searchTerms.add((SQLTerm) allPost.get(currentAnd + 1));
                } else {
                    results.add((HashSet<Tuple>) allPost.get(currentAnd + 1));
                }
                currentAnd = nextAnd;
                nextAnd = getNextAnd(allPost, currentAnd);
            }
            while (searchTerms.size() != 0) {
                Vector<Object> bestIndex = table.getBestIndex(searchTerms);
                if (bestIndex.size() != 0) {
                    Index index = (Index) bestIndex.get(0);
                    HashSet<String> maxCon = (HashSet<String>) bestIndex.get(1);
                    Vector<SQLTerm> maxVector = new Vector<SQLTerm>();
                    for (SQLTerm searchTerm : searchTerms) {
                        if (maxCon.contains(searchTerm._strColumnName))
                            maxVector.add(searchTerm);
                    }
                    for (SQLTerm o : maxVector) {
                        searchTerms.remove(o);
                    }
                    // result ==null || {} break
                    HashSet<Tuple> result = index.search(maxVector);
                    results.add(result);
                } else {
                    HashSet<Tuple> result = table.search(searchTerms);
                    searchTerms = new Vector<>();
                    results.add(result);
                }
            }
            while (results.size() != 0 && results.size() != 1) {
                results.add(retainAll(results.remove(0), results.remove(results.size() - 1)));
            }
            for (int i = currentAnd; i >= firstAndIndex - 2; i--) {
                allPost.remove(i);
            }
            if (results.size() != 0)
                allPost.add(firstAndIndex - 2, results.get(0));
            firstAndIndex = getNextAnd(allPost, firstAndIndex);
            currentAnd = getNextAnd(allPost, firstAndIndex);
            nextAnd = getNextAnd(allPost, firstAndIndex);
        }
        return allPost;
    }

    public static HashSet<Tuple> retainAll(HashSet<Tuple> hashSet, HashSet<Tuple> hashSet2) {
        HashSet<Tuple> result = new HashSet<Tuple>();
        for (Tuple o : hashSet) {
            if (hashSet2.contains(o)) {
                result.add(o);
            }
        }
        return result;
    }

    public static HashSet<Tuple> xorAll(HashSet<Tuple> hashSet, HashSet<Tuple> hashSet2) {
        HashSet<Tuple> result = new HashSet<Tuple>();
        for (Tuple o : hashSet2) {
            if (hashSet.contains(o)) {
                continue;
            } else {
                result.add(o);
                System.out.println(o);
            }
        }
        for (Tuple o : hashSet) {
            if (hashSet2.contains(o)) {
                continue;
            } else {
                result.add(o);
                System.out.println(o);
            }
        }
        return result;
    }

    public static HashSet<Tuple> mergeAll(HashSet<Tuple> hashSet, HashSet<Tuple> hashSet2) {
        for (Tuple o : hashSet2) {
            if (hashSet.contains(o)) {
                continue;
            } else {
                hashSet.add(o);
            }
        }
        return hashSet;
    }

    public static int getNextAnd(Vector<Object> allPost, int index) {
        for (int i = index + 1; i < allPost.size(); i++) {
            if (allPost.get(i) instanceof String && ((String) allPost.get(i)).toLowerCase().equals("and"))
                return i;
        }
        return index;
    }

    public static int getNextOr(Vector<Object> allPost, int index) {
        for (int i = index + 1; i < allPost.size(); i++) {
            if (allPost.get(i) instanceof String && ((String) allPost.get(i)).toLowerCase().equals("or"))
                return i;
        }
        return index;
    }

    public static int getNextXor(Vector<Object> allPost, int index) {
        for (int i = index + 1; i < allPost.size(); i++) {
            if (allPost.get(i) instanceof String && ((String) allPost.get(i)).toLowerCase().equals("xor"))
                return i;
        }
        return index;
    }

    public static boolean compareOpr(String opr1, String opr2) {
        if (opr1.toLowerCase() == "and" || (opr1.toLowerCase() == "or" && opr2.toLowerCase() != "and")
                || (opr2.toLowerCase() == "xor")) {
            return true;
        }
        return false;
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

     // below method returns Iterator with result set if passed// strbufSQL is a select, otherwise returns null.
     public Iterator parseSQL(StringBuffer strbufSQL ) throws DBAppException{
        return null;
    }
}