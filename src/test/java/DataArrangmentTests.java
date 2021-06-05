import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.*;

import java.awt.Polygon;
import java.io.*;
import java.util.*;
import java.lang.reflect.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DataArrangmentTests {

    public static Vector<Tuple> page1 = new Vector<Tuple>(), page3 = new Vector<Tuple>();
    public static int tableSize = 0;

    @Test
    @Order(1)
    public void testSetPageSize() throws Exception {
        DBApp app = new DBApp();
        app.init();
        System.out.printf("[BEFORE] %d\n", app.maxPerPage);
        app = null;
        String configFilePath = "src/main/resources/";

        if (Files.exists(Paths.get(configFilePath, "DBApp.config"))) {
            configFilePath = Paths.get(configFilePath, "DBApp.config").toString();
        } else if (Files.exists(Paths.get(configFilePath, "DBApp.properties"))) {
            configFilePath = Paths.get(configFilePath, "DBApp.properties").toString();
        } else {
            throw new Exception("`DBApp.config` does not exist in resources directory");
        }

        List<String> config = Files.readAllLines(Paths.get(configFilePath));
        boolean lineFound = false;
        for (int i = 0; i < config.size(); i++) {
            if (config.get(i).toLowerCase().contains("page")) {
                config.set(i, config.get(i).replaceAll("\\d+", "250"));
                lineFound = true;
                break;
            }
        }

        if (!lineFound) {
            throw new Exception(
                    "Cannot set page size, make sure that key `MaximumRowsCountinTablePage` is present in DBApp.config");
        }

        Files.write(Paths.get(configFilePath), config);

        app = new DBApp();
        app.init();
        System.out.printf("[AFTER] %d\n", app.maxPerPage);
        app = null;
    }

    @Test
    @Order(2)
    public void testClearMetaDataFile() throws Exception {

        String metaFilePath = "src/main/resources/metadata.csv";
        File metaFile = new File(metaFilePath);

        if (!metaFile.exists()) {
            throw new Exception("`metadata.csv` in Resources folder does not exist");
        }

        PrintWriter writer = new PrintWriter(metaFile);
        writer.write("");
        writer.close();
    }

    @Test
    @Order(3)
    public void testDataDirectory() throws Exception {
        String dataDirPath = "src/main/resources/data";
        File dataDir = new File(dataDirPath);

        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new Exception("`data` Directory in Resources folder does not exist");
        }

        ArrayList<String> files = new ArrayList<>();
        try {
            files = Files.walk(Paths.get(dataDirPath)).map(f -> f.toAbsolutePath().toString())
                    .filter(p -> !Files.isDirectory(Paths.get(p))).collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String file : files) {
            Files.delete(Paths.get(file));
        }
    }

    @Test
    @Order(4)
    public void testTableCreation() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        createStudentTable(dbApp);
        dbApp = null;
    }

    @Test
    @Order(5)
    public void testRecordInsertions() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        int limit = 3000;

        System.out.println("Inserting Students");
        insertStudentRecords(dbApp, limit);
        System.out.println("[DONE] inserting Students");
        dbApp = null;
    }

    @Test
    @Order(6)
    public void checkDataArrangmentBefore() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table = dbApp._getTable("students");

        Vector<String> data = new Vector<String>();

        for (Page page : table.buckets) {
            Assertions.assertTrue(page.size() <= DBApp.maxPerPage);
            page.load();
            for (Tuple t : page.data)
                data.add((String) t.getPrimeKey());
            page.free();
        }

        // for (int i = 0; i < data.size() - 1; i++)
        //     if (data.get(i).compareTo(data.get(i + 1)) > 0) {
        //         System.out.printf("%s !< %s \n", data.get(i), data.get(i + 1));
        //         throw new Exception("data is not sorted");
        //     }
        for(int i=0; i<table.buckets.size();i++)
            System.out.printf(
                "[%2d]: %s ---> %s\n", 
                i, 
                table.buckets.get(i).min.toString(), 
                table.buckets.get(i).max.toString()
            );

        boolean flag = true;
        int totalErrors = 0;
        
        Vector<String> dataClone = (Vector<String>) data.clone();
        Collections.sort(dataClone);
        
        System.out.println(data.size());
        System.out.printf("[LOG] data size is %d\n", data.size());
        
        for(int i=0; i<data.size(); i++)
            if(!data.get(i).equals(dataClone.get(i))){
                System.out.printf("not in place %s != %s \n", data.get(i), dataClone.get(i));
                flag = false;
                totalErrors ++;
            }
        
            System.out.printf("[LOG] total data not in place %d \n", totalErrors);
        if(!flag) throw new Exception("data is not sorted ahh shit!?");
        dbApp = null;
    }

    @Test
    @Order(7)
    public void TestQueryReslut() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table = dbApp._getTable("students");

        Hashtable<String, Object> skeys = new Hashtable<>();
        skeys.put("gpa", 1.0);

        System.out.println("Getting students with gpa 1.0");

        Class tableClass = Table.class;
        Method _searchRowsMethod = tableClass.getDeclaredMethod("_searchRows", Hashtable.class);
        _searchRowsMethod.setAccessible(true);
        int numberOfRows = 0;
        Hashtable<Page, Vector<Integer>> ret = (Hashtable<Page, Vector<Integer>>) _searchRowsMethod.invoke(table,
                skeys);
        Assertions.assertNotNull(ret);
        for (Map.Entry<Page, Vector<Integer>> entries : ret.entrySet()) {
            Page p = entries.getKey();
            Vector<Integer> indexes = entries.getValue();
            p.load();
            for (int i : indexes) {
                System.out.printf("[RESUTL Tuple] %s \n", p.data.get(i));
                numberOfRows++;
            }
            p.free();
        }
        Assertions.assertEquals(numberOfRows, 8);
        System.out.println("[DONE] Getting the students");
        dbApp = null;
    }

    @Test
    @Order(8)
    public void TestUpdate() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        Table table = dbApp._getTable("students");

        Hashtable<String, Object> newName = new Hashtable<>();
        newName.put("first_name", "hamada");

        System.out.println("Updateing the name of the student with id 88-2778");
        dbApp.updateTable("students", "88-2778", newName);
        System.out.println("[DONE] Updating name");

        Method _searchRowsMethod = Table.class.getDeclaredMethod("_searchRows", Hashtable.class);
        _searchRowsMethod.setAccessible(true);

        Hashtable<Page, Vector<Integer>> ret = (Hashtable<Page, Vector<Integer>>) _searchRowsMethod.invoke(table,
                newName);
        String name = "";
        for (Map.Entry<Page, Vector<Integer>> en : ret.entrySet()) {
            Page p = en.getKey();
            int index = en.getValue().get(0);
            p.load();
            name = (String) p.data.get(index).get("first_name");
            p.free();
            break;
        }

        Assertions.assertEquals(name, "hamada");
    }

    @Test
    @Order(9)
    public void TestDeleteRows() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        Hashtable<String, Object> skeys = new Hashtable<>();
        skeys.put("gpa", 1.0);

        System.out.println("Deleteing the students with gpa 1.0");
        dbApp.deleteFromTable("students", skeys);
        System.out.println("[DONE] Deleting the students");

        dbApp = null;
    }

    @Test
    @Order(10)
    public void testQueryDeletedData() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table = dbApp._getTable("students");

        Hashtable<String, Object> skeys = new Hashtable<>();
        skeys.put("gpa", 1.0);

        System.out.println("Getting students with gpa 1.0");

        Class tableClass = Table.class;
        Method _searchRowsMethod = tableClass.getDeclaredMethod("_searchRows", Hashtable.class);
        _searchRowsMethod.setAccessible(true);
        int numberOfRows = 0;
        Hashtable<Page, Vector<Integer>> ret = (Hashtable<Page, Vector<Integer>>) _searchRowsMethod.invoke(table,
                skeys);
        if (ret != null) {
            for (Map.Entry<Page, Vector<Integer>> entries : ret.entrySet()) {
                Page p = entries.getKey();
                Vector<Integer> indexes = entries.getValue();
                p.load();
                for (int i : indexes) {
                    System.out.printf("[RESUTL Tuple] %s\n", p.data.get(i).getPrimeKey());
                    numberOfRows++;
                }
                p.free();
            }
        }
        Assertions.assertNull(ret);
        Assertions.assertEquals(numberOfRows, 0);
        System.out.println("[DONE] Getting the students with gpa 1.0");
        dbApp = null;
    }

    @Test
    @Order(11)
    public void checkDataArrangment() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table = dbApp._getTable("students");

        Vector<String> data = new Vector<String>();

        for (Page page : table.buckets) {
            Assertions.assertTrue(page.size() <= DBApp.maxPerPage);
            page.load();
            for (Tuple t : page.data)
                data.add((String) t.getPrimeKey());
            page.free();
        }

        for (int i = 0; i < data.size() - 1; i++)
            if (data.get(i).compareTo(data.get(i + 1)) > 0) {
                System.out.printf("%s !< %s \n", data.get(i), data.get(i + 1));
                throw new Exception("data is not sorted");
            }
        System.out.println(data.size());
        dbApp = null;
    }

    @Test
    @Order(12)
    public void testMinMaxPage() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table = dbApp._getTable("students");

        for (Page page : table.buckets) {
            Object min = page.min, max = page.max;
            page.load();
            System.out.printf("%s should be equal %s\n", min.toString(), page.data.firstElement().toString());
            System.out.printf("%s should be equal %s\n", max.toString(), page.data.lastElement().toString());
            Assertions.assertEquals(min, page.data.firstElement().getPrimeKey());
            Assertions.assertEquals(max, page.data.lastElement().getPrimeKey());
            page.free();

        }
        dbApp = null;
    }

    @Test
    @Order(13)
    public void testDeletePages() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        Table table = dbApp._getTable("students");
        tableSize = (int) table.size.longValue();

        Page p1, p3;
        Assertions.assertEquals(table.buckets.size(), 15);
        p1 = table.buckets.get(0);
        p3 = table.buckets.get(2);
        page1 = new Vector<Tuple>();
        page3 = new Vector<Tuple>();

        p1.load();
        p3.load();
        for (Tuple t : p1.data)
            page1.add(t);
        p1.free();
        for (Tuple t : p3.data)
            page3.add(t);
        p3.free();

        // delete the first page
        for (Tuple t : page1) {
            Hashtable<String, Object> row = new Hashtable<>();
            row.put(t.primaryKeyName, t.getPrimeKey());
            dbApp.deleteFromTable("students", row);
        }
        System.out.println("[DONE] Deleting first Page");
        // delete the thrid page
        for (Tuple t : page3) {
            Hashtable<String, Object> row = new Hashtable<>();
            row.put(t.primaryKeyName, t.getPrimeKey());
            dbApp.deleteFromTable("students", row);
        }

        System.out.println("[DONE] Deleting third Page");

        Assertions.assertEquals(table.buckets.size(), 13);

        Assertions.assertEquals(getNumberOfPages(), 13);
    }

    @Test
    @Order(14)
    public void reInsertPages() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        Table table = dbApp._getTable("students");

        for (Tuple t : page3) {
            Hashtable<String, Object> row = new Hashtable<>();

            row.put(t.primaryKeyName, t.getPrimeKey());
            row.put("first_name", t.get("first_name"));
            row.put("last_name", t.get("last_name"));
            row.put("gpa", t.get("gpa"));
            row.put("dob", t.get("dob"));

            dbApp.insertIntoTable("students", row);
        }

        System.out.println("[DONE] Inserting thrid Page");

        Assertions.assertEquals(table.buckets.size(), 14);
        Assertions.assertEquals(getNumberOfPages(), 14);

        for (Tuple t : page1) {
            Hashtable<String, Object> row = new Hashtable<>();

            row.put(t.primaryKeyName, t.getPrimeKey());
            row.put("first_name", t.get("first_name"));
            row.put("last_name", t.get("last_name"));
            row.put("gpa", t.get("gpa"));
            row.put("dob", t.get("dob"));

            dbApp.insertIntoTable("students", row);
        }
        System.out.println("[DONE] Inserting first Page");

        Assertions.assertEquals(table.buckets.size(), 15);
        Assertions.assertEquals(getNumberOfPages(), 15);
        Assertions.assertEquals((int) table.size.longValue(), tableSize);
    }

    @Test
    @Order(15)
    public void checkDataArrangment2() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        Table table = dbApp._getTable("students");

        Vector<String> data = new Vector<String>();

        for (Page page : table.buckets) {
            Assertions.assertTrue(page.size() <= DBApp.maxPerPage);
            page.load();
            for (Tuple t : page.data)
                data.add((String) t.getPrimeKey());
            page.free();
        }

        for (int i = 0; i < data.size() - 1; i++)
            if (data.get(i).compareTo(data.get(i + 1)) > 0) {
                System.out.printf("%s !< %s \n", data.get(i), data.get(i + 1));
                throw new Exception("data is not sorted");
            }
        System.out.println(data.size());
        dbApp = null;
    }

    private int getNumberOfPages() throws Exception {
        int number = 0;
        Stream<Path> files = Files.walk(Paths.get(Resources.getResourcePath(), "data", "students"))
                .filter(Files::isRegularFile);
        Iterator itr = files.iterator();
        while (itr.hasNext()) {
            number++;
            itr.next();
        }
        return number;
    }

    private void insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }

    private void insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(
                new FileReader("src/main/resources/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }

    private void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        // ! it was not checking for the c
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }

    private void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }

    private void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1990-01-01");
        minValues.put("course_id", "000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2000-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }

    private void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
}
