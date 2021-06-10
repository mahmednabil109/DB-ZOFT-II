import org.junit.jupiter.api.*;

import java.io.*;
import java.util.*;
import java.lang.reflect.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"all", "unchecked"})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MetaDataFileTests {

    public static Vector<Tuple> page1 = new Vector<Tuple>(), page3 = new Vector<Tuple>();
    public static int tableSize = 0;

    @Test
    @Order(0)
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
    @Order(1)
    public void testTableCreation() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        createStudentTable(dbApp);
        createCoursesTable(dbApp);
        createPCsTable(dbApp);
        createTranscriptsTable(dbApp);
        dbApp = null;
    }

    @Test
    @Order(2)
    public void testMDBeforeIndexes() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        // path to `metadata.csv`
        Path p = Paths.get(Resources.getResourcePath(), "metadata.csv");
        Assertions.assertTrue(Files.exists(p));

        List<String> lines = Files.readAllLines(p);

        // check the total number of the lines
        Assertions.assertEquals(lines.size(), 15);

        // number of indexed columns 
        Hashtable<String, Integer> noi = new Hashtable<>();
        // number of primary columns
        Hashtable<String, Integer> nop = new Hashtable<>();

        for(String line  : lines){
            String data [] = line.split(",");
            int numi = (noi.containsKey(data[0])) ? noi.get(data[0]) : 0;
            int nump = (nop.containsKey(data[0])) ? nop.get(data[0]) : 0;
            if(data[4].toLowerCase().equals("true"))
                noi.put(data[0], numi + 1);
            if(data[3].toLowerCase().equals("true"))
                nop.put(data[0], nump + 1);
        }

        for(int num : noi.values())
            Assertions.assertEquals(num, 0);
        
        for(int num : nop.values())
            Assertions.assertEquals(num, 1);
    }

    @Test
    @Order(3)
    public void testCreateIndexes() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

         // Index 1
         System.out.println("[LOG] Creating the first Index");
         Assertions.assertDoesNotThrow(
             () -> dbApp.createIndex(
                 "students", 
                 new String[] { "id", "first_name", "gpa" }
             )
         );
         System.out.println("[LOG] Done Creating the first Index");
 
         // Index 2
         System.out.println("[LOG] Creating the second Index");
         Assertions.assertDoesNotThrow(
             () -> dbApp.createIndex(
                 "courses", 
                 new String[] { "date_added" }
             )
         );
         System.out.println("[LOG] Done Creating the second Index");
 
         // Index 3
         System.out.println("[LOG] Creating the third Index");
         Assertions.assertDoesNotThrow(() -> 
             dbApp.createIndex(
                 "pcs", 
                 new String[]{ "pc_id", "student_id"}
             )
         );
         System.out.println("[LOG] Done Creating the third Index");
 
         // Index 4
         System.out.println("[LOG] Creating the fourth Index");
         Assertions.assertDoesNotThrow(() ->
             dbApp.createIndex(
                 "transcripts", 
                 new String[]{ "student_id", "gpa", "date_passed", "course_name"}    
             )
         );
         System.out.println("[LOG] Done Creating the fourth Index");

    }

    @Test
    @Order(4)
    public void testMDAfterIndexes() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, URISyntaxException, IOException {
        DBApp dbApp = new DBApp();
        dbApp.init();

        // path to `metadata.csv`
        Path p = Paths.get(Resources.getResourcePath(), "metadata.csv");
        Assertions.assertTrue(Files.exists(p));

        List<String> lines = Files.readAllLines(p);

        // check the total number of the lines
        Assertions.assertEquals(lines.size(), 15);

        // number of indexed columns 
        Hashtable<String, Integer> noi = new Hashtable<>();
        // number of primary columns
        Hashtable<String, Integer> nop = new Hashtable<>();

        for(String line  : lines){
            String data [] = line.split(",");
            int numi = (noi.containsKey(data[0])) ? noi.get(data[0]) : 0;
            int nump = (nop.containsKey(data[0])) ? nop.get(data[0]) : 0;
            if(data[4].toLowerCase().equals("true"))
                noi.put(data[0], numi + 1);
            if(data[3].toLowerCase().equals("true"))
                nop.put(data[0], nump + 1);
        }

        Assertions.assertEquals(noi.get("students"), 3);
        Assertions.assertEquals(noi.get("courses"), 1);
        Assertions.assertEquals(noi.get("pcs"), 2);
        Assertions.assertEquals(noi.get("transcripts"), 4);

        
        for(int num : nop.values())
            Assertions.assertEquals(num, 1);

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
