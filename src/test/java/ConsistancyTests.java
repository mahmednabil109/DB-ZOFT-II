
import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
//import main.java.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ConsistancyTests {


    @Test
    @Order(1)
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
    @Order(2)
    public void testDataDirectory() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        String dataDirPath = "src/main/resources/data";
        File dataDir = new File(dataDirPath);

        if (!dataDir.isDirectory() || !dataDir.exists()) {
            throw new Exception("`data` Directory in Resources folder does not exist");
        }

        ArrayList<String> files = new ArrayList<>();
        try {
            files = Files.walk(Paths.get(dataDirPath))
                    .map(f -> f.toAbsolutePath().toString())
                    .filter(p -> !Files.isDirectory(Paths.get(p)))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String file : files) {
            Files.delete(Paths.get(file));
        }
    }

    @Test
    @Order(3)
    public void testTableCreationIfNot() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();

        System.out.println("[LOG] Creating Table Students");
        createStudentTable(dbApp);
        System.out.println("[LOG] Done Creating Table Students");
        System.out.println("[LOG] Creating Table Students2");
        createStudentTable2(dbApp);
        System.out.println("[LOG] Done Creating Table Students2");
        // System.out.println("[LOG] Creating Table Courses");
        // createCoursesTable(dbApp);
        // System.out.println("[LOG] Done Creating Table Courses");
        System.out.println("[LOG] Done Creating Table Transcripts");
        createTranscriptsTable(dbApp);
        System.out.println("[LOG] Done Creating Table Transcripts");
        System.out.println("[LOG] Done Creating Table Transcripts2");
        createTranscriptsTable2(dbApp);
        System.out.println("[LOG] Done Creating Table Transcripts2");
        // System.out.println("[LOG] Done Creating Table PCs");
        // createPCsTable(dbApp);
        // System.out.println("[LOG] Done Creating Table PCs");


        dbApp = null;
    }

    @Test
    @Order(5)
    public void testRecordInsertions() throws Exception {
        DBApp dbApp = new DBApp();
        dbApp.init();
        int limit = 500;

        System.out.println("[LOG] Inserting into Table Students");
        insertStudentRecords(dbApp, limit);
        System.out.println("[LOG] Done Inserting into Table Students");
        System.out.println("[LOG] Inserting into Table Students2");
        insertStudentRecords2(dbApp, limit);
        System.out.println("[LOG] Done Inserting into Table Students2");
        // insertCoursesRecords(dbApp, limit);
        System.out.println("[LOG] Inserting into Table Transcript");
        insertTranscriptsRecords(dbApp, limit);
        System.out.println("[LOG] Done Inserting into Table Transcript");
        System.out.println("[LOG] Inserting into Table Transcript2");
        insertTranscriptsRecords2(dbApp, limit);
        System.out.println("[LOG] Done Inserting into Table Transcript2");
        // insertPCsRecords(dbApp, limit);
        dbApp = null;
    }

    @Test
    @Order(4)
    public void createIndexes() throws Exception {
        final DBApp dbApp = new DBApp();
        dbApp.init();



        // Index 1
        System.out.println("[LOG] Creating the first Index");
        Assertions.assertDoesNotThrow(
            () -> dbApp.createIndex(
                "students", 
                new String[] { "id", "gpa" }
            )
        );
        System.out.println("[LOG] Done Creating the first Index");

        // Index 2
        System.out.println("[LOG] Creating the second Index");
        Assertions.assertDoesNotThrow(
            () -> dbApp.createIndex(
                "students", 
                new String[] { "first_name", "gpa", "dob" }
            )
        );
        System.out.println("[LOG] Done Creating the second Index");

        // Index 3
        System.out.println("[LOG] Creating the third Index");
        Assertions.assertDoesNotThrow(() -> 
            dbApp.createIndex(
                "transcripts", 
                new String[]{ "gpa", "student_id"}
            )
        );
        System.out.println("[LOG] Done Creating the third Index");

        // Index 4
        System.out.println("[LOG] Creating the fourth Index");
        Assertions.assertDoesNotThrow(() ->
            dbApp.createIndex(
                "transcripts", 
                new String[]{ "student_id", "course_name"}    
            )
        );
        System.out.println("[LOG] Done Creating the fourth Index");

    }

    @Test
    @Order(6)
    public void testAfterBforeStudent() throws Exception{
        final DBApp dbApp = new DBApp();
        dbApp.init();

        SQLTerm t1 = new SQLTerm(
            "students",
            "id",
            ">",
            "44-1010"
        );

        SQLTerm t2 = new SQLTerm(
            "students",
            "gpa",
            "<=",
            new Double(2)
        );

        SQLTerm t12 = new SQLTerm(
            "students2",
            "id",
            ">",
            "44-1010"
        );

        SQLTerm t22 = new SQLTerm(
            "students2",
            "gpa",
            "<=",
            new Double(2)
        );

        Iterator itr1 = dbApp.selectFromTable(
            new SQLTerm [] {
                t1, t2
            }, 
            new String[]{
                "and"
            }
        );

        System.out.println("============================");

        Iterator itr2 = dbApp.selectFromTable(
            new SQLTerm [] {
                t12, t22
            }, 
            new String[]{
                "and"
            }
        );

        Assertions.assertEquals(itr1.hasNext(), itr2.hasNext());

        Vector<Tuple> set1 = new Vector<>();
        Vector<Tuple> set2 = new Vector<>();

        while(itr1.hasNext()) set1.add((Tuple) itr1.next());
        while(itr2.hasNext()) set2.add((Tuple) itr2.next());

        System.out.printf("[LOG] %d should equal %d\n", set1.size(), set2.size());


        Collections.sort(set1);
        Collections.sort(set2);

        System.out.println("===================");
        for(Tuple t : set1)
            System.out.println(t);

        System.out.println("===================");
        for(Tuple t : set2)
            System.out.println(t);
 
        System.out.println("===================");
        for(Tuple t : set2)
            if(!set1.contains(t))
                System.out.println("D: " + t.toString());
    
        Set<Tuple> s1 = set1.stream().collect(Collectors.toSet());
        Set<Tuple> s2 = set2.stream().collect(Collectors.toSet());
        
        s1.retainAll(s2);

        System.out.printf("[LOG] size in common ==> %d\n", s1.size());
        Assertions.assertEquals(set1.size(), set2.size());

        Collections.sort(set1);
        Collections.sort(set2);

        Assertions.assertDoesNotThrow(() -> {
            for(int i=0; i<set1.size();i++)
                if(!(set1.get(i).compareTo(set2.get(i)) == 0))
                    throw new Exception("non equal");
        });

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

    private void insertStudentRecords2(DBApp dbApp, int limit) throws Exception {
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

            dbApp.insertIntoTable("students2", row);
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

    private void insertTranscriptsRecords2(DBApp dbApp, int limit) throws Exception {
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

            dbApp.insertIntoTable("transcripts2", row);
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
        // ! why not to check on the limit
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

    private void createStudentTable2(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students2";

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
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
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

    private void createTranscriptsTable2(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts2";

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
