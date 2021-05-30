
import sqlite3, os, time
# genrate testcase for the students table

# removing old db files
# os.system("rm *.db")
fileExists = (os.path.exists("./students.db") and os.path.isfile("./students.db"))
# if the db exists then do not create any
conn = sqlite3.connect("./students.db")

if not fileExists:
    print("[LOG] Done Creating DB")

    # creating the table in the DB
    conn.execute("""
        CREATE TABLE student(
            id VARCHAR(10),
            first_name VARHAR(10),
            last_name VARCHAR(10),
            dob DATETIME,
            gpa FLOAT
        )
    """)
    conn.commit()
    print("[LOG] Done Creating TABLE student")

    with open("../../main/resources/students_table.csv") as csvFile:
        lines = csvFile.readlines()
        for line in lines:
            line = line.split(',')
            conn.execute(f"""
                INSERT INTO student(id, first_name, last_name, dob, gpa)
                VALUES('{line[0]}','{line[1]}','{line[2]}','{line[3]}',{line[4]})
            """)
        conn.commit()
        print("[LOG] Done Inserting the Data into the Table")

cursor = conn.execute("""
    SELECT * FROM student WHERE gpa = 1.0 order by id
""")

with open("./result.txt", "w") as result:
    for row in cursor:
        print(row)
        result.write(row.__str__())
        result.write("\n")
