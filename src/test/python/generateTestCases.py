
import sqlite3, os, time
# genrate testcase for the students table

# removing old db files
os.system("rm *.db")

conn = sqlite3.connect("./students.db")
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

with open("../../main/resources/students_table.csv") as csvFile:
    lines = csvFile.readlines()
    for line in lines:
        line = line.split(',')
        print(line[0])
        conn.execute(f"""
            INSERT INTO student(id, first_name, last_name, dob, gpa)
            VALUES({line[0]},'{line[1]}','{line[2]}','{line[3]}',{line[4]})
        """)
    conn.commit()
cursor = conn.execute("""
    SELECT COUNT(id), COUNT(DISTINCT id) FROM studen;
""")
for row in cursor:
    print(row)