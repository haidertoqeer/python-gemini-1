import sqlite3

## Connect to sqlite
connection=sqlite3.connect("student.db")

## Create a cursor object to insert record, create table, retrieve
cursor=connection.cursor()

## create the table
table_info="""
    Create table STUDENT (NAME VARCHAR(25), CLASS VARCHAR(25), SECTION VARCHAR(25), MARKS INT );
"""

cursor.execute(table_info)


insert_records = """
    INSERT INTO STUDENT (NAME, CLASS, SECTION, MARKS) VALUES
    ('John Doe', '10', 'A', 85),
    ('Jane Smith', '10', 'B', 90),
    ('Alice Johnson', '11', 'A', 78),
    ('Bob Brown', '11', 'B', 88),
    ('Charlie Davis', '12', 'A', 92),
    ('Diana Miller', '12', 'B', 84),
    ('Ethan Wilson', '10', 'A', 75),
    ('Fiona Garcia', '10', 'B', 89),
    ('George Martinez', '11', 'A', 91),
    ('Hannah Lee', '11', 'B', 83),
    ('Ian Clark', '12', 'A', 87),
    ('Jasmine Lewis', '12', 'B', 80),
    ('Kevin Young', '10', 'A', 95),
    ('Laura Hall', '10', 'B', 77),
    ('Mike Allen', '11', 'A', 82),
    ('Nina Scott', '11', 'B', 93),
    ('Oscar Adams', '12', 'A', 85),
    ('Paula Baker', '12', 'B', 88),
    ('Quincy Wright', '10', 'A', 79),
    ('Rachel Harris', '10', 'B', 86);
"""

## insert some record


cursor.executescript(insert_records)


## Display all the records

print("The inserted records are")

data=cursor.execute(''' Select * From STUDENT''')

for row in data:
    print(row)

    ##

connection.commit()
connection.close()
