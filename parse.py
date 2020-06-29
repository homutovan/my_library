import psycopg2 as pg
from random import randint as rd
import pandas as pd

params = {
    'dbname': 'lib',
    'user': 'postgres',
    'password': ''
        }

class DBexecutor:
    
    def __init__(self, params):
        self.params = params
        self.conn, self.curs = self.connect_db()
        
    def connect_db(self):
        try:
            conn = pg.connect(**self.params)
            conn.set_session(autocommit=True)
            if conn:
                curs = conn.cursor()
                return conn, curs
        except Exception:
            print('Ошибка подключения к БД')
            return None, None
        
    def __del__(self):
        if self.conn:
            self.curs.close()
            self.conn.close()
        
db = DBexecutor(params)

db_schema = [
            """CREATE TABLE first_name (id serial PRIMARY KEY, first_name varchar(100));""",
            """CREATE TABLE middle_name (id serial PRIMARY KEY, middle_name varchar(100));""",
            """CREATE TABLE last_name (id serial PRIMARY KEY, last_name varchar(100));""",
            """CREATE TABLE faculty (id serial PRIMARY KEY, faculty varchar(100));""",
            """CREATE TABLE book_name (id serial PRIMARY KEY, book_name varchar(100));""",
            """CREATE TABLE publishing (id serial PRIMARY KEY, publishing varchar(100));""",
            """CREATE TABLE student (id serial PRIMARY KEY, first_name_id 
                INTEGER REFERENCES first_name(id), middle_name_id 
                INTEGER REFERENCES  middle_name(id), last_name_id 
                INTEGER REFERENCES  last_name(id), faculty_id 
                INTEGER REFERENCES  faculty(id));""",
            """CREATE TABLE author (id serial PRIMARY KEY, first_name_id 
                INTEGER REFERENCES first_name(id), middle_name_id 
                INTEGER REFERENCES  middle_name(id), last_name_id 
                INTEGER REFERENCES  last_name(id));""",
            """CREATE TABLE books (id serial PRIMARY KEY, name_id 
            INTEGER REFERENCES book_name(id), author_id 
            INTEGER REFERENCES  author(id), publishing_id 
            INTEGER REFERENCES  publishing(id), year date);""",
            """CREATE TABLE books_students (id serial PRIMARY KEY, book_id 
            INTEGER REFERENCES books(id), student_id 
            INTEGER REFERENCES  student(id), date_of_issue date, date_of_return date);"""
            ]

for query in db_schema:
    db.curs.execute(query)

with open('authors', 'r') as f:     
    author_list = [author.split() for author in f]

author_list = [author for author in author_list if len(author) == 3]
author_list = list(map(lambda x: [x[0][:-1], x[1], x[2]], author_list))

last_name_list = []
first_name_list = []
middle_name_list = []

for last_name, first_name, middle_name in author_list:
    last_name_list.append(last_name)
    first_name_list.append(first_name)
    middle_name_list.append(middle_name)
    
last_name_list = list(set(last_name_list))
first_name_list = list(set(first_name_list))
middle_name_list = list(set(middle_name_list))

with open('books', 'r') as f:
    books_list = [book.strip() for book in f]
    
with open('publishing', 'r') as f:
    pub_list = [pub.strip() for pub in f]
    
with open('faculty', 'r') as f:
    faculty_list = [faculty.strip() for faculty in f]

def fill_table(iter, curs, query):
    for value in iter:
        curs.execute(query, (value, ))
        
fill_table(last_name_list, db.curs, "INSERT INTO last_name (last_name) VALUES (%s)")
fill_table(first_name_list, db.curs, "INSERT INTO first_name (first_name) VALUES (%s)")
fill_table(middle_name_list, db.curs, "INSERT INTO middle_name (middle_name) VALUES (%s)")
fill_table(books_list, db.curs, "INSERT INTO book_name (book_name) VALUES (%s)")
fill_table(pub_list, db.curs, "INSERT INTO publishing (publishing) VALUES (%s)")
fill_table(faculty_list, db.curs, "INSERT INTO faculty (faculty) VALUES (%s)")

for last_name, first_name, middle_name in author_list:
    db.curs.execute("""SELECT id FROM last_name WHERE last_name = %s""", (last_name, ))
    ln_id = db.curs.fetchall()[0][0]
    db.curs.execute("""SELECT id FROM first_name WHERE first_name = %s""", (first_name, ))
    fn_id = db.curs.fetchall()[0][0]
    db.curs.execute("""SELECT id FROM middle_name WHERE middle_name = %s""", (middle_name, ))
    mn_id = db.curs.fetchall()[0][0]
    db.curs.execute("""INSERT INTO author (first_name_id, middle_name_id, last_name_id) 
                        VALUES (%s, %s, %s)""", (fn_id, mn_id, ln_id))

for i in range(100):
    db.curs.execute("""INSERT INTO student (first_name_id, middle_name_id, last_name_id, faculty_id) 
                        VALUES (%s, %s, %s, %s)""", (rd(1, 216), rd(1, 246), rd(1, 1082), rd(1, 11)))

for i in range(1000):
    db.curs.execute("""INSERT INTO books (name_id, author_id, publishing_id, year) 
                        VALUES (%s, %s, %s, %s)""", (rd(1, 200), rd(1, 1172), rd(1, 50), f'{rd(1970, 2020)}-01-01'))

for i in range(10000):
    year1 = rd(2015, 2020)
    year = year1 + rd(0, 2)
    year2 = year if year <= 2020 else 2020
    
    month1 = rd(1, 12)
    month = month1 + rd(0, 2)
    month2 = month if month <= 12 else 12
    
    day1 = rd(1, 28)
    day = day1 + rd(0, 7)
    day2 = day if day <= 28 else 28
    
    db.curs.execute("""INSERT INTO books_students (book_id, student_id, date_of_issue, date_of_return) 
                    VALUES (%s, %s, %s, %s)""", 
                    (rd(1, 1000), rd(2, 100), f'{year1}-{month1}-{day1}', f'{year2}-{month2}-{day2}'))
