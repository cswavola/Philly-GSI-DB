import json
import sqlite3
import re

conn = sqlite3.connect('gsi.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Projects;
DROP TABLE IF EXISTS Retrofits;
DROP TABLE IF EXISTS Joint;
DROP TABLE IF EXISTS Grants;

CREATE TABLE Projects (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    pname TEXT UNIQUE,
    grant_id INTEGER,
    grant_amount INTEGER,
    approve_yr INTEGER
);

CREATE TABLE Retrofits (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    rname TEXT UNIQUE
);

CREATE TABLE Grants (
    id INTEGER NOT NULL PRIMARY KEY UNIQUE,
    gname TEXT UNIQUE
);

CREATE TABLE Joint (
    number INTEGER,
    project_id INTEGER,
    retro_id INTEGER,
    UNIQUE (project_id, retro_id)
)''')

grants = [(0,'SMIP'),(1,'GARP')]
cur.executemany('INSERT INTO Grants (id, gname) VALUES (?, ?)', grants)

fname = input('Enter file name: ')
if len(fname) < 1 : fname = 'GSI_Private_Projects_Retrofit.geojson'

with open(fname) as f :
    dict_data = json.loads(f.read())


for f in dict_data['features'] :
     project = f['properties']

     #if project['NAME'] != project['PROJECTNAME'] : print(project['NAME'], project['PROJECTNAME'])
     if project['APPROVALDATE'] is None : continue

     #handle missing name fields
     if project['NAME'] is not None : pname = project['NAME']
     elif project['PROJECTNAME'] is not None : pname = project['PROJECTNAME']
     else :
         print('Error in Record: ', project['TRACKINGNUMBER'])
         continue
     grant_amount = project['GRANTAMOUNT']
     #parse approval year
     approve_date = project['APPROVALDATE']
     approve_yr = int(approve_date[:4])
     #get retrofit names from json keys
     kl = list(project.keys())
     rname = kl[11:]
     # get grant values
     if project['SMIP'] == -1 : grant_id = 0
     elif project['GARP'] == -1 : grant_id = 1
     else : grant_id = None

     for i in range(len(rname)) :
          rname[i] = (rname[i],) # make rname into a list of tuples for executemany argument

     cur.execute('''INSERT OR IGNORE INTO Projects (pname, grant_id, grant_amount, approve_yr)
     VALUES (?, ?, ?, ?)''', (pname, grant_id, grant_amount, approve_yr))
     cur.execute('SELECT id FROM Projects WHERE pname = ?', (pname,))
     project_id = cur.fetchone()[0]

     stmt = 'INSERT OR IGNORE INTO Retrofits (rname) VALUES (?)'
     cur.executemany(stmt, rname)

     rstr = kl[11:] #get retrofit names as list of strings to loop through
     for i in range(len(rstr)) :
         if project[rstr[i]] > 0 :
             number = project[rstr[i]]
             cur.execute('SELECT id FROM Retrofits WHERE rname = ?', rname[i])
             retro_id = cur.fetchone()[0]

             #create many-to-many join table
             cur.execute('''INSERT OR REPLACE INTO Joint (number, project_id, retro_id)
             VALUES (?, ?, ?)''', (number, project_id, retro_id))
         else : continue

conn.commit()

#Sanity check joining tables

# sqlstmt = '''SELECT Projects.pname, Grants.gname, Projects.grant_amount,
# Retrofits.rname, Joint.number FROM Projects JOIN Grants JOIN Retrofits JOIN Joint
# ON Joint.project_id = Projects.id AND Joint.retro_id = Retrofits.id AND Projects.grant_id = Grants.id
# ORDER BY Grants.gname, Projects.pname, Joint.number DESC'''
#
# for row in cur.execute(sqlstmt) :
#     print((row[0], row[1], row[2], row[3], row[4]))

#How much money was given out by each grant program?
sqlstmt = '''SELECT Grants.gname, SUM(Projects.grant_amount), COUNT(Projects.pname)
FROM Grants JOIN Projects ON Grants.id = Projects.grant_id
GROUP BY Grants.gname'''

for row in cur.execute(sqlstmt) :
    print('For %s grant: %d dollars allocated to %d projects' % (row[0], row[1], row[2]))

#How many of each type of retrofit was done per grant?
#I also want this to display the totals for each retrofit for a "Null" grant group...
sqlstmt = '''SELECT coalesce(Grants.gname, 'No grant'), Retrofits.rname, SUM(Joint.number)
FROM Projects LEFT JOIN Grants LEFT JOIN Retrofits LEFT JOIN Joint
ON Joint.project_id = Projects.id AND Joint.retro_id = Retrofits.id
AND Projects.grant_id = Grants.id
GROUP BY 2, 1'''

# for row in cur.execute(sqlstmt) :
#     print(row[0], row[1], row[2])

#Total of each retrofit implemented in private projects
sqlstmt = '''SELECT Retrofits.rname, SUM(Joint.number)
FROM Retrofits LEFT JOIN Joint ON Retrofits.id = Joint.retro_id
GROUP BY 1 ORDER BY 2 DESC'''

# for row in cur.execute(sqlstmt) :
#     print(row[0], row[1])

cur.execute(sqlstmt)
maxr = cur.fetchone()
print('The most popular retrofit was the %s with %d implementation(s).' % (maxr))

sqlstmt = '''SELECT Retrofits.rname, SUM(Joint.number)
FROM Retrofits LEFT JOIN Joint ON Retrofits.id = Joint.retro_id
GROUP BY 1 ORDER BY 2'''

cur.execute(sqlstmt)
minr = cur.fetchone()
print('The least popular retrofit was the %s with %d implementation(s).' % (minr))

#How many total retrofits have been done?
cur.execute('''SELECT SUM(Joint.number) FROM Joint''')
total = cur.fetchone()[0]
print('%d retrofits have been verified by the program.' % (total))

#print approvals by year
sqlstmt = '''SELECT approve_yr, COUNT(pname)
FROM Projects GROUP BY 1'''

for row in cur.execute(sqlstmt) :
    print('In %d, %d projects approved.' % (row[0], row[1]))

cur.close()
