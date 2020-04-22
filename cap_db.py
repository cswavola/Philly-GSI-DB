import json
import sqlite3
import re

conn = sqlite3.connect('gsi.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Projects;
DROP TABLE IF EXISTS Retrofits;
DROP TABLE IF EXISTS Joint;

CREATE TABLE Projects (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    pname TEXT UNIQUE,
    grant INTEGER,
    grant_amount INTEGER,
    approve_yr INTEGER
);

CREATE TABLE Retrofits (
    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    rname TEXT UNIQUE
);

CREATE TABLE Joint (
    number INTEGER,
    project_id INTEGER,
    retro_id INTEGER,
    UNIQUE (project_id, retro_id)
)''')

fname = input('Enter file name: ')
if len(fname) < 1 : fname = 'GSI_Private_Projects_Retrofit.geojson'

with open(fname) as f :
    dict_data = json.loads(f.read())


for f in dict_data['features'] :
     project = f['properties']

     #if project['NAME'] != project['PROJECTNAME'] : print(project['NAME'], project['PROJECTNAME'])
     if project['APPROVALDATE'] is None : continue

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
     if project['SMIP'] == -1 : grant = 0
     elif project['GARP'] == -1 : grant = 1
     else : grant = None

     for i in range(len(rname)) :
          rname[i] = (rname[i],) # make rname into a list of tuples for executemany argument

     cur.execute('''INSERT OR IGNORE INTO Projects (pname, grant, grant_amount, approve_yr)
     VALUES (?, ?, ?, ?)''', (pname, grant, grant_amount, approve_yr))
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



cur.close()
