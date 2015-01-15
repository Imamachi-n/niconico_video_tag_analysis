import json
import sqlite3
import os
import glob

path = 'D:/DATA_ANALYSIS/niconico_video_metadata/video/'
con = sqlite3.connect('niconico_video_ver2.sqlite')
c = con.cursor()

#c.execute('''DROP TABLE niconico_tag_0''')
count = 0

for ff in glob.glob('D:/DATA_ANALYSIS/niconico_video_metadata/video/*.dat'):
    sql_create = "CREATE TABLE niconico_tag_" + str(count) +  " (video_id,title,view_counter,mylist_counter,length,comment_counter,upload_time,tags)"
    c.execute(sql_create)
    print(ff)
    for data in open(ff,'r'):
        #use variables for multiple file
        #data = ff.read()
        #data as strings
        for s in data.splitlines():
            j = json.loads(s)
            q = ""
            for p in j["tags"]:
                for r in p.values():
                    if isinstance(r,unicode):
                        q = q + r + "||"
        print (j["video_id"])
        sql_insert = "INSERT INTO niconico_tag_" + str(count) + "(video_id,title,view_counter,mylist_counter,length,comment_counter,upload_time,tags) VALUES (?,?,?,?,?,?,?,?)"
        c.execute(sql_insert,[j["video_id"],j["title"],j["view_counter"],j["mylist_counter"],j["length"],j["comment_counter"],j["upload_time"],q])
    count += 1
        
con.commit()
con.close()
