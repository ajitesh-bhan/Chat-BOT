#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3
import json
from datetime import datetime

timeframe = '2015-01'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()



def create_table():
    
    #if tables not created this will create table
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")
    
def format_data(data):
    data = data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return data


# basically we are pussihng data in set of 1000 
def transaction_bldr(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sql_transaction = []
        
def sql_insert_replace_comment(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))
                                                                      

def sql_insert_has_parent(commentid,parentid,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentid, commentid, parent, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))
                                                                      

def sql_insert_no_parent(commentid,parentid,comment,subreddit,time,score):
    try:
        # not inserting parent 
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentid, commentid, comment, subreddit, int(time), score)
        transaction_bldr(sql)
    except Exception as e:
        print('s0 insertion',str(e))   
    
def acceptable(data):
    # conditions for a comment to be acceptable 
    if len(data.split(' '))>50 or len(data) <1 or len(data)>1000 or data == '[deleted]' or data == '[removed]': 
        return False
    else: return True

    

def find_parent(pid):
    #comment has the parent id in the string we are looking for that 
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id ='{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        #fetch one returns tuuple
        if result != None: return result[0]
        else: return False   
    except Exception as e: return False
    
def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else: return False
    except Exception as e:
        #print(str(e))
        return False



                                                                   
    
                                                                      
if __name__== "__main__":
    create_table()
    count_row=0
    pair_row=0
    # pair_row parent and child comments as there will be comments with no reply
    
    with open(r"C:\Users\ajite\Desktop\NLP chat bot\RC_2015-01\RC_{}".format(timeframe)) as file:
        for row in file:
            #print(row)
            count_row+=1 
            row= json.loads(row)
            parent_id= row['parent_id']     #  comment in response to what
            body= format_data(row['body'])  # body is comment data
            comment_id = row['name']         # ID of this comment and if there is response to this same will be added as parent id in some other row for that comment                                                 
            created_utc = row['created_utc']
            score = row['score']             # basically no. of upvotes
            subreddit = row['subreddit']
            parent_data= find_parent(parent_id)
            # I am trying to get parent comment  for every child comment so in comment the string has the comment id that would be equal to parent_id 
            if score >= 2:
                existing_comment_score = find_existing_score(parent_id)
                if existing_comment_score:
                    
                    if score > existing_comment_score:
                        
                        #print(acceptable(body))
                        if acceptable(body):
                            
                            sql_insert_replace_comment(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                            
                else:
                    if acceptable(body):
                        
                        if parent_data:
                            
                            sql_insert_has_parent(comment_id,parent_id,parent_data,body,subreddit,created_utc,score)
                            pair_row += 1 
                        else:
                            sql_insert_no_parent(comment_id,parent_id,body,subreddit,created_utc,score)
            
          
                        
                            
                          
                    #only doing it for comment that in database will be start of conversation 
            if count_row % 100000 == 0:
                #getting details after every 1000000 rows                                                      
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(count_row, pair_row, str(datetime.now())))


# In[ ]:




