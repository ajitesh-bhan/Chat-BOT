#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sqlite3
import pandas as pd
timeframes= ['2015-01']
# multiple db timeframes can be put


def file_creation(filename, data_frame):
    
    with open(filename,'a', encoding='utf8') as f:
            data_frame.to_csv(f,header=False)
            
    return 

for t in timeframes:
    connection = sqlite3.connect('{}.db'.format(t))
    c=connection.cursor()
    limit=5000
    last_unix=0
    stop_time=limit
    counter =0
    test_done=False
    while stop_time==limit:
        df = pd.read_sql("SELECT * FROM parent_reply WHERE unix > {} and parent NOT NULL and score > 0 ORDER BY unix ASC LIMIT {}".format(last_unix,limit),connection)
        last_unix = df.tail(1)['unix'].values[0]
        # updating last_unix
        #when to stop
        stop_time = len(df)
        data= pd.DataFrame(df, columns = ['parent', 'comment'])
        if test_done== False: 
            file_creation('Testing.json', data)
            test_done= True
        else: 
            file_creation('Training.json', data)
            # testing /traning first column will have parent commnts and second will be there responses 
        
        counter +=1
        if counter % 20 == 0: 
            print(counter*limit,'rows completed so far')
            #updates after 1000 rows 
    
        

