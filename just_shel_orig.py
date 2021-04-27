import os
import numpy as np
import pandas as pd
from importt import make_query
import os.path
import sys

sqlpath = "/home/yerzh/comp1/queries/"
stats = "/home/yerzh/comp1/new/Orig/stats/"
plans = "/home/yerzh/comp1/new/Orig/plans/"

print("Hello!")
onlyfiles = [f for f in os.listdir(sqlpath) if os.path.isfile(os.path.join(sqlpath, f))]
onlyfiles.sort()

if len(sys.argv) > 1:
    onlyfiles = sys.argv[1:]
    
for filename in onlyfiles:

    f = open(sqlpath + filename, "r")
    print("Use file", sqlpath + filename)
    query = f.read()
    query = "EXPLAIN (ANALYZE ON, VERBOSE ON, FORMAT JSON) " + query
    f.close()
    list1 = []
    list2 = []
    print(filename+" original aqo")
    if os.path.isfile(stats+filename.split('.')[0]+".csv"):
        continue
    
    make_query(list1, list2, query)
    df = pd.DataFrame(list1, columns =["#iteration", "query_hash", "execution_time_with_aqo", "execution_time_without_aqo", "planning_time_with_aqo", "planning_time_without_aqo", "cardinality_error_with_aqo", "cardinality_error_without_aqo", "executions_with_aqo", "executions_without_aqo"])
    dff = pd.DataFrame(list2, columns =["#iteration", "plans"])
    df.to_csv(stats+filename.split('.')[0]+".csv")
    dff.to_csv(plans+filename.split('.')[0]+".csv")   
