import psycopg2
import numpy as np
import os

def make_query(ls, lss, query):
    conn = None

    try:

        conn = psycopg2.connect(dbname="postgres", user="postgres", password="55695387", host="127.0.0.1")

        cur = conn.cursor()

        cur.execute("create extension aqo;")

        cur.execute("set aqo.mode = 'learn';")

        cur.execute(query)

        res = cur.fetchone()[0][0]

        lss.append([1, res])
        cur.execute("set aqo.mode = 'disabled';")
        conn.commit()
        cur.execute("select * from aqo_query_stat where query_hash = (select query_hash from aqo_query_texts where query_text=%s);", (query,))
        
        res_stats = cur.fetchone()
        ls.append([1, res_stats[0],res_stats[1],res_stats[2],res_stats[3],res_stats[4],res_stats[5],res_stats[6],res_stats[7],res_stats[8]])

        for i in range(2):
            print(i+1, " - iteration")
            cur.execute("set aqo.mode = 'learn';")
            cur.execute("update aqo_queries set use_aqo = true, learn_aqo = false, auto_tuning = false where fspace_hash = (select query_hash from aqo_query_texts where query_text=%s);", (query,))
            conn.commit()
            cur.execute(query)
            res_without = cur.fetchone()[0][0]
            
            cur.execute("update aqo_queries set use_aqo = true, learn_aqo = true, auto_tuning = false where fspace_hash = (select query_hash from aqo_query_texts where query_text=%s);", (query,))
            conn.commit()
            cur.execute(query)

            dictt_with = {}

            lss.append([i + 2, res_without])

            cur.execute("set aqo.mode = 'disabled';")
            conn.commit()
            cur.execute("select * from aqo_query_stat where query_hash = (select query_hash from aqo_query_texts where query_text=%s);", (query,))
            res_stats = cur.fetchone()
            ls.append([i + 2, res_stats[0],res_stats[1],res_stats[2],res_stats[3],res_stats[4],res_stats[5],res_stats[6],res_stats[7],res_stats[8]])
            conn.commit()
        cur.execute("drop extension aqo;")
        conn.commit()


        cur.close()
    except psycopg2.DatabaseError as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
