
import psycopg2
import csv
import configparser
import json

Config = configparser.ConfigParser()
Config.read("../Relevance/config.ini")

hostname = Config.get("deepend","hostname")
username = Config.get("deepend","username")
password = Config.get("deepend","password")
database = Config.get("deepend","database")


hostname2 = Config.get("nucleus","hostname")
username2 = Config.get("nucleus","username")
password2 = Config.get("nucleus","password")
database2 = Config.get("nucleus","database")

w = 0.3

def getEngagement(conn,conn2,original_id):
        cur = conn.cursor()
        cur2 = conn2.cursor()
        cur2.execute("select id from content where original_content_id =" + "'{}'".format(original_id) )
        copied_ids = [items[0] for items in cur2.fetchall()]
        # print copied_ids
        competencies = {}
        for activity_id in copied_ids:
            cur.execute("select distinct(competency_code) from competency_collection_map where collection_id in (select collection_id from ds_master where resource_id = '{}')".format(activity_id))
            competency = [items[0] for items in cur.fetchall()]
            # print competency
            reslist = []
            res = []
            eng_list = []
            for m in competency:
                mq = "select distinct(resource_id) from ds_master where collection_id in ( select collection_id from competency_collection_map where competency_code = '{}')".format(m);
                cur.execute(mq)
                reslist = [items[0] for items in cur.fetchall()]
                cur.execute("select sum(reaction) from ds_master where resource_id = '{}' group by resource_id".format(activity_id))
                reac = [items[0] for items in cur.fetchall()]
                maxreacq = 'select sum(reaction) from ds_master where resource_id in (\'' + '\',\''.join(map(str, reslist)) + '\') group by resource_id';

                cur.execute(maxreacq)
                reaclist = [items[0] for items in cur.fetchall()]
                maxreac=0
                reaction_L=0
                if reaclist:
                    maxreac = max(reaclist)
                if reac:
                    reaction_L=int(reac[0])
                if maxreac:
                    reaction_L=float(reaction_L)/maxreac
                query="select sum(views) from ds_master where resource_id = '{}' group by resource_id".format(activity_id)
                cur.execute(query)
                views=[items[0] for items in cur.fetchall()]
                query = 'select sum(views) from ds_master where resource_id in (\'' + '\',\''.join(map(str, reslist)) + '\') group by resource_id'
                cur.execute(query)

                viewlist=[items[0] for items in cur.fetchall()]
                maxview=0;
                view_L=0
                if viewlist:
                    maxview=max(viewlist)
                if views:
                    view_L=int(views[0])
                if maxview:
                    view_L=float(view_L)/maxview

                engagement = w*view_L + (1-w) * reaction_L
                eng_list.append(engagement)
            for i in range(len(competency)):
                if competency[i] in competencies:
                    competencies[competency[i]][0] += eng_list[i]
                    competencies[competency[i]][1] += 1
                else:
                    competencies[competency[i]] = [eng_list[i], 1]
            # print competencies
        fin_ans = {}
        for key, value in competencies.items():
            fin_ans[key] = value[0]/value[1]
        if fin_ans:
            print("original_id: ",  fin_ans)
        return fin_ans

def calculateEngagement( conn,conn2) :
    cur = conn2.cursor()
    cur.execute("select distinct(activity_id) from deepend_abt")
    resource = cur.fetchall()
    # json_obj=getEngagement(conn,conn2,"b1ca9fcc-1a44-492a-9802-4a2ca909080a")
    csv_file="/goorulabs/relevance/testing_engagement_score.csv"
    with open(csv_file, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(['Resource ID', 'Engagement'])

        for p in resource:
            if p[0] == None:
                continue
            json_obj=getEngagement(conn,conn2,p[0])
            writer.writerow([p[0], json.loads(json.dumps(json_obj))])
	    # print json_obj
        print("You're done! Output written to engagement_score.csv")


myConnection = psycopg2.connect( host=hostname, user=username, password = password,  dbname=database)
myConnection2 = psycopg2.connect( host=hostname2, user=username2, password = password2,  dbname=database2)
calculateEngagement( myConnection, myConnection2)
myConnection.close()
myConnection2.close()
