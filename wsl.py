import psycopg2
import csv
import configparser
import json
import random


Config = configparser.ConfigParser()
Config.read("config.ini")

#Fraction change below which the loop will terminate
epsilon = 0.0001

#small value added 0 to avoid floating point exception while calculating fraction change
delta = 0.000000005

#get database info from config file
hostname = Config.get("nucleus","hostname")
username = Config.get("nucleus","username")
password = Config.get("nucleus","password")
database = Config.get("nucleus","database")

#Return a list of proficiency value of each user.
def proficiency_in_sub(conn, sub):
    cur = myConnection.cursor()
    cur.execute("select proficiency->'badge' from deepend_lbt where subject_id = '{}'".format(sub))
    items = [item[0] for item in cur.fetchall()]
    ret = [0]*len(items)
    max_cit = 1;
    #If the badge is null that means the user has not attempted any assesment yet.
    for i in range(len(items)):
        if items[i] is None:
            ret[i] = 0.0 + delta
        elif items[i] == "0.00-0.20":
            ret[i] = 0.2 + delta
        elif items[i] == "0.20-0.40":
            ret[i] = 0.4 + delta
        elif items[i] == "0.40-0.60":
            ret[i] = 0.6 + delta
        elif items[i] == "0.60-0.80":
            ret[i] = 0.8 + delta
        elif items[i] == "0.80-1.00":
            ret[i] = 1 + delta
    return ret

#Place holder for endorsement data. It will return a adjacency matrix of n*n which will give information about which user is endorsed by whom.
def place_holder_endorsements(n):
    endorsed_by = [[0]*n for i in range(n)];
    #for now it will have randomly 0/1 about whether the user j has endorsed user i or not.
    for i in range(n):
        for j in range(n):
            endorsed_by[i][j] = random.randint(0,1)
    return endorsed_by

#Place holder for authority. It will return random value between 1-50 which will give information about contribution of each user in the given subject.
def place_holde_auth_seed(n):
    authority_seed = [0]*n
    max_cont = 1;
    for i in range(n):
        authority_seed[i] = random.randint(1,50)
        max_cont = max(max_cont, authority_seed[i]);
    for i in range(n):
        authority_seed[i] /= max_cont*1.0
    return authority_seed

#Main function to calculate final authority andcitizen score using HITS dual relation algorithm.
def auth_citi(conn, n, sub):
    percent_change = 10000;
    endorsement_mat = place_holder_endorsements(n)
    authority = place_holde_auth_seed(n)
    citizenship = proficiency_in_sub(conn, sub)

    #this loop will terminate when max change occured in the last iteration was less than the above defined epsilon.
    while(percent_change > epsilon):
        authority_new = [0]*n
        citizenship_new = [0]*n
        max_auth = 0
        max_cit = 0

        #A good authority is whom who got endorsed by good citizens. so adding citizenship of user who endorsed authority i.
        for i in range(n):
            authority_new[i] = authority[i]
            for j in range(n):
                if(endorsement_mat[i][j] == 1):
                    authority_new[i] += citizenship[j]
            max_auth = max(authority_new[i], max_auth)
        #normalising new authority wrt new max.
        for i in range(n):
            authority_new[i] /= max_auth*1.0

        #A good citizen is whom who endorses content of a good authority. so adding authority of users whom citizen i has endorsed.
        for i in range(n):
            citizenship_new[i] = citizenship[i]
            for j in range(n):
                if(endorsement_mat[j][i] == 1):
                    citizenship_new[i] += authority[j]
            max_cit = max(citizenship_new[i], max_cit)
        for i in range(n):
            citizenship_new[i] /= max_cit*1.0

        max_change_a = 0
        max_change_c = 0

        #calculating max change occured after the above step.
        for i in range(n):
            max_change_a = max(max_change_a, abs(authority_new[i] - authority[i])/authority[i])
            max_change_c = max(max_change_c, abs(citizenship_new[i] - citizenship[i])/citizenship[i])
            authority[i] = authority_new[i]
            citizenship[i] = citizenship_new[i]

        percent_change = max(max_change_a, max_change_c)

        print percent_change
    return authority, citizenship


if __name__ == '__main__':
    myConnection = psycopg2.connect( host=hostname, user=username, password = password,  dbname=database)

    #For now code is for english subject
    sub = 'K12.ELA'
    
    csv_file = "auth_citi_score.csv"
    cur = myConnection.cursor()
    cur.execute("select count(*) from deepend_lbt where subject_id = '{}'".format(sub))
    n = int(cur.fetchall()[0][0])
    cur.execute("select user_id from deepend_lbt where subject_id = '{}'".format(sub))
    users = [item[0] for item in cur.fetchall()]
    authority = []
    citizenship = []
    proficiency = proficiency_in_sub(myConnection, sub)

    #for multiple subject we will put a loop for all subjects here.
    authority, citizenship = auth_citi(myConnection, n, sub)
    with open(csv_file, "w") as output:
        writer = csv.writer(output, lineterminator='\n')
        writer.writerow(['user_id', 'subject_id', 'authority', 'citizenship', 'proficiency'])
        for i in range(n):
            writer.writerow([users[i], sub, authority[i], citizenship[i], proficiency[i]])
        print("You're done! Output written to auth_citi_score.csv")
    myConnection.close()
