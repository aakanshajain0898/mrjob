#!/usr/bin/env python
import csv

if __name__=='__main__':
    courses=[]
    job_posts=[]
    with open('coursea_data-1.csv', 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            courses.append(row)         

    with open('data_job_posts.csv', 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
        for row in csvreader:
            row[0] = row[0].replace("\n", " ")
            row[0] = row[0].replace("\t", " ")
            job_posts.append(row)        
 
    courses_hash=[]
    for post in job_posts:
        for course in courses:
            course_item = course[1].split(" ")
            if ([ele for ele in course_item if(ele in post[0])]) :
                courses_hash.append([post[0], course[1], course[4] ]) 
 
    with open('recommended_courses.data','w+') as f:
        for course in courses_hash:
            f.write(course[0]+'\t'+course[1]+'\t'+course[2]+'\n')

    print("PreProcessing Done ---------------------------------------------------------->")       
  
