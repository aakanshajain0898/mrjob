#!/usr/bin/env python
# coding=utf-8
from mrjob.job import MRJob
from mrjob.step import MRStep
 
class CourseRecommendation(MRJob):
    """ 
    The is to aggregate all rating data of a single job_post
    The format is: job_post, (course_item_count, [(course_title,course_rating)...])
    """
    def group_by_course_rating(self, _, line):
        """
        The mapper output is:
        abc bcd,4.7
        xyz vxy,1.7
        abc cde,2.7
        xyz uvx,3.7
        """
        job_post ,course_title, course_rating = line.split('\t')
        yield job_post, (course_title, float(course_rating))
 
    def count_ratings_freq(self, job_post, values):
        """
        The reducer output is:
        abc ( [2,[[bcd,4.7],[cde,2.7]]])
        xyz ( [2,[[vxy,1.7],[uvx,3.7]]])
        """
        course_count = 0
        ratings = []
        for course_title, course_rating in values:
            course_count += 1
            ratings.append((course_title, course_rating))
        yield job_post, (course_count, ratings) 
 
    def steps(self):
        return [
            MRStep(mapper=self.group_by_course_rating,
                reducer=self.count_ratings_freq),
        ]

if __name__ =='__main__':
    CourseRecommendation.run()                      
