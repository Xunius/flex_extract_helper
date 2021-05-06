'''Check existance of meteorological input files

Author: guangzhi XU (xugzhi1987@gmail.com)
Update time: 2021-05-06 13:31:05.
'''

from __future__ import print_function
from datetime import datetime, timedelta
import os
import glob

SEARCH_DIR='/home/guangzhi/datasets/flexpart_erai/outputs'               # folder to save outputs


START_DATE='20130203'             # start date
END_DATE='20130207'               # end date
HOURS=range(0, 24, 3)
PREFIX='EI'

def getTargetFiles(search_dir, start_date, end_date, hours, prefix):
    '''Construct list of target data file names

    Args:
        search_dir (str): absolute path to the folder to search for data files.
        start_date (str): start time point in format YYYYMMDD.
        end_date (str): end time point in format YYYYMMDD.
        hours (list/tuple): list/tuple of ints, hours in each day.
        prefix (str): prefix string, e.g. 'EI', 'EA5'.
    Returns:
        exp_list (list): list of strings, filenames of expected data. E.g.
            'EI13010100', 'EI13010106', ... 'EI13053118'.
    '''


    t_start=datetime.strptime(start_date, '%Y%m%d')
    t_end=datetime.strptime(end_date, '%Y%m%d')

    if t_start>t_end:
        raise Exception("<start_date> is later than <end_date>.")

    d1=timedelta(days=1)
    dhours=[timedelta(hours=ii) for ii in hours]
    exp_list=[]

    t1=t_start
    while t1<=t_end:
        for hii in dhours:
            # loop through hours in a day
            tii=t1+hii
            if tii>t_end:
                break
            tii_str=tii.strftime('%y%m%d%H')
            tii_str='%s%s' %(prefix, tii_str)
            exp_list.append(tii_str)

        # to next day
        t1=t1+d1

    return exp_list

def mergeContinuousTime(time_list, time_step):
    '''Merge a list of time points to a nested list of time intervals.

    Args:
        time_list (list/tuple): a list/tuple of time points, as datetime objects.
        time_step (int): maximum allowed time gaps measured in number of hours.
                 This flag defines how far 2 time points are treated as
                 a "break down" gap.
                 E.g. if <time_step> is 6, the any gap between 2 sebsequent
                 values in <time_list> that is larger than 6 hours will be
                 treated as a "break down".
    Returns:
        time_interval (list): a nested list of time intervals denoted by
                            the same format as in <time_list>.
    '''

    time_list.sort()
    index=[]
    for ii in range(len(time_list)-1):
        if (time_list[ii+1]-time_list[ii]).seconds/3600 > time_step:
            index.append(ii+1)

    lines=[time_list[i:j] for i, j in zip([0]+index, index+[None])]
    time_interval=[[ii[0],ii[-1]] for ii in lines]

    return time_interval


#-------------Main---------------------------------
if __name__=='__main__':

    exp_list=getTargetFiles(SEARCH_DIR, START_DATE, END_DATE, HOURS, PREFIX)
    got_files=glob.glob(os.path.join(SEARCH_DIR, '%s*' %PREFIX))
    got_files=[os.path.basename(fii) for fii in got_files]

    missing_list=list(set(exp_list).difference(got_files))

    #------------------Report missing------------------
    missing_dates=[]
    for fii in missing_list:
        tii=fii.lstrip(PREFIX)
        tii=datetime.strptime(tii, '%y%m%d%H')
        missing_dates.append(tii)

    missing_periods=mergeContinuousTime(missing_dates, 3)
    missing_periods=[[tii.strftime('%Y-%m-%d %H') for tii in gii] for gii in missing_periods]

    print('\n# <check_files>: Search period: %s - %s' %(START_DATE, END_DATE))
    print('\n# <check_files>: Missing data periods:')
    for ii in missing_periods:
        print(ii)







