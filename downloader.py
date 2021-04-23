'''Submit parallel jobs to retrieve ECMWF meteorological data using flex_extract.

Author: guangzhi XU (xugzhi1987@gmail.com)
Update time: 2021-04-16 21:47:14.
'''

from __future__ import print_function
import os
import re
from datetime import datetime, timedelta
import subprocess
from multiprocessing import Pool

#---------Globals--------
FLEX_EXTRACT_FOLDER='/home/guangzhi/Downloads/flex_extract/'     # FLEX_EXTRACT intallation folder
OUTPUTDIR='/home/guangzhi/datasets/flexpart_erai/'               # folder to save outputs

CONTROL_FILE='CONTROL_EI.public'  # CONTROL file used as default
START_DATE='20130206'             # start date
END_DATE='20130208'               # end date
DAYS_PER_JOB=1                    # number of days to retrieve in each sub-job
TIME_OUT=4*60*60                  # seconds, timeout for the submit.py call
TIME_OUT_RETRY=3                  # number of retries if sumbit.py times out
N_WORKERS=3                       # max number of parallel retrievals to launch.
JOB_PREFIX='EI_job'               # prefix string for all sub-jobs
COPY_CONTROL=True                 # whether to make a copy of the CONTROL file for each sub-job

DRY=True                          # If True, only print a summary.



def launchJobUnpack(arg_dict):
    '''Unpack input arguments and feed into the job-process func.
    '''

    args=arg_dict['args']
    job_dates=arg_dict['job_dates']
    timeout=arg_dict['timeout']
    retry=arg_dict['retry']
    logfile=arg_dict['logfile']

    return launchJob(args, job_dates, logfile, timeout, retry)

def launchJob(args, job_dates, logfilename, timeout=None, retry=3):
    '''Run the submit.py script to process a job

    Args:
        args (list): list of strings. This is the commandline command to run
            in the shell: "python -u /path/to/submit.py --controlfile etc."
        job_dates (tuple): tuple of strings: (id, start_date, end_date). Only
            used as a short description of the job for printing/logging purposes.
        logfilename (str): absolute path to a file to log outputs of the job.
    Keyword Args:
        timeout (int or None): time out period (in seconds). If None, no time
            out.
        retry (int): if <timeout> is not None, the maximum number of retries if
            the call of `submit.py` times out.
    Returns:
        ret (int): returncode of the subprocess call to `sumbit.py`. 0 for
            success, non-0 otherwise.

    The function issues a call to the `sumbit.py` script using subprocess,
    and captures (with no buffering) the stdout and stderr of `sumbit.py`
    to a log file. One can use the `tail -f logfile` command to monitor the
    process in real-time.

    After sucessful run (ret==0), it will write a dummy text file with content
    "job done." to the same folder as the output file saving folder. This dummy
    file is used to skip the job if the script is executed for a second time.
    Delete this file to force a re-run.
    '''

    with open(logfilename, 'w') as logfile:
        tried=0
        ret=1

        while tried<=retry:
            proc=subprocess.Popen(args, stdout=logfile, stderr=subprocess.STDOUT,
                    bufsize=0)
            try:
                print('\n# <serial_batch_job>: launch of job', str(job_dates), 'try #', tried+1)
                ret=proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired as e:
                print('\n# <serial_batch_job>: try #%d for job' %(tried+1), str(job_dates), 'timed out.')
                print(e)
                proc.kill()
                tried+=1
            except Exception as e:
                print('\n# <serial_batch_job>: job failed:', str(job_dates))
                print(e)
                break
            else:
                if ret==0:
                    # write a dummy text file if job done, such that subsequent run can skip it
                    outputdir=args[args.index('--outputdir')+1]
                    if os.path.exists(outputdir):
                        with open(os.path.join(outputdir, 'job_done'), 'w') as fout:
                            fout.write('job done.')
                break

    return ret


def checkControlFile(flex_extract_folder, control_file):
    '''Check existance of default CONTROL file

    Args:
        flex_extract_folder (str): absolute path to the FLEX_EXTRACT installation
            folder.
        control_file (str): name of the CONTROL file used as default, e.g.
            CONTROL_EI.public
    Returns:
        ctrl_folder (str): absolute path to the folder containing CONTROL file.
            By default this is /flex_extract_folder/Run/Control/.
        ctrl_file (str): absolute path to the CONTROL file used as default.
            E.g. /flex_extract_folder/Run/Control/CONTROL_EI.public
    '''

    ctrl_folder=os.path.join(flex_extract_folder, 'Run/Control')
    ctrl_file=os.path.join(ctrl_folder, control_file)

    if not os.path.exists(ctrl_file):
        raise Exception("CONTROL file %s not found." %ctrl_file)

    return ctrl_folder, ctrl_file


def checkExeFile(flex_extract_folder):
    '''Check existance of submit.py script

    Args:
        flex_extract_folder (str): absolute path to the FLEX_EXTRACT installation
            folder.
    Returns:
        exe_file (str): absolute path to the `submit.py` file.
            E.g. /flex_extract_folder/Source/Python/submit.py
    '''

    exe_file=os.path.join(flex_extract_folder, 'Source/Python/submit.py')
    exe_file=os.path.abspath(exe_file)

    if not os.path.exists(exe_file):
        raise Exception("submit.py file %s not found at here:" %exe_file)

    return exe_file

def replaceControlDates(ctrl_str, start_date, end_date):
    '''Write/replace START_DATE and END_DATE to a CONTROL file contents

    Args:
        ctrl_str (str): contents of the default CONTROL file.
        start_date (str): start date in format "YYYYMMDD"
        end_date (str): end date in format "YYYYMMDD"
    Returns:
        ctrl_str (str): contents of the CONTROL file with the START_DATE
            and END_DATE options as specified by <start_date> and <end_date>

    The function will perform a regex search in <ctrl_str> looking for options
    of START_DATE and END_DATE. If found, replace with <start_date> or <end_date>.
    Otherwise, add a new line at the beginning of the string.
    '''

    start_pattern=re.compile(r'^START_DATE +(\d{8})', re.MULTILINE)
    end_pattern=re.compile(r'^END_DATE +(\d{8})', re.MULTILINE)

    try:
        end_pattern.search(ctrl_str).group(1)
    except:
        print("END_DATE line not in CONTROL file. Add line.")
        ctrl_str='END_DATE ' + end_date + '\n' + ctrl_str
    else:
        ctrl_str=end_pattern.sub('END_DATE %s' %end_date, ctrl_str)

    try:
        start_pattern.search(ctrl_str).group(1)
    except:
        print("START_DATE line not in CONTROL file. Add line.")
        ctrl_str='START_DATE ' + start_date + '\n' + ctrl_str
    else:
        ctrl_str=start_pattern.sub('START_DATE %s' %start_date, ctrl_str)

    return ctrl_str


def breakDownDates(start_date, end_date, days_per_job):
    '''Break a date range into a list of non-overlapping chunks

    Args:
        start_date (str): start date in format "YYYYMMDD"
        end_date (str): end date in format "YYYYMMDD"
        days_per_job (int): number of days in each chunk.
    Returns:
        date_list (list): list of (day1, day2) tuples, where day1 is the start
            of the chunk, day2 the end. day2 of the last tuple is <end_date>.
    '''

    if days_per_job<1:
        raise Exception("<days_per_job> should be >=1.")

    t_start=datetime.strptime(start_date, '%Y%m%d')
    t_end=datetime.strptime(end_date, '%Y%m%d')

    if t_start>t_end:
        raise Exception("<start_date> is later than <end_date>.")

    d1=timedelta(days=1)
    dt=timedelta(days=days_per_job-1)

    #-----------Get time period for each job-----------
    date_list=[]
    t1=t_start
    t2=t1+dt

    while t1<=t_end:
        if t2>t_end:
            t2=t_end
        t1_str=t1.strftime('%Y%m%d')
        t2_str=t2.strftime('%Y%m%d')
        date_list.append((t1_str, t2_str))
        t1=t2+d1
        t2=t1+dt

    return date_list


def isSkip(outputdir):
    '''Check whether to skip the job or not by existance of a record file

    Args:
        outputdir (str): absolute path to the folder to save the final outputs
            of `sumbit.py`.
    Returns:
        result (bool): whether a 'job_done' file exists in <outputdir>.
    '''

    rec_file=os.path.join(outputdir, 'job_done')
    return os.path.exists(rec_file)


def prepareJobList(exe_file, ctrl_file, date_list, outputdir, time_out=None,
        time_out_retry=3, job_prefix='ecmwf_retrieve_log',
        copy_control=True, verbose=True):
    '''Prepare sub-jobs

    Args:
        exe_file (str): absolute path to the `submit.py` script.
            E.g. /flex_extract_folder/Source/Python/submit.py
        ctrl_file (str): absolute path to the CONTROL file used as default.
            E.g. /flex_extract_folder/Run/Control/CONTROL_EI.public
        date_list (list): list of (day1, day2) tuples, where day1 is the start
            of the chunk, day2 the end. day2 of the last tuple is <end_date>.
        outputdir (str): absolute path to the folder to save results.
    Keyword Args:
        timeout (int or None): time out period (in seconds) of the subprocess
            call to the `sumbit.py` script. If None, no time out.
        time_out_retry (int): if <timeout> is not None, the maximum number of
            retries if the call of `submit.py` times out.
        job_prefix (str): prefix to the sub-folder inside <outputdir> to save
            outputs of a sub-job. The sub-folder will have a name of
            "<job_prefix>_<nn>_<start_date>-<end_date>", and the log file of
            the sub-job will have a filename of "<job_prefix>_log_<nn>.txt",
            where <nn> is the job id.
        copy_control (bool): If True, create a copy of the default CONTROL file
            as specified by <ctrl_file> and replace the START_DATE and END_DATE
            records within. The copied file is saved to the same folder as
            <ctrl_file>, and the `sumbit.py` is called with the `--controlfile`
            option pointing to the copied filed. This way, if you modify the
            default CONTROL during the process of the script, subsequent
            calls to the `submit.py` won't be affected.
            If False, don't copy the default CONTROL file, and the `submit.py`
            is called with `--controlfile <ctrl_file>`. Be careful that if
            the entire retrieval task takes a long time to finish and you change
            the contents of <ctrl_file>, subsequent `sumbit.py` calls will
            get affected.
    Returns:
        job_list (list): list of dicts, each describing a retrieval job of
            a small chunk. See launchJob() for details of the dict.
    '''

    if copy_control:
        ctrl_folder, ctrl_fname=os.path.split(ctrl_file)

    ctrl_str=open(ctrl_file, 'r').read()
    if verbose:
        print('\n# <prepareJobList>: Default CONTROL file:',)
        print(ctrl_str)

    job_list=[]
    for idii, dateii in enumerate(date_list):

        t1ii, t2ii = dateii
        idii=str(idii).rjust(len(str(len(date_list))), '0')

        outputdirii=os.path.join(outputdir, '%s_%s_%s-%s' %(job_prefix, idii, t1ii, t2ii))
        outputdir_tmpii=outputdirii+'_tmp'
        outputdir_outii=outputdirii+'_out'
        if isSkip(outputdir_outii):
            print('# <prepareJobList>: Skip job #%s: %s-%s' %(idii, t1ii, t2ii))
            continue
        else:
            print('# <prepareJobList>: Add job #%s: %s-%s' %(idii, t1ii, t2ii))

        if copy_control:
            ctrl_fileii=os.path.join(ctrl_folder, '%s_%s_%s' %(ctrl_fname, job_prefix, idii))
            ctrl_strii=replaceControlDates(ctrl_str, t1ii, t2ii)
            with open(ctrl_fileii, 'w') as fout:
                fout.write(ctrl_strii)
        else:
            ctrl_fileii=ctrl_file

        jobii=['python', '-u', exe_file, '--controlfile', ctrl_fileii,
            '--inputdir', outputdir_tmpii,
            '--outputdir', outputdir_outii,
            ]
        # for the -u flag, see: https://stackoverflow.com/a/5922599/2005415

        job_list.append({'args': jobii,
                         'job_dates': (idii, t1ii, t2ii),
                         'timeout': time_out,
                         'logfile': '%s_%s.txt' %(job_prefix, idii),
                         'retry': time_out_retry})

    return job_list


def printJobSummary(start_date, end_date, days_per_job, job_list, n_workers):

    print('\n# <downloader>: ####### Job summary #######')

    print('Time period: %s - %s' %(start_date, end_date))

    t_start=datetime.strptime(start_date, '%Y%m%d')
    t_end=datetime.strptime(end_date, '%Y%m%d')
    n_days=(t_end-t_start).days

    print('Total number of days = %d' %n_days)
    print('Number of days for each job = %d' %days_per_job)
    print('Number of jobs = %d' %(len(job_list)))
    print('Maximum number of parallel jobs = %d' %n_workers)

    print('\n######### Loop through jobs ##############')

    for jobii in job_list:
        idii, t1ii, t2ii = jobii['job_dates']
        logii=jobii['logfile']
        retryii=jobii['retry']
        timeoutii=jobii['timeout']
        argsii=jobii['args']
        ctrlii=argsii[argsii.index('--controlfile')+1]
        inputii=argsii[argsii.index('--inputdir')+1]
        outii=argsii[argsii.index('--outputdir')+1]

        print('\n#### Summary of job #%s:' %idii)
        print('Time period: %s - %s' %(t1ii, t2ii))
        print('CONTROL file:', ctrlii)
        print('Log file:', logii)
        print('inputdir:', inputii)
        print('outputdir:', outii)
        print('Time-out (seconds):', timeoutii)
        print('Time-out retries:', retryii)

    return

def main(flex_extract_folder, control_file, start_date, end_date, days_per_job,
        n_workers, outputdir, time_out, time_out_retry, job_prefix, copy_control, dry):
    '''Start parallel jobs

    Args:
        flex_extract_folder (str): absolute path to the FLEX_EXTRACT installation
            folder.
        control_file (str): name of the CONTROL file used as default, e.g.
            CONTROL_EI.public
        start_date (str): start date in format "YYYYMMDD"
        end_date (str): end date in format "YYYYMMDD"
        days_per_job (int): number of days in each chunk.
        n_workers (init): max number of parallel retrievals to launch.
        outputdir (str): absolute path to the folder to save results.
        timeout (int or None): time out period (in seconds) of the subprocess
            call to the `sumbit.py` script. If None, no time out.
        time_out_retry (int): if <timeout> is not None, the maximum number of
            retries if the call of `submit.py` times out.
        job_prefix (str): prefix to the sub-folder inside <outputdir> to save
            outputs of a sub-job. The sub-folder will have a name of
            "<job_prefix>_<nn>_<start_date>-<end_date>", and the log file of
            the sub-job will have a filename of "<job_prefix>_log_<nn>.txt",
            where <nn> is the job id.
        copy_control (bool): If True, create a copy of the default CONTROL file
            as specified by <ctrl_file> and replace the START_DATE and END_DATE
            records within. The copied file is saved to the same folder as
            <ctrl_file>, and the `sumbit.py` is called with the `--controlfile`
            option pointing to the copied filed. This way, if you modify the
            default CONTROL during the process of the script, subsequent
            calls to the `submit.py` won't be affected.
            If False, don't copy the default CONTROL file, and the `submit.py`
            is called with `--controlfile <ctrl_file>`. Be careful that if
            the entire retrieval task takes a long time to finish and you change
            the contents of <ctrl_file>, subsequent `sumbit.py` calls will
            get affected.
        dry (bool): if True, only print summary of jobs.
    '''

    #------------------Get date list------------------
    date_list=breakDownDates(start_date, end_date, days_per_job)

    #----------------Get submit.py file----------------
    exe_file=checkExeFile(flex_extract_folder)

    #-------------Get default CONTROL file-------------
    ctrl_folder, ctrl_file=checkControlFile(flex_extract_folder, control_file)

    #-----------------Prepare job list-----------------
    job_list=prepareJobList(exe_file, ctrl_file, date_list, outputdir, time_out,
            time_out_retry, job_prefix, copy_control)

    #-------------------Launch jobs-------------------
    if DRY:
        printJobSummary(start_date, end_date, days_per_job, job_list, n_workers)
    else:
        pool=Pool(n_workers)

        #results=pool.imap_unordered(launchJobUnpack, job_list)
        results=pool.map(launchJobUnpack, job_list)
        for rii in results:
            print(rii)
        print('\n# <serial_batch_job>: Results:', results)


#-------------Main---------------------------------
if __name__=='__main__':

    main(FLEX_EXTRACT_FOLDER, CONTROL_FILE, START_DATE, END_DATE, DAYS_PER_JOB,
            N_WORKERS, OUTPUTDIR, TIME_OUT, TIME_OUT_RETRY, JOB_PREFIX,
            COPY_CONTROL, DRY)












