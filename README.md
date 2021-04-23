# A Python helper script to launch small parallel data retrieval tasks using FLEX_EXTRACT


## What's this

[**FLEX_EXTRACT**](https://www.flexpart.eu/flex_extract/index.html#) is

> an open-source software to retrieve meteorological fields from the MARS
> archive of the European Centre for Medium-Range Weather Forecasts (ECMWF) to
> serve as input for the FLEXTRA/FLEXPART atmospheric transport modelling
> system.

The current version of `FLEX_EXTRACT` (v7.1.2, time of writing: 2021-04-23)
will send a retrieval to MARS of the desired meteorological fields specified
by a "CONTROL" file, and perform some pre-processing on the downloaded data.

However, the pre-processing is only performed after all data during a given
period are ready. If the downloading gets interrupted during the process, you
have to start over again. For retrievals of large amounts this can be very
time consuming.

This helper tool divides the requested data period into smaller chunks, e.g. one
for a week's data, and launch a maximum of N workers to consume the job queue.

One can optionally specify a time-out period for each chunk so it won't block
forever. Once the chunk's data are downloaded and pre-processing finished, a
dummy text file is created to indicate a job-done. Subsequent runs of the helper tool,
if needed to fill-up the failed sub-jobs, will skip those finished jobs, by
looking for this dummy text file.

**NOTE** that this is only used for the **local mode** of `FLEX_EXTACT`. See
[their doc](https://www.flexpart.eu/flex_extract/installation.html) for more
info on different modes.

## Usage

E.g. Modify the `downloader.py` Python script like this:

```
FLEX_EXTRACT_FOLDER='/path/to/flex_extract/'     # FLEX_EXTRACT intallation folder
OUTPUTDIR='/path/to/save/data/'                  # folder to save outputs

CONTROL_FILE='CONTROL_EI.public'  # CONTROL file used as default
START_DATE='20130201'             # start date
END_DATE='20130206'               # end date
DAYS_PER_JOB=3                    # number of days to retrieve in each sub-job
TIME_OUT=3*60*60                  # seconds, timeout for the submit.py call
TIME_OUT_RETRY=3                  # number of retries if sumbit.py times out
N_WORKERS=3                       # max number of parallel retrievals to launch.
JOB_PREFIX='EI_job'               # prefix string for all sub-jobs
COPY_CONTROL=True                 # whether to make a copy of the CONTROL file for each sub-job

DRY=True                          # If True, only print a summary.

# skip ...


if __name__=='__main__':

    #------------------Get date list------------------
    date_list=breakDownDates(START_DATE, END_DATE, DAYS_PER_JOB)

    #----------------Get submit.py file----------------
    exe_file=checkExeFile(FLEX_EXTRACT_FOLDER)

    #-------------Get default CONTROL file-------------
    ctrl_folder, ctrl_file=checkControlFile(FLEX_EXTRACT_FOLDER, CONTROL_FILE)

    #-----------------Prepare job list-----------------
    job_list=prepareJobList(exe_file, ctrl_file, date_list, OUTPUTDIR, TIME_OUT,
            TIME_OUT_RETRY, JOB_PREFIX, COPY_CONTROL)

    #-------------------Launch jobs-------------------
    if DRY:
        printJobSummary(START_DATE, END_DATE, DAYS_PER_JOB, job_list, N_WORKERS)
    else:
        pool=Pool(N_WORKERS)

        #results=pool.imap_unordered(launchJobUnpack, job_list)
        results=pool.map(launchJobUnpack, job_list)
        for rii in results:
            print(rii)
        print('\n# <serial_batch_job>: Results:', results)
```


## Dependencies

One needs a working `FLEX_EXTRACT` installation to use this. See
https://www.flexpart.eu/flex_extract/Installation/local.html
for instructions.

Other than that, no extra dependencies.

## Contribution

Improvements, bug reports and questions are all welcome. Please just open
a new issue.
