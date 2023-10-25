### Michael Engel ### 2022-03-11 ### upload.py ###
import boto3 
from boto3.s3.transfer import TransferConfig
import os
import sys
import threading
import concurrent

thread_local_awsME = threading.local()

#%% main methods
#%%% upload files
def upload(file,bucket,savename=None,region="eu-central-1",AWS_ACCESS_KEY_ID=None,AWS_SECRET_ACCESS_KEY=None,bequiet=True,threads=10,max_bandwidth=None,smallfiles=False):
    #%%%% parse input
    #%%%%% list of files
    if type(file)==list:
        if type(bucket)==list:
            if len(file)==len(bucket):
                pass
            else:
                raise RuntimeError('awsME.upload: length of bucket must be equal to the length of file!')
        else:
            bucket = [bucket]*len(file)
        
        if type(savename)==list:
            if len(file)==len(savename):
                pass
            else:
                raise RuntimeError('awsME.upload: length of savename must be equal to length of file!')
        else:
            savename = [savename]*len(file)
        
        if type(region)==list:
            if len(file)==len(region):
                pass
            else:
                raise RuntimeError('awsME.upload: length of region must be equal to length of file!')
        else:
            region = [region]*len(file)
        
        #%%%%%% query
        if smallfiles:
            upload_args = [(file_,bucket_,savename_,region_,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,bequiet,threads,max_bandwidth,smallfiles) for file_,bucket_,savename_,region_ in zip(file,bucket,savename,region)]
            
            if not bequiet:
                global ProgressBar
                ProgressBar = ProgressPercentageSmallfiles(file)
                
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1,min(threads,len(file)))) as executor:
                success = executor.map(lambda args: upload(*args),upload_args)
            
            if not bequiet:
                print(' DONE')
                
            return [suc for suc in success] # necessary to keep order! Don't just do list(generator)!
        else:
            success = []
            i = 1
            for file_,bucket_,savename_,region_ in zip(file,bucket,savename,region):
                if not bequiet:
                    print(f"{i}/{len(file)} to bucket '{bucket_}' at '{savename_}' in region '{region_}'",end='\n')
                success.append(upload(file=file_,bucket=bucket_,savename=savename_,region=region_,AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY,bequiet=bequiet,threads=threads,max_bandwidth=max_bandwidth,smallfiles=smallfiles))
                i = i+1
            return success
    
    #%%%%%
    elif type(file)==str and type(bucket)==str and type(region)==str:
        if savename==None:
            savename = os.path.basename(file)
        elif type(savename)==str:
            savename = savename.replace('\\','/').strip()
            if savename.strip().startswith('/'):
                savename = savename[1:]
        else:
            return False
    else:
        print('awsME.upload: wrong input given - either string or list of strings!')
        return False
    
    #%%%% config
    config = TransferConfig(multipart_threshold=8388608, max_concurrency=max(1,threads) if not smallfiles else 1, multipart_chunksize=8388608, num_download_attempts=5, max_io_queue=100, io_chunksize=262144, use_threads=False if threads<=1 or smallfiles else True, max_bandwidth=max_bandwidth)
        
    #%%%% client
    s3client = getME_s3client(region=region,AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    
    #%%%% check if bucket exists
    response = s3client.list_buckets()
    buckets = [buck['Name'] for buck in response['Buckets']]
    if bucket in buckets:
        pass
    else:
        raise RuntimeWarning('awsME.upload: bucket does not exist!')
        return False
    
    #%%%% query
    try:
        if not bequiet:
            if smallfiles:
                response = s3client.upload_file(file, bucket, savename, Callback=ProgressBar, Config=config)
            else:
                response = s3client.upload_file(file, bucket, savename, Callback=ProgressPercentage(file), Config=config)
                print(' DONE')
        else:
            response = s3client.upload_file(file, bucket, savename, Config=config)
    except Exception as e:
        print(e)
        print(response)
        return False
    return True

#%%% get client
def getME_s3client(region,AWS_ACCESS_KEY_ID=None,AWS_SECRET_ACCESS_KEY=None):
    if not hasattr(thread_local_awsME,'s3client'):
        session = boto3.session.Session() # thread-safe!
        thread_local_awsME.s3client = session.client(service_name='s3',
                                                     region_name=region,
                                                     aws_access_key_id=AWS_ACCESS_KEY_ID,
                                                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                                                     aws_session_token=None)
    return thread_local_awsME.s3client

#%%% create new bucket
def create_bucket(bucket_name, region="eu-central-1", AWS_ACCESS_KEY_ID=None, AWS_SECRET_ACCESS_KEY=None):
    try:
        s3client = getME_s3client(region=region, AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
        s3client.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(e)
        return False
    return True

#%%% get information about your aws account
def getME_s3info(): ### TODO
    pass

#%% auxiliary methods
#%%% display progress
class ProgressPercentage(object): ### copied from aws doc: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html ###
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

class ProgressPercentageSmallfiles(object): ### adapted version for smallfiles implementation
    def __init__(self,files):
        self.files = files
        self.len = len(files)
        self.size = float(sum([os.path.getsize(file) for file in files]))
        self.seen = 0
        self.lock = threading.Lock()
        self.counter = 0
        
    def __call__(self,bytes_amount):
        with self.lock:
            self.seen = self.seen + bytes_amount
            percentage = (self.seen/self.size)*100
            sys.stdout.write(f"\r{self.counter+1}/{self.len} {self.seen}/{self.size} ({percentage:.2f}%)")
            sys.stdout.flush()
            self.counter = self.counter+1

#%% main
if __name__=='__main__':
    import time
    print('Starting Upload!')
    start = time.time()
    upload(
        files = <>,
        bucket = <>,
        savename = <>,
        region = <>,
        AWS_ACCESS_KEY_ID = <>,
        AWS_SECRET_ACCESS_KEY = <>,
        bequiet = False,
        threads = <>,
        max_bandwidth = 1e23,
        smallfiles = False
    )
    stop = time.time()
    print(f'Upload took {stop-start} seconds!')