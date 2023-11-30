### Michael Engel ### 2023-02-06 ### downloadME.py ###
import numpy as np
import sys
import os
import concurrent
import threading
import requests
import time

#%% main
def downloadME(
    url, # desired URL(s): list or string
    savename, # desired name of file(s) (without ending): list or string
    ending = '', # desired ending of file(s): list or string
    
    threads = 0, # number of threads (multithreading for threads>0)
    threads_randomized = True, # query at randomized times, seconds of randomization or False for no randomization (as fast as possible)
    maxtries = 3, # number of trials
    
    quote = False, # quote url(s)?
    chunked = False, # chunksize in Byte, False for not chunking
    retrievekwargs = {}, # for Authorization, API-Keys, etc. pp.
    
    overwrite = False, # overwrite if existing?
    bequiet = False, # print loggings/errors?
    ProgressBar = True, # print progress?
):
    ### list case
    if type(url)==list:
        if type(savename)==list:
            pass
        else:
            savename = [savename+f'_{i}' for i in range(len(url))]
            
        if type(ending)==list:
            pass
        else:
            ending = [ending]*len(url)
            
        if type(quote)==list:
            pass
        else:
            quote = [quote]*len(url)
        
        if type(chunked)==list:
            pass
        else:
            chunked = [chunked]*len(url)
            
        if type(retrievekwargs)==list:
            pass
        else:
            retrievekwargs = [retrievekwargs]*len(url)
            
        assert len(url)==len(savename)
        assert len(savename)==len(ending)
        assert len(quote)==len(url)
        assert len(chunked)==len(url)
        assert len(retrievekwargs)==len(url)
        
    
        if ProgressBar:
            progressbar = ProgressPercentageDownloads(url)
        else:
            progressbar = ProgressBar
            
        download_kwargs = []
        for url_,savename_,ending_,quote_,chunked_,retrievekwargs_ in zip(url,savename,ending,quote,chunked,retrievekwargs):
            download_kwargs.append({})
            download_kwargs[-1].update(
                url = url_,
                savename = savename_, 
                ending = ending_,
                
                threads = 0,
                threads_randomized = threads_randomized,
                maxtries = maxtries,
                
                quote = quote_,
                chunked = chunked_,
                retrievekwargs = retrievekwargs_,
                
                overwrite = overwrite,
                bequiet = bequiet,
                ProgressBar = progressbar
            )
        
        if threads==0:
            results = [downloadME(**kwargs) for kwargs in download_kwargs]
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1,min(threads,len(download_kwargs)))) as executor:
                results = executor.map(lambda kwargs: downloadME(**kwargs), download_kwargs)
            
        return [res for res in results]
    
    ### string case
    else:
        if not overwrite and os.path.exists(savename+ending):
            if not bequiet:
                print(f"### downloadME: {savename+ending} does already exist! ###") # return True reasonable?
        else:
            if threads and threads_randomized:
                time.sleep(np.random.rand()*threads_randomized)
                
            error = 1
            counter = 0
            while error==1 and counter<maxtries:
                try:
                    _retrieve(url=url, outfile=savename+ending, quote=quote, chunked=chunked, **retrievekwargs)
                    error = 0
                except Exception as e:
                    if not bequiet:
                        print(e)
                    error = 1
                counter = counter+1
            
            if ProgressBar:
                ProgressBar()
            
            if error==1:
                if not bequiet:
                    print('### downloadME: download did not work! Maybe the queried url/ressource is not downloadable (e.g. string)? ###')
                deleteME(savename+ending, bequiet=bequiet)
                return False
            
            if os.stat(savename+ending).st_size == 0:
                if not bequiet:
                    print('### downloadME: no data found! ###')
                deleteME(savename+ending, bequiet=bequiet)
                return False
            
        return True


#%% auxiliary
def _retrieve(url, outfile, quote=False, chunked=False, **kwargs):
    if quote:
        url = requests.utils.quote(url)
        
    with requests.get(url, stream = True if chunked else False, **kwargs) as response:
        with open(outfile,"wb") as f:
            if chunked:
                for chunk in response.iter_content(chunk_size=chunked):
                    f.write(chunk)
            else:
                f.write(response.content)
    return True
    
def deleteME(file, bequiet=False):
    if type(file)==list:
        success = []
        for i in range(len(file)):
            success.append(deleteME(file[i]))
        return success
    else:
        try:
            os.remove(file)
            return True
        except Exception as e:
            if not bequiet:
                print(e)
                print("deleteME: removing did not work! Either it is not existing or you don't have permission for that, e.g. if it is still open in another application!")
            return False
        
        return True
    
class ProgressPercentageDownloads(object):
    def __init__(self,files):
        self.files = files
        self.len = len(files)
        self.lock = threading.Lock()
        self.counter = 0
        
    def __call__(self):
        with self.lock:
            percentage = (self.counter+1)/self.len*100
            sys.stdout.write(f"\r{self.counter+1}/{self.len} - ({percentage:.2f}%) ")
            sys.stdout.flush()
            self.counter = self.counter+1

if __name__=="__main__":
    downloadME(
        url = [r"http://wms.geo.admin.ch/?SERVICE=WMS&REQUEST=GetMap&VERSION=1.3.0&LAYERS=ch.swisstopo.images-swissimage&STYLES=default&CRS=EPSG:21781&BBOX=550000,60000,660000,140000&WIDTH=800&HEIGHT=582&FORMAT=image/png"]*10, 
        savename = "testDownloadME",
        ending = '.png',
        
        threads = 4,
        threads_randomized = 2, 
        maxtries = 3,
        
        quote = False,
        chunked = 50*1024,
        retrievekwargs = {},
        
        overwrite = False, 
        bequiet = False, 
        ProgressBar = True,  
    )