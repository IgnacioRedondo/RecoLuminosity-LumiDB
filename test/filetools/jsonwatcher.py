#!/usr/bin/env python
import sys,time
import json,glob,os,re,itertools
from itertools import izip_longest
from optparse import OptionParser

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def splitjson(inputjson,basefilename,workdir,nitems):
    '''
    split basefilename into pieces with nitems per piece
    '''
    [basebase,basesuffice]=basefilename.split('.')
    outputfilenamebase=basebase+'_{0}.'+basesuffice
    totitems=len(inputjson.keys())
    nblocks=int(totitems/nitems)
    l=totitems%nblocks
    if l: nblocks=nblocks+1
    print '   populating %s, will write into %d files '%(workdir,nblocks)
    for i, group in enumerate(grouper(inputjson, nitems)):
        outputfile = open(os.path.join(workdir,outputfilenamebase.format(i)), 'w')
        if not group: continue
        submap = dict([(k,inputjson[k]) for k in group if k is not None])
        json.dump(submap,outputfile)
    
def findfiles(workdir,basefilename):
    '''
    find input files in workdir of pattern basename_n.suffix
    '''
    filelist=[]
    [basebase,basesuffix]=basefilename.split('.')
    p=re.compile('^'+basebase+'_\d+.'+basesuffix+'$')
    filelist=[f for f in os.listdir(workdir) if re.search(p, f)]
    return filelist

class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):#current added item wrt past
        return self.set_current - self.intersect
    def removed(self):#current removed item wrt past
        return self.set_past - self.intersect
    def changed(self):#current item content changed
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])
    
class FileSetJson(object):
    """
    manipulate json files with the same basename pattern in specified directory
    """
    def __init__(self,filebasename,workdir):
        [self.basefilepattern,self.basefilesuffix]=filebasename.split('.')
        self.workdir=workdir
        filenames=findfiles(workdir,filebasename)
        self.filenames=[os.path.join(workdir,x) for x in filenames]
        self.keyfilemap={}
        self.totaljson={}
        self.filejson={}
        self.changedfiles={'+':{},'-':{},'o':{}}
        self.updatecache()
        
    def files(self):
        return self.filenames
    
    def updatecache(self):
        '''
        build {key:filename_in_fileset}
        '''
        if self.changedfiles.has_key('+'):
            self.filenames=self.filenames+self.changedfiles['+'].keys()
            self.changedfiles={'+':{},'-':{},'o':{}}#reset status
            
        for myf in self.filenames:
            myfile=open(myf,'r')
            myfilestr=myfile.read().replace('\n','').replace(' ','')
            myjson=json.loads(myfilestr)
            myfile.close()
            self.totaljson=dict(self.totaljson.items()+myjson.items())
            self.filejson[myf]=myjson
            for key in myjson.keys():
                self.keyfilemap[key]=myf
            
    def asJSON(self,filename=None):
        '''
        files in set as one json
        if filename == None, all set as one json
        '''
        if filename is None:
            return self.totaljson
        else:
            return self.filejson[filename]
        
    def removeItems(self,pieces):
        '''
        remove json item specified by key from file it belongs
        '''
        for key,value in pieces.items():
            filewithkey=self.keyfilemap[key]
            del self.filejson[filewithkey][key]
            del self.totaljson[key]
            self.changedfiles['-'].setdefault(filewithkey,[]).append((key,value))
        
    def addItem(self,pieces):
        '''
        add new json item to file
        '''
        filenames=sorted(self.filenames)
        maxlastfile=filenames[-1]
        p=re.compile(self.basefilepattern+'_'+'(\d+)\.'+self.basefilesuffix)
        lastmax=p.search(maxlastfile)
        if lastmax:
            lastmax=int(lastmax.groups()[0])
        mynum=lastmax+1
        newfilename=(os.path.join(self.workdir,self.basefilepattern)+'_{0}.'+self.basefilesuffix).format(str(mynum))
        for key,value in pieces.items():
            self.keyfilemap[key]=newfilename
            self.changedfiles['+'].setdefault(newfilename,[]).append((key,value))
        self.filejson[newfilename]=pieces
        self.totaljson=dict(self.totaljson.items()+pieces.items())
        
    def updateItem(self,item):
        '''
        update item on the spot
        '''
        for key,value in item.items():
            filename=self.keyfilemap[key]
            self.filejson[filename][key]=item[key]
            self.totaljson[key]=item[key]
            self.changedfiles['o'].setdefault(filename,[]).append((key,value))
            
    def writeChangedFiles(self):
        '''
        materialize all the changes to disk
        '''
        print 'updated files in %s'%self.workdir
        print 'changes reported in %s,%s'%(os.path.join(self.workdir,'jsonchange.summary'),os.path.join(self.workdir,'jsonchange.detail'))
        for filedelta in self.changedfiles.values():
            filenames=filedelta.keys()
            for filename in filenames:
                outfile=open(filename,'w')
                json.dump(self.filejson[filename],outfile)

    def fileChanged(self):
        '''
        format: {'+':{filename:[(key,value)],'-':{filename:[(key,value)],'o':{filename:[(key,value)]}}
        '''
        return self.changedfiles

    def reportChanges(self):
        '''
        summary of total changes
        will writeout 2 report files
        jsonchange.summary
        jsonchange.detail
        '''
        summaryfilename=os.path.join(self.workdir,'jsonchange.summary')
        detailfilename=os.path.join(self.workdir,'jsonchange.detail')
        fchanged=self.changedfiles.values()
        flatflist=list(itertools.chain.from_iterable(fchanged))
        if len(flatflist)==0:
            print 'no change in json file'
            return 0

        if os.path.exists(summaryfilename):
            os.rename(summaryfilename,summaryfilename+'.bak')
        summaryfile=open(summaryfilename,'w')
        for f in flatflist:
            summaryfile.write(f+'\n')
        summaryfile.close()
        
        if os.path.exists(detailfilename):
            os.rename(detailfilename,detailfilename+'.bak')
        detailfile=open(detailfilename,'w')            
        for optype,opfiles in self.changedfiles.items():
            detailfile.write('  %s :\n'%optype)
            for opfilename,opfilechanged in opfiles.items():
                opcontent=dict(opfilechanged)
                detailfile.write('    %s: %s\n'%(opfilename,str(opcontent)))
        detailfile.close()
        return len(flatflist)
            
if __name__ == '__main__':
    '''
    watch a reference json file in a workdir
    initialize workdir: if workdir empty, populate workdir with splited reference json into n pieces:
    maintain workdir: run command regularly in workdir, the command reports the changes in the reference json and adjust the working dir accordingly
    report format:
        summary report: list of files changed
        detail report: details of each exact chunck changed
        old report files are changed to *.bak
    jsonwatcher -d workdir -n nentries-per-file -i referencejsonfile
    '''
    
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage)
    parser.add_option('-i','--input',type='string',dest='inputfilename',help='input reference file name.')
    parser.add_option('-d','--dir',type='string',dest='workdir',default=os.getcwd(),help='workdir')
    parser.add_option('-n','--nitems',type='int',dest='nitems',default=100,help='split reference json with nitems per piece')
    parser.add_option('--dryrun',dest='isdryrun',action='store_true',default=False,help='dry run mode, not to implement the changes in the mirror json files')
    
    (options, args) = parser.parse_args()
    isdryrun=False
    isdryrun=options.isdryrun
    
    inputfilename=None
    if options.inputfilename is None:
        print 'mandatory option -i is missing\n'
        parser.print_help()
        sys.exit(-1)
    inputfilename=options.inputfilename
    referencefilename=os.path.basename(inputfilename)
    outputjsonfiles=findfiles(options.workdir,referencefilename)
    inputstr=open(inputfilename,'r').read().replace('\n','').replace(' ','')
    inputjson=json.loads(inputstr)
    if len(outputjsonfiles)==0:
        splitjson(inputjson,referencefilename,options.workdir,options.nitems)
        sys.exit(0)

    jsonwatcher=FileSetJson(referencefilename,options.workdir)
    myfilenames=jsonwatcher.files()[:]#deep copy!
    mytotaljson=jsonwatcher.asJSON()
    d=DictDiffer(mytotaljson,inputjson)
    
    removedkeys=list(d.added())#note: it's reverse! added by me means *removed by reference file*
    removeditems={}
    for k in removedkeys:
        removeditems[k]=mytotaljson[k]
    
    addedkeys=list(d.removed())#note: it's reverse! removed by me means added by reference file
    addeditems={}
    for k in addedkeys:
        addeditems[k]=inputjson[k]
        
    changedkeys=[]
    #print mytotaljson
    for myfilename in myfilenames:#loop over on disk files
        myjson=jsonwatcher.asJSON(myfilename)
        #print myjson
        d=DictDiffer(myjson,inputjson)        
        changeditems=d.changed()
        if changeditems:
            changedkeys=changedkeys+list(changeditems)#changed item means change in the content of a key
    modifieditems={}
    for k in changedkeys:
        modifieditems[k]=inputjson[k]
    
    if addeditems:
        jsonwatcher.addItem(addeditems)
    if removeditems:
        jsonwatcher.removeItems(removeditems)
    if modifieditems:
        jsonwatcher.updateItem(modifieditems)
    totchanged=jsonwatcher.reportChanges()
    if not isdryrun:
        if totchanged !=0:
            jsonwatcher.writeChangedFiles()

    
