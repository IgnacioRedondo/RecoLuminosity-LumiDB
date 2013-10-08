#!/usr/bin/env python
import csv,os,re,sys
from optparse import OptionParser

def findfiles(workdir,basefilename):
    '''
    find input files in workdir
    '''
    filelist=[]
    [basebase,basesuffix]=basefilename.split('.')
    p=re.compile('^'+basebase+'_\d+.'+basesuffix+'$')
    filelist=[f for f in os.listdir(workdir) if re.search(p, f)]
    return filelist


def myfieldsorter(*items):
    '''
    algorithm method for sorting string fields with
    runnum:fillnum,lumils:cmsls 
    '''
    print items
    if len(items) == 1:
        item = items[0]
        def g(obj):
            v=obj[item]
            fields=[int(x) for x in v.split(':') if x]
            return tuple(fields[0])#sort by run num
    else:
        def g(obj):
            fields=[]
            for item in items:
              v=obj[item]
              fields=fields+v.split(':')
            print fields
            fields=[fields[0],fields[2]]#sort by lumilsnum
            return tuple(fields)
    return g
    
def mergecsvfiles(oldfilename,newfilename,nidfields):
    '''
    merge new csv file into the old.
    if idfields duplicate found, replace old with new
    else append new to the old
    sort old according to idfields
    return old
    '''
    newfile=None
    try:
        newfile=open(newfilename,"rb")
    except IOError:
        print 'cannot open file ',newfile
        return
    oldfile=None
    try:
        oldfile=open(oldfilename,"rb")
    except IOError:
        newfile.close()
        os.rename(newfilename,oldfilename)
        return

    outcsvreader=csv.reader(oldfile)
    headeriter=outcsvreader.next()
    outputheader=[]
    for r in headeriter:
        outputheader.append(r)
        
    incsvreader=csv.reader(newfile)
    
    outputdata=[]    
    outblock=[line for line in outcsvreader]
    
    inblock=[line for line in incsvreader]

    newfile.close()
    oldfile.close()
    
    if len(outblock[1:])!=0:
        outputdata=outblock[1:]
    else:
        os.rename(newfile,oldfile)
        return
    
    for inputrow in inblock[1:]:
        inputheadcol=inputrow[:nidfields]
        isnewrow=True
        for index,outputrow in enumerate(outputdata):
            if inputrow[:nidfields] == outputrow[:nidfields]: #found overlap, replace
                outputdata[index]=inputrow
                isnewrow=False
                break
        if isnewrow:
            outputdata.append(inputrow) #found new row, append
    if isnewrow:
        fieldargs=range(nidfields)
        outputdata=sorted( outputdata,key=myfieldsorter(*fieldargs) )
    outputdata.insert(0,outputheader)
    outfile=open(oldfilename,"w")
    outwriter=csv.writer(outfile)
    outwriter.writerows(outputdata)
    outfile.close()
    os.remove(newfilename)
    return

if __name__ == '__main__':
    usage = 'usage: %prog [options]'
    parser = OptionParser(usage)
    parser.add_option('-i','--input',type='string',dest='inputfilename',help='input file name. The input file will be deleted after merging.')
    parser.add_option('-a','--all',dest='runall',default=False,action='store_true',help='all files found in workdir of name pattern outputfile_{n}.csv will be merged to outputfile.csv')
    parser.add_option('-o','--output',type='string',dest='outputfilename',help='output file name. Will be created if non-existing.')
    parser.add_option('-d','--dir',type='string',dest='workdir',default=os.getcwd(),help='workdir')
    (options, args) = parser.parse_args()

    workdir=options.workdir    
    runall=options.runall
    inputfilenames=[]
    inputfilename=None
    
    if options.outputfilename is None:
        print 'mandatory option -o is missing\n'
        parser.print_help()
        sys.exit(-1)       
    if not runall:
        inputfilename=options.inputfilename
        if not inputfilename:
            print 'either -a or -i is required\n'
            parser.print_help()
            sys.exit(-1)
        inputfilenames.append(inputfilename)
    else:
        inputfilenames=findfiles(workdir,os.path.basename(options.outputfilename))

    if not inputfilenames:
        print 'no input files found, do nothing.'
        sys.exit(-1)
    for inputfilename in inputfilenames:
      if not os.path.isfile(os.path.join(workdir,inputfilename)):
          print '  input file %s does not exist in %s, do next...\n'%(inputfilename,workdir)
          continue
      print '  Merging %s into %s in %s...\n'%(inputfilename,options.outputfilename,workdir)
      #mergecsvfiles(os.path.join(workdir,options.outputfilename),os.path.join(workdir,inputfilename),2)
    
    
    
