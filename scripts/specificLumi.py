#!/usr/bin/env python
#
# dump all fills into files.
# allfills.txt all the existing fills.
# fill_num.txt all the runs in the fill
# dumpFill -o outputdir
# dumpFill -f fillnum generate runlist for the given fill
#
import os,os.path,sys,math,array,datetime,time
import coral

from RecoLuminosity.LumiDB import argparse,lumiTime,CommonUtil,lumiQueryAPI

allfillname='allfills.txt'

class constants(object):
    def __init__(self):
        self.debug=False
        self.nbx=3564
        self.normfactor=6.37
        self.xingMinLum=1.0E-04
        #self.lumischema='CMS_LUMI_PROD'
        self.lumischema='CMS_LUMI_DEV_OFFLINE'
        #self.lumidb='sqlite_file:///afs/cern.ch/user/x/xiezhen/w1/luminewschema/CMSSW_3_8_0/src/RecoLuminosity/LumiProducer/test/lumi.db'
        self.lumidb='oracle://cms_orcoff_prep/cms_lumi_dev_offline'
        #self.lumidb='oracle://cms_orcoff_prod/cms_lumi_prod'
        self.runsummaryname='CMSRUNSUMMARY'
        self.lumisummaryname='LUMISUMMARY'
        self.lumidetailname='LUMIDETAIL'
        self.allfillname='allfills.txt'

def filltofiles(allfills,runsperfill,runtimes,dirname):
    f=open(os.path.join(dirname,allfillname),'w')
    for fill in allfills:
        print >>f,'%d'%(fill)
    f.close()
    for fill,runs in runsperfill.items():
        filename='fill_'+str(fill)+'.txt'
        if len(runs)!=0:
            f=open(os.path.join(dirname,filename),'w')
            for run in runs:
                print >>f,'%d,%s'%(run,runtimes[run])
            f.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),description = "Dump Fill",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parse arguments
    parser.add_argument('-c',dest='connect',action='store',required=False,help='connect string to lumiDB,optional',default='frontier://LumiProd/CMS_LUMI_PROD')
    parser.add_argument('-P',dest='authpath',action='store',help='path to authentication file,optional')
    parser.add_argument('-i',dest='inputdir',action='store',required=False,help='output dir',default='.')
    parser.add_argument('-o',dest='outputdir',action='store',required=False,help='output dir',default='.')
    parser.add_argument('-f',dest='fillnum',action='store',required=False,help='specific full',default=None)
    parser.add_argument('-siteconfpath',dest='siteconfpath',action='store',help='specific path to site-local-config.xml file, optional. If path undefined, fallback to cern proxy&server')
    parser.add_argument('--debug',dest='debug',action='store_true',help='debug')
    parser.add_argument('--toscreen',dest='toscreen',action='store_true',help='dump to screen')
    options=parser.parse_args()
    if options.authpath:
        os.environ['CORAL_AUTH_PATH'] = options.authpath
    parameters = lumiQueryAPI.ParametersObject()
    session,svc =  lumiQueryAPI.setupSession (options.connect or \
                                              'frontier://LumiProd/CMS_LUMI_PROD',
                                               options.siteconfpath,parameters,options.debug)

    ##
    #query DB for all fills and compare with allfills.txt
    #if found newer fills, store  in mem fill number
    #reprocess anyway the last 5 fills in the dir
    #redo specific lumi for all marked fills
    ##
 
    allfillsFromFile=[]
    fillstoprocess=[]
    if os.path.exists(os.path.join(options.inputdir,allfillname)):
        allfillF=open(os.path.join(options.inputdir,allfillname),'r')
        for line in allfillF:
            allfillsFromFile.append(line)
        allfillF.close()
 
    session.transaction().start(True)
    q=session.nominalSchema().newQuery()    
    allfillsFromDB=lumiQueryAPI.allfills(q)
    del q
    if len(allfillsFromDB)==0:
        print 'no fill found in DB, exit'
        sys.exit(-1)
    allfillsFromDB.sort()
    if len(allfillsFromFile) != 0:
        allfillsFromFile.sort()
        if max(allfillsFromDB)>max(allfillsFromFile) : #need not to be one to one match because data can be deleted in DB
            print 'found new fill'
            for fill in allfillsFromDB:
                if fill>max(allfillsFromFile):
                    fillstoprocess.append(fill)
        if len(allfillsFromFile)>5: #reprocess anyway old fills
            fillstoprocess+=allfillsFromFile[-6:-1]
        elif len(allfillsFromFile)>2:
            fillstoprocess+=allfillsFromFile[-3:-1]
        else:
            fillstoprocess+=allfillsFromFile[-1]
    else:
        fillstoprocess=allfillsFromDB #process everything from scratch
    runsperfillFromDB={}
    session.transaction().start(True)
    q=session.nominalSchema().newQuery()
    runsperfillFromDB=lumiQueryAPI.runsByfillrange(q,int(min(fillstoprocess)),int(max(fillstoprocess)))
    del q
    runtimes={}
    runs=runsperfillFromDB.values()#list of lists
    allruns=[item for sublist in runs for item in sublist]
    allruns.sort()
    for run in allruns:
        q=session.nominalSchema().newQuery()
        runtimes[run]=lumiQueryAPI.runsummaryByrun(q,run)[3]
        del q
    #write specificlumi to outputdir
    session.transaction().commit()
    #update inputdir
    if len(fillstoprocess)!=0:
        filltofiles(allfillsFromDB,runsperfillFromDB,runtimes,options.inputdir)
    else:
        print 'nothing to do '
