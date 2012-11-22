import sys,os,os.path,glob,csv,math
def parseplotcache(filelist,fillmin,fillmax):
    result={}#{fill:{run:delivered}}
    tot=0
    for f in filelist:
        fileobj=open(f,'rb')
        plotreader=csv.reader(fileobj,delimiter=',')
        idx=0
        for row in plotreader:
            if idx!=0:
                [run,fill]=row[0].split(':')
                [lumils,cmsls]=row[1].split(':')
                if int(fill) not in range(fillmin,fillmax+1):
                    continue
                delivered=float(row[5])
                #if int(fill)==3292:
                #    tot+=1
                #    print run,lumils,cmsls,delivered
                if not result.has_key(int(fill)):
                    result[int(fill)]={}
                else:
                    if result[int(fill)].has_key(int(run)):
                        result[int(fill)][int(run)]+=delivered
                    else:
                        result[int(fill)][int(run)]=0.
            idx+=1    
        fileobj.close()
    #print 'tot ',tot
    return result
def findlpcdir(lpcdir,fillmin):
    result=[]
    cachedir=lpcdir
    lpcfilldir=[f for f in glob.glob(cachedir+'/????') if os.path.isdir(f) ]
    lpcfills=[os.path.split(f)[1] for f in lpcfilldir]
    #print lpcfills
    result=[int(f) for f in lpcfills if int(f)>=fillmin]
    return result

if __name__ == "__main__" :
    ofile=open('a.txt','w')
    delta=1000000.0 #1000/nb perrun
    lpcdir='/afs/cern.ch/cms/CAF/CMSCOMM/COMM_GLOBAL/LHCFILES/'
    #lpcdir='/afs/cern.ch/user/l/lumipro/scratch0/lumiprodev/head/CMSSW_5_0_1/src/RecoLuminosity/LumiDB/test/'
    plotcachedir='/afs/cern.ch/cms/lumi/www/publicplots/public_lumi_plots_cache/pp_all'
    plotfiles=[f for f in glob.glob(os.path.join(plotcachedir,'lumicalc_cache_2012*.csv')) if os.path.getsize(f)>0]
    fillmin=2400
    lpcfill2012=findlpcdir(lpcdir,fillmin)
    lpcfill2012.sort()
    lpcresult={}#{fill:[delivered]}

    plotfilldata={}#{fill:{run:delivered}}
    plotfilldata=parseplotcache(plotfiles,min(lpcfill2012),max(lpcfill2012))
    #print plotfilldata
    #for fill in [3292]:
    ofile.write('fills %s\n'%str(lpcfill2012))
    for fill in lpcfill2012:
        lpcfile=os.path.join(lpcdir,str(fill),str(fill)+'_summary_CMS.txt')
        if not os.path.exists(lpcfile):
            continue
        l=open(lpcfile,'rb')
        for line in l.readlines():
            line=line.strip()
            rundataline=line.split()
            if len(rundataline)!=4: continue
            lpcdelperrun=float(rundataline[3])
            lpcresult.setdefault(fill,[]).append(lpcdelperrun)
        l.close()
        if plotfilldata.has_key(fill) and lpcresult.has_key(fill):
            if len(plotfilldata[fill])!=len(lpcresult[fill]):
                ofile.write('====different n runs ====\n')
                ofile.write('fill,n runs_in_pplot,n runs_in_lpc\n')
                ofile.write('%d,%d,%d\n'%(fill,len(plotfilldata[fill]),len(lpcresult[fill])))
                runs=plotfilldata[fill].keys()
                runs.sort()
                ofile.write('runs_in_pplot lumi %s,runnum %s\n'%(str([plotfilldata[fill][l] for l in runs]),str(runs)))
                ofile.write('runs_in_lpc lumi %s\n'%(str([l for l in lpcresult[fill]])))
                ofile.write('=========================\n')
                ofile.write('\n')
            else:               
                runs=plotfilldata[fill].keys()
                runs.sort()
                lpcdelivered=lpcresult[fill]
                #print 'plotfilldata'
                #print plotfilldata[fill]
                for idx,run in enumerate(runs):
                    plotdelrun=plotfilldata[fill][run]
                    mydelta=plotdelrun-lpcdelivered[idx]
                    #print fill,runs[idx],plotdelrun,lpcdelivered[idx]
                    if math.fabs(mydelta)>delta:
                        ofile.write('****different lumi****\n')
                        ofile.write('fill,run,plotlumi,lpclumi\n')
                        ofile.write('%d,%d,%.4f,%.4f\n'%(fill,runs[idx],plotdelrun,lpcdelivered[idx]))
                        ofile.write('*************************\n')
                        ofile.write('\n')
        elif not plotfilldata.has_key(fill) and lpcresult.has_key(fill):
            ofile.write('====plot has no data====\n')
            ofile.write('fill : %d\n'%fill)
            ofile.write('=========================\n')
        elif not lpcresult.has_key(fill) and plotfilldata.has_key(fill) :
            ofile.write('====lpc has no data====\n')
            ofile.write('fill : %d\n',fill)
            ofile.write('=========================\n')
        else:
            pass

