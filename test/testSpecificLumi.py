import coral
import os,binascii,math
import array,datetime,time
from RecoLuminosity.LumiDB import lumiTime
from RecoLuminosity.LumiDB import inputFilesetParser,csvSelectionParser, selectionParser,csvReporter,argparse,CommonUtil,lumiQueryAPI
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

def calculateSpecificLumi(lumi,lumierr,beam1intensity,beam1intensityerr,beam2intensity,beam2intensityerr):
    '''
    '''
    specificlumi=0.0
    specificlumierr=0.0
    if lumi!=0.0 and beam1intensity!=0.0 and  beam2intensity!=0.0:
        specificlumi=float(lumi)/(float(beam1intensity)*float(beam2intensity))
        specificlumierr=specificlumi*math.sqrt(lumierr**2/lumi**2+beam1intensityerr**2/beam1intensity**2+beam2intensityerr**2/beam2intensity**2)
    return (specificlumi,specificlumierr)

def getFill(dbsession,c,fillnum):
    '''
    select RUNNUM , STARTTIME from CMSRUNSUMMARY where FILLNUM=1234
    output: result {runnum:starttime}
    '''
    result={}
    t=lumiTime.lumiTime()
    try:
        dbsession.transaction().start(True)
        schema=dbsession.schema(c.lumischema)
        if not schema:
            raise 'cannot connect to schema ',c.lumischema
        cmsrunsummaryOutput=coral.AttributeList()
        cmsrunsummaryOutput.extend('runnum','unsigned int')
        cmsrunsummaryOutput.extend('starttime','string')
        #cmsrunsummaryOutput.extend('starttime','time stamp')
        condition=coral.AttributeList()
        condition.extend('fillnum','unsigned int')
        condition['fillnum'].setData(int(fillnum))
        query=schema.newQuery()
        query.addToTableList(c.runsummaryname)
        query.addToOutputList('RUNNUM','runnum')
        query.addToOutputList('TO_CHAR(STARTTIME,\''+t.coraltimefm+'\')','starttime')#for oracle!!!
        #query.addToOutputList('STARTTIME','starttime')
        query.defineOutput(cmsrunsummaryOutput)
        query.setCondition('FILLNUM=:fillnum',condition)
        cursor=query.execute()
        while cursor.next():
            runnum=cursor.currentRow()['runnum'].data()
            starttime=cursor.currentRow()['starttime'].data()
            #pyt=t.StrToDatetime(starttime)
            #pyt=datetime.datetime(starttime.year(),starttime.month(),starttime.day(),starttime.hour(),starttime.minute(),starttime.second(),starttime.nanosecond()/1000,None)
            #print runnum
            result[runnum]=starttime
        #result=sorted(result,key=lambda x:x[0])
        
    except Exception,e:
        print str(e)
        dbsession.transaction().rollback()
        del dbsession
    #print result
    return result

def beamIntensityForRun(dbsession,c,runnum):
    '''
    select CMSBXINDEXBLOB,BEAMINTENSITYBLOB_1,BEAMINTENSITYBLOB_2 from LUMISUMMARY where runnum=146315 and LUMIVERSION='0001'
    
    output : result {startorbit: [(bxidx,beam1intensity,beam2intensity)]}
    '''
    result={} #{startorbit:[(bxidx,occlumi,occlumierr,beam1intensity,beam2intensity)]}
    try:
        dbsession.transaction().start(True)
        schema=dbsession.schema(c.lumischema)
        if not schema:
            raise 'cannot connect to schema ',c.lumischema
        lumisummaryOutput=coral.AttributeList()
        lumisummaryOutput.extend('cmslsnum','unsigned int')
        lumisummaryOutput.extend('startorbit','unsigned int')
        lumisummaryOutput.extend('bxindexblob','blob');
        lumisummaryOutput.extend('beamintensityblob1','blob');
        lumisummaryOutput.extend('beamintensityblob2','blob');
        condition=coral.AttributeList()
        condition.extend('runnum','unsigned int')
        condition.extend('lumiversion','string')
        condition['runnum'].setData(int(runnum))
        condition['lumiversion'].setData('0001')
        query=schema.newQuery()
        query.addToTableList(c.lumisummaryname)
        query.addToOutputList('CMSLSNUM','cmslsnum')
        query.addToOutputList('STARTORBIT','startorbit')
        query.addToOutputList('CMSBXINDEXBLOB','bxindexblob')
        query.addToOutputList('BEAMINTENSITYBLOB_1','beamintensityblob1')
        query.addToOutputList('BEAMINTENSITYBLOB_2','beamintensityblob2')
        query.setCondition('RUNNUM=:runnum AND LUMIVERSION=:lumiversion',condition)
        query.defineOutput(lumisummaryOutput)
        cursor=query.execute()
        while cursor.next():
            #cmslsnum=cursor.currentRow()['cmslsnum'].data()
            startorbit=cursor.currentRow()['startorbit'].data()
            if not cursor.currentRow()["bxindexblob"].isNull():
                bxindexblob=cursor.currentRow()['bxindexblob'].data()
                beamintensityblob1=cursor.currentRow()['beamintensityblob1'].data()
                beamintensityblob2=cursor.currentRow()['beamintensityblob2'].data()
                if bxindexblob.readline() is not None and beamintensityblob1.readline() is not None and beamintensityblob2.readline() is not None:
                    bxidx=array.array('h')
                    bxidx.fromstring(bxindexblob.readline())
                    bb1=array.array('f')
                    bb1.fromstring(beamintensityblob1.readline())
                    bb2=array.array('f')
                    bb2.fromstring(beamintensityblob2.readline())
                    for index,bxidxvalue in enumerate(bxidx):
                        if not result.has_key(startorbit):
                            result[startorbit]=[]
                        b1intensity=bb1[index]
                        b2intensity=bb2[index]
                        result[startorbit].append((bxidxvalue,b1intensity,b2intensity))
            else:
                print ' bxindexblob is null'
        del query
        dbsession.transaction().commit()
        #queryresult=sorted(queryresult,key=lambda x:x[0])
        #print 'queryresult ',queryresult
        #for perlsdata in queryresult:
            #print perlsdata[1]
            #startorbit=perlsdata[0] #startorbit
            #cmslsnum=perlsdata[1] #cmslsnum
            #bb1=perlsdata[3]#beam1 intensities
            #bb2=perlsdata[4]#beam2 intensities
            #print 'bb1 === ',bb1
            #for index,bxidxvalue in enumerate(perlsdata[2]):
                #print startorbit,bxidxvalue,bb1[index],bb2[index]
                #result.append((startorbit,bxidxvalue,bb1[index],bb2[index]))
        return result
    except Exception,e:
        print str(e)
        dbsession.transaction().rollback()
        del dbsession
            
def detailForRun(dbsession,c,runnum):
    '''select 
    s.cmslsnum,d.bxlumivalue,d.bxlumierror,d.bxlumiquality,d.algoname from LUMIDETAIL d,LUMISUMMARY s where s.runnum=133885 and d.algoname='OCC1' and s.lumisummary_id=d.lumisummary_id order by s.startorbit,s.cmslsnum
    result={startorbit:[(lumivalue,lumierr),]}
    '''
    result={}
    try:
        runnum=int(runnum)
        dbsession.transaction().start(True)
        schema=dbsession.schema(c.lumischema)
        if not schema:
            raise 'cannot connect to schema ',c.lumischema
        detailOutput=coral.AttributeList()
        detailOutput.extend('cmslsnum','unsigned int')
        detailOutput.extend('startorbit','unsigned int')
        detailOutput.extend('bxlumivalue','blob')
        detailOutput.extend('bxlumierror','blob')
        detailCondition=coral.AttributeList()
        detailCondition.extend('runnum','unsigned int')
        detailCondition.extend('algoname','string')
        detailCondition['runnum'].setData(runnum)
        detailCondition['algoname'].setData('OCC1')
        query=schema.newQuery()
        query.addToTableList(c.lumisummaryname,'s')
        query.addToTableList(c.lumidetailname,'d')
        query.addToOutputList('s.CMSLSNUM','cmslsnum')
        query.addToOutputList('s.STARTORBIT','startorbit')
        query.addToOutputList('d.BXLUMIVALUE','bxlumivalue')
        query.addToOutputList('d.BXLUMIERROR','bxlumierror')
        query.addToOutputList('d.BXLUMIQUALITY','bxlumiquality')
        query.setCondition('s.RUNNUM=:runnum and d.ALGONAME=:algoname and s.LUMISUMMARY_ID=d.LUMISUMMARY_ID',detailCondition)
        #query.addToOrderList('s.STARTORBIT')
        query.defineOutput(detailOutput)
        cursor=query.execute()
        count=0
        while cursor.next():
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            bxlumivalue=cursor.currentRow()['bxlumivalue'].data()
            bxlumierror=cursor.currentRow()['bxlumierror'].data()
            startorbit=cursor.currentRow()['startorbit'].data()

            bxlumivalueArray=array.array('f')
            bxlumivalueArray.fromstring(bxlumivalue.readline())
            bxlumierrorArray=array.array('f')
            bxlumierrorArray.fromstring(bxlumierror.readline())
            xingLum=[]
            #apply selection criteria
            maxlumi=max(bxlumivalueArray)*c.normfactor
            for index,lum in enumerate(bxlumivalueArray):
                lum *= c.normfactor
                lumierror = bxlumierrorArray[index]*c.normfactor
                #print index,lum,lumierror,c.normfactor
                if lum<max(c.xingMinLum,maxlumi*0.2): 
                    continue
                xingLum.append( (index,lum,lumierror) )
            if len(xingLum)!=0:
                result[(startorbit,cmslsnum)]=xingLum
        del query
        dbsession.transaction().commit()
        return result
    except Exception,e:
        print str(e)
        dbsession.transaction().rollback()
        del dbsession

def getSpecificLumi(dbsession,c,fillnum):
    '''
    specific lumi in 1e-30 (ub-1s-1) unit
    lumidetail occlumi in 1e-27
    1309_lumireg_401_CMS.txt
    ip fillnum time l(lumi in Hz/ub) dl(point-to-point error on lumi in Hz/ub) sl(specific lumi in Hz/ub) dsl(point-to-point error on specific lumi)
    5  1309 20800119.0 -0.889948 0.00475996848729 0.249009 0.005583287562 -0.68359 6.24140208607 0.0 0.0 0.0 0.0 0.0 0.0 0.0383576 0.00430892097862 0.0479095 0.00430892097862 66.6447 4.41269758764 0.0 0.0 0.0
    result [(time,lumi,lumierror,speclumi,speclumierror)]
    '''
    #result=[]
    runtimesInFill=getFill(dbsession,c,fillnum)#[(runnum,starttimestr)]
    #print runtimesInFill
    t=lumiTime.lumiTime()
    fillbypos={}#{bxidx:(lstime,lumi,lumierror,specificlumi,specificlumierror)}
    #'referencetime=time.mktime(datetime.datetime(2009,12,31,23,0,0).timetuple())
    #referencetime=time.mktime(datetime.datetime(2010,1,1,0,0,0).timetuple())
    referencetime=1262300400-7232
    #for i in range(3564):
    #    fillbypos[i]=[]
    for runnum,starttime in runtimesInFill.items():
        if not runtimesInFill.has_key(runnum):
            print 'run '+str(runnum)+' does not exist'
            continue
        occlumidata=detailForRun(dbsession,c,runnum)#{(startorbit,cmslsnum):[(bxidx,lumivalue,lumierr)]} #values after cut
        #print occlumidata
        beamintensitydata=beamIntensityForRun(dbsession,c,runnum)#{startorbit:[(bxidx,beam1intensity,beam2intensity)]}
        for (startorbit,cmslsnum),lumilist in occlumidata.items():
            if len(lumilist)==0: continue
            lstimestamp=t.OrbitToTimestamp(starttime,startorbit)
            if beamintensitydata.has_key(startorbit) and len(beamintensitydata[startorbit])>0:
                for lumidata in lumilist:
                    bxidx=lumidata[0]
                    lumi=lumidata[1]
                    lumierror=lumidata[2]
                    for beamintensitybx in beamintensitydata[startorbit]:
                        if beamintensitybx[0]==bxidx:
                            if not fillbypos.has_key(bxidx):
                                fillbypos[bxidx]=[]
                            beam1intensity=beamintensitybx[1]
                            beam2intensity=beamintensitybx[2]
                            speclumi=calculateSpecificLumi(lumi,lumierror,beam1intensity,0.0,beam2intensity,0.0)
                            fillbypos[bxidx].append([lstimestamp-referencetime,lumi,lumierror,beam1intensity,beam2intensity,speclumi[0],speclumi[1]])
    return fillbypos

def toscreen(fillnum,filldata):
    ipnumber=5
    print 'fill ',fillnum
    for cmsbxidx,perbxdata in filldata.items():
        lhcbucket=0
        if cmsbxidx!=0:
            lhcbucket=(cmsbxidx-1)*10+1
        a=sorted(perbxdata,key=lambda x:x[0])
        for perlsdata in a:
            if perlsdata[-2]!=0 and perlsdata[-1]!=0 and perlsdata[1]!=0:
                print '%d\t%d\t%d\t%e\t%e\t%e\t%e'%(int(ipnumber),lhcbucket,int(perlsdata[0]),perlsdata[1],perlsdata[2],perlsdata[-2],perlsdata[-1])
                
def tofile(fillnum,filldata,outdir):
    timedict={}#{lstime:[[lumi,lumierr,speclumi,speclumierr]]}

    ipnumber=5
    for cmsbxidx,perbxdata in filldata.items():
        lhcbucket=0
        if cmsbxidx!=0:
            lhcbucket=(cmsbxidx-1)*10+1
        a=sorted(perbxdata,key=lambda x:x[0])
        lscounter=0
        filename=str(fillnum)+'_lumi_'+str(lhcbucket)+'_CMS.txt'
        for perlsdata in a:
            if perlsdata[-2]>0 and perlsdata[-1]>0 and perlsdata[1]>0:
                if lscounter==0:
                    f=open(os.path.join(outdir,filename),'w')
                print >>f, '%d\t%d\t%d\t%e\t%e\t%e\t%e\n'%(int(ipnumber),int(fillnum),int(perlsdata[0]),perlsdata[1],perlsdata[2],perlsdata[-2],perlsdata[-1])
                if not timedict.has_key(int(perlsdata[0])):
                    timedict[int(perlsdata[0])]=[]
                timedict[int(perlsdata[0])].append([perlsdata[1],perlsdata[2],perlsdata[-2],perlsdata[-1]])
                lscounter+=1
        f.close()
        summaryfilename=str(fillnum)+'_lumi_CMS.txt'
        f=open(os.path.join(outdir,summaryfilename),'w')
        lstimes=timedict.keys()
        lstimes.sort()
        for lstime in lstimes:
            allvalues=timedict[lstime]
            transposedvalues=CommonUtil.transposed(allvalues,0.0)
            lumivals=transposedvalues[0]
            lumitot=sum(lumivals)
            lumierrs=transposedvalues[1]
            lumierrortot=math.sqrt(sum(map(lambda x:x**2,lumierrs)))
            specificvals=transposedvalues[2]
            specificavg=sum(specificvals)/float(len(specificvals))#avg spec lumi
            specificerrs=transposedvalues[3]
            specifictoterr=math.sqrt(sum(map(lambda x:x**2,specificerrs)))
            specificerravg=specifictoterr/float(len(specificvals))
            print >>f,'%d\t%d\t%d\t%e\t%e\t%e\t%e\n'%(int(ipnumber),int(fillnum),lstime,lumitot,lumierrortot,specificavg,specificerravg)
        f.close()
def main():
    c=constants()
    os.environ['CORAL_AUTH_PATH']='/afs/cern.ch/user/x/xiezhen'
    svc=coral.ConnectionService()
    session=svc.connect(c.lumidb,accessMode=coral.access_ReadOnly)
    session.typeConverter().setCppTypeForSqlType("unsigned int","NUMBER(10)")
    session.typeConverter().setCppTypeForSqlType("unsigned long long","NUMBER(20)")
    msg=coral.MessageStream('')
    msg.setMsgVerbosity(coral.message_Level_Error)
    outdir='testspecout'
    fillnum=1369
    filldata=getSpecificLumi(session,c,fillnum)
    
    #toscreen(fillnum,filldata)
    tofile(fillnum,filldata,outdir)
if __name__=='__main__':
    main()
