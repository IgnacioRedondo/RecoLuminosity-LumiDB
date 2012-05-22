import os,coral,datetime,fnmatch,time
from RecoLuminosity.LumiDB import nameDealer,revisionDML,dataDML,lumiTime,CommonUtil,selectionParser,hltTrgSeedMapper,normFunctors,lumiParameters

########################################################################
# Lumi data management and calculation API                             #
#                                                                      #
# Author:      Zhen Xie                                                #
########################################################################

#
# Corrections/Norms  API
#
#def normByContext(schema,runcontextMap,lumitype='HF'):
#    '''
#    best norm in context
#    input: {run:(amodetag,egev)}
#    output: (normName,{minrun:[normid,formname,occ1norm,occ2norm,etnorm,punorm,drift,a1,a2,a3,a4,a5,c1,c2]}
#    '''
#    result={}
#    tmpresult={}#{(amodetag,egev):normdataid}
#    tmpmap={}
#    normmap={}#{normdataid:normvalues}
#    
#    for run in sorted(runcontextMap):
#        context=runcontextMap[run]
#        mymodetag=context[0]
#        myegev=context[1]
#        if not tmpresult.has_key(mymodetag,myegev):#loop over context
#            []=dataDML.normsByContext(schema,mymodetag,myegev)
#            
#            tmpresult[context]=normdataid
#        tmpmap[run]=tmpresult[context]
        
def normByName(schema,normname,runlist,lumitype='HF'):
    '''
    input:
        runlist [run]
        normname
    output: {minrun:[normid,formname,occ1norm,occ2norm,etnorm,punorm,drift,a1,a2,a3,a4,a5,c1,c2]}
    '''
    pass

def normForRange(schema,runcontextMap):
    '''
    decide from context
    input {run:(amodetag,egev) }
    output: {run:(normval,occ2norm,etnorm,punorm,constfactor)}    
    '''
    result={}
    tmpresult={}#{(amodetag,egev):normdataid}
    tmpmap={}
    normmap={}#{normdataid:normvalues}
    for r,context in runcontextMap.items():
        mymodetag=context[0]
        myegev=context[1]
        if not tmpresult.has_key(context):
            tmpresult[context]=None
            normdataid=dataDML.guessnormIdByContext(schema,mymodetag,myegev)
            tmpresult[context]=normdataid
        tmpmap[r]=tmpresult[context]
    for myid in tmpmap.values():
        normmap[myid]=dataDML.luminormById(schema,normdataid)
    for r,myid in tmpmap.items():
        result[r]=normmap[myid]
    return result

def normByName(schema,norm):
    '''
    output: (normname(0),amodetag(1),egev(2),norm(3),occ2norm(4),etnorm(5),punorm(6),constfactor(7))
    '''
    if isinstance(norm,int) or isinstance(norm,float) or CommonUtil.is_floatstr(norm) or CommonUtil.is_intstr(norm):
        return (None,None,None,float(norm),1.0,1.0,1.0,1.0)
    if not isinstance(norm,str):
        raise ValueError('wrong parameter type')
    normdataid=dataDML.guessnormIdByName(schema,norm)
    if not normdataid:
        raise  ValueError('unknown norm '+norm)
    normresult=dataDML.luminormById(schema,normdataid)
    return normresult

def correctionByName(schema,tagname=None):
    '''
    output:{tagname:(data_id(0),a1(1),a2(2),driftcoeff(3)}
    '''
    correctiondataid=dataDML.guesscorrIdByName(schema,tagname)
    if not correctiondataid:
        raise  ValueError('unknown correction '+tagname)
    correctionresult=dataDML.lumicorrById(schema,correctiondataid)
    return correctionresult

def driftCorrectionForRange(schema,inputRange,driftcoeff):
    '''
    select intglumi from intglumi where runnum=:runnum and startrun=:startrun
    input : inputRange. str if a single run, [runs] if a list of runs
            driftcoeff float
    output: {run:driftcorrection} driftcorrection=intglumi*driftcoeff
    '''
    result={}
    runs=[]
    if isinstance(inputRange,str):
        runs.append(int(inputRange))
    else:
        runs=inputRange
    if not runs: return result    
    for r in runs:
        defaultresult=1.0
        intglumi=0.0
        lint=0.0
        if r<150008 :# no drift corrections for 2010 data
            result[r]=defaultresult
            continue
        if r>189738: # no drift correction for 2012 data
            result[r]=defaultresult
            continue
        qHandle=schema.newQuery()
        try:
            qHandle.addToTableList(nameDealer.intglumiTableName())
            qResult=coral.AttributeList()
            qResult.extend('INTGLUMI','float')
            qHandle.addToOutputList('INTGLUMI')
            qConditionStr='RUNNUM=:runnum AND STARTRUN<=:startrun'
            qCondition=coral.AttributeList()
            qCondition.extend('runnum','unsigned int')
            qCondition.extend('startrun','unsigned int')
            qCondition['runnum'].setData(int(r))
            qCondition['startrun'].setData(int(r))
            qHandle.setCondition(qConditionStr,qCondition)
            qHandle.defineOutput(qResult)
            cursor=qHandle.execute()
            while cursor.next():
                intglumi=cursor.currentRow()['INTGLUMI'].data()
            lint=intglumi*6.37*1.0e-9*driftcoeff #(convert to /fb)
            #print lint
        except :
            del qHandle
            raise
        del qHandle
        if not lint:
            print '[WARNING] null intglumi for run ',r,' '
        result[r]=defaultresult+driftcoeff*lint
    return result

def runsummary(schema,irunlsdict):
    '''
    output  [[run(0),l1key(1),amodetag(2),egev(3),hltkey(4),fillnum(5),fillscheme(6),starttime(7),stoptime(8)]]
    '''
    result=[]
    for run in sorted(irunlsdict):
        runinfo=dataDML.runsummary(schema,run)
        runinfo.insert(0,run)
        result.append(runinfo)
    return result

def runsummaryMap(schema,irunlsdict):
    '''
    output  {run:[l1key(0),amodetag(1),egev(2),hltkey(3),fillnum(4),fillscheme(5),starttime(6),stoptime(7)]}
    '''
    result={}
    seqresult=runsummary(schema,irunlsdict)
    for [run,l1key,amodetag,egev,hltkey,fillnum,fillscheme,starttime,stoptime] in seqresult:
        result[run]=[l1key,amodetag,egev,hltkey,fillnum,fillscheme,starttime,stoptime]
    return result

def fillInRange(schema,fillmin=1000,fillmax=9999,amodetag='PROTPHYS',startT=None,stopT=None):
    '''
    output [fill]
    '''
    fills=dataDML.fillInRange(schema,fillmin,fillmax,amodetag,startT,stopT)
    return fills
def fillrunMap(schema,fillnum=None,runmin=None,runmax=None,startT=None,stopT=None,l1keyPattern=None,hltkeyPattern=None,amodetag=None):
    '''
    output: {fill:[runnum,...]}
    '''
    return dataDML.fillrunMap(schema,fillnum=fillnum,runmin=runmin,runmax=runmax,startT=startT,stopT=stopT,l1keyPattern=l1keyPattern,hltkeyPattern=hltkeyPattern,amodetag=amodetag)
             
def runList(schema,fillnum=None,runmin=None,runmax=None,startT=None,stopT=None,l1keyPattern=None,hltkeyPattern=None,amodetag=None,nominalEnergy=None,energyFlut=0.2,requiretrg=True,requirehlt=True,lumitype='HF'):
    '''
    output: [runnumber,...]
    '''
    return dataDML.runList(schema,fillnum,runmin,runmax,startT,stopT,l1keyPattern,hltkeyPattern,amodetag,nominalEnergy,energyFlut,requiretrg,requirehlt,lumitype)

def hltpathsForRange(schema,runlist,hltpathname=None,hltpathpattern=None):
    '''
    input: runlist [run],     (required)      
           datatag: data version (optional)
    output : {runnumber,[(hltpath,l1seedexpr,l1bitname)...]}
    '''
    result={}
    for run in runlist:
        hlttrgmap=dataDML.hlttrgMappingByrun(schema,run,hltpathname=hltpathname,hltpathpattern=hltpathpattern)
        result[run]=[]
        for hltpath in sorted(hlttrgmap):
            l1seedexpr=hlttrgmap[hltpath]
            l1bitname=hltTrgSeedMapper.findUniqueSeed(hltpath,l1seedexpr)
            result[run].append((hltpath,l1seedexpr,l1bitname))
    return result

def trgbitsForRange(schema,runlist,datatag=None):
    '''
    input: runlist [run],(required)
           datatag: data version (optional)
    output: {runnumber:[datasource,normbit,[bitname,..]]}
    '''
    result={}
    for run in runlist:
        trgdataid=dataDML.guessTrgDataIdByRun(schema,run)
        if not trgdataid :
            result[run]=None
            continue
        if not result.has_key(run):
            result[run]=[]
        trgconf=dataDML.trgRunById(schema,trgdataid)
        datasource=trgconf[1]
        bitzeroname=trgconf[2]
        bitnamedict=trgconf[3]
        bitnames=[x[1] for x in bitnamedict if x[1]!='False']
        result[run].extend([datasource,bitzeroname,bitnames])
    return result

def beamForRange(schema,inputRange,withBeamIntensity=False,minIntensity=0.1,tableName=None,branchName=None):
    '''
    input:
           inputRange: {run:[cmsls]} (required)
    output : {runnumber:[(lumicmslnum,cmslsnum,beamenergy,beamstatus,[(ibx,b1,b2)])...](4)}
    '''
    if tableName is None:
        tableName=nameDealer.lumidataTableName()
    if branchName is None:
        branchName='DATA'
    result={}
    for run in inputRange.keys():
        lslist=inputRange[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue
        lumidataid=dataDML.guessLumiDataIdByRun(schema,run,tableName)
        if lumidataid is None:
            result[run]=None
            continue #run non exist
        lumidata=dataDML.beamInfoById(schema,lumidataid,withBeamIntensity=withBeamIntensity,minIntensity=minIntensity)
        #(runnum,[(lumilsnum(0),cmslsnum(1),beamstatus(2),beamenergy(3),beaminfolist(4)),..])
        result[run]=[]
        perrundata=lumidata[1]
        if not perrundata:
            result[run]=[]
            continue
        for perlsdata in perrundata:
            lumilsnum=perlsdata[0]
            cmslsnum=perlsdata[1]
            if lslist is not None and cmslsnum not in lslist:
                continue
            beamstatus=perlsdata[2]
            beamenergy=perlsdata[3]
            beamintInfolist=[]
            if withBeamIntensity:
                beamintInfolist=perlsdata[4]
            result[run].append((lumilsnum,cmslsnum,beamstatus,beamenergy,beamintInfolist))        
    return result

def hltForRange(schema,inputRange,hltpathname=None,hltpathpattern=None,withL1Pass=False,withHLTAccept=False,tableName=None,branchName=None):
    '''
    input:
           inputRange: {run:[cmsls]} (required)
           hltpathname: exact match hltpathname  (optional) 
           hltpathpattern: regex match hltpathpattern (optional)
           branchName : data version
    output: {runnumber:[(cmslsnum,[(hltpath,hltprescale,l1pass,hltaccept),...]),(cmslsnum,[])})}
    '''
    #if tableName is None:
    #    tableName=nameDealer.hltdataTableName()
    #if branchName is None:
    #    branchName='DATA'
    result={}
    for run in inputRange.keys():
        lslist=inputRange[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue
        hltdataid=dataDML.guessHltDataIdByRun(schema,run)
        if hltdataid is None:
            result[run]=None
            continue #run non exist
        hltdata=dataDML.hltLSById(schema,hltdataid,hltpathname=hltpathname,hltpathpattern=hltpathpattern,withL1Pass=withL1Pass,withHLTAccept=withHLTAccept)
        #(runnum,{cmslsnum:[(pathname,prescale,l1pass,hltaccept),...]})
        result[run]=[]
        if hltdata and hltdata[1]:
            for cmslsnum in sorted(hltdata[1]):
                if lslist is not None and cmslsnum not in lslist:
                    continue
                lsdata=[]
                for perpathdata in hltdata[1][cmslsnum]:
                    pathname=perpathdata[0]
                    prescale=perpathdata[1]
                    l1pass=None
                    hltaccept=None
                    if withL1Pass:
                        l1pass=perpathdata[2]
                    if withHLTAccept:
                        hltaccept=perpathdata[3]
                    lsdata.append((pathname,prescale,l1pass,hltaccept))
                result[run].append((cmslsnum,lsdata))
    return result

def hltForIds(schema,irunlsdict,dataidmap,hltpathname=None,hltpathpattern=None,withL1Pass=False,withHLTAccept=False,datatag=None):
    '''
    input:
           irunlsdict: {run:[cmsls]} (required)
           dataidmap: {run:(lumiid,trgid,hltid)}
           hltpathname: exact match hltpathname  (optional) 
           hltpathpattern: regex match hltpathpattern (optional)
           withL1Pass: with L1 pass count
           withHLTAccept: with HLT accept
           tablename: 
           branchName : data version
    output: {runnumber:[(cmslsnum,[(hltpath,hltprescale,l1pass,hltaccept),...]),(cmslsnum,[])})}
    '''
    result={}
    for run in irunlsdict.keys():
        lslist=irunlsdict[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue
        hltdataid=dataidmap[run][2]
        if hltdataid is None:
            result[run]=None
            continue #run non exist
        hltdata=dataDML.hltLSById(schema,hltdataid,hltpathname=hltpathname,hltpathpattern=hltpathpattern,withL1Pass=withL1Pass,withHLTAccept=withHLTAccept)
        #(runnum,{cmslsnum:[(pathname,prescale,l1pass,hltaccept),...]})
        result[run]=[]            
        if hltdata and hltdata[1]:
            lsdict={}            
            for cmslsnum in sorted(hltdata[1]):
                if lslist is not None and cmslsnum not in lslist:
                    continue
                lsdata=[]
                for perpathdata in hltdata[1][cmslsnum]:
                    pathname=perpathdata[0]
                    prescale=perpathdata[1]
                    l1pass=None
                    hltaccept=None
                    if withL1Pass:
                        l1pass=perpathdata[2]
                    if withHLTAccept:
                        hltaccept=perpathdata[3]
                    lsdata.append((pathname,prescale,l1pass,hltaccept))
                result[run].append((cmslsnum,lsdata))
    return result

def trgForRange(schema,inputRange,trgbitname=None,trgbitnamepattern=None,withL1Count=False,withPrescale=False,tableName=None,branchName=None):
    '''
    input :
            inputRange  {run:[cmsls]} (required)
            trgbitname exact match  trgbitname (optional)
            trgbitnamepattern match trgbitname (optional)
            tableName : trgdata table name
            branchName : data version
    output
            result {run:[cmslsnum,deadfrac,deadtimecount,bitzero_count,bitzero_prescale,[(bitname,prescale,counts)]]}
    '''
    #if tableName is None:
    #    tableName=nameDealer.trgdataTableName()
    #if branchName is None:
    #    branchName='DATA'
    result={}
    withprescaleblob=True
    withtrgblob=True    
    for run in inputRange.keys():
        lslist=inputRange[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue
        trgdataid=dataDML.guessTrgDataIdByRunInBranch(schema,run)
        if trgdataid is None:
            result[run]=None
            continue #run non exist
        trgdata=dataDML.trgLSById(schema,trgdataid,trgbitname=trgbitname,trgbitnamepattern=trgbitnamepattern,withL1Count=withL1Count,withPrescale=withPrescale)
        #(runnum,{cmslsnum:[deadtimecount(0),bitzerocount(1),bitzeroprescale(2),deadfrac(3),[(bitname,trgcount,prescale)](4)]})
        result[run]=[]
        if trgdata and trgdata[1]:
            lsdict={}
            for cmslsnum in sorted(trgdata[1]):
                if lslist is not None and cmslsnum not in lslist:
                    continue
                lsdata=[]
                deadtimecount=trgdata[1][cmslsnum][0]
                bitzerocount=trgdata[1][cmslsnum][1]
                bitzeroprescale=trgdata[1][cmslsnum][2]
                #if float(bitzerocount)*float(bitzeroprescale)==0.0:
                #    deadfrac=1.0
                #else:
                #    deadfrac=float(deadtimecount)/(float(bitzerocount)*float(bitzeroprescale))
                deadfrac=trgdata[1][cmslsnum][3]
                if deadfrac<0 or deadfrac>1.0:
                    deadfrac=1.0
                allbitsinfo=trgdata[1][cmslsnum][4]
                lsdata.append(cmslsnum)
                lsdata.append(deadfrac)
                lsdata.append(deadtimecount)
                lsdata.append(bitzerocount)
                lsdata.append(bitzeroprescale)
                lsdata.append(allbitsinfo)
                result[run].append(lsdata)
    return result

def trgForIds(schema,irunlsdict,dataidmap,trgbitname=None,trgbitnamepattern=None,withL1Count=False,withPrescale=False,tableName=None,branchName=None):
    '''
    input :
            irunlsdict  {run:[cmsls]} (required)
            dataidmap: {run:(lumiid,trgid,hltid)}
            trgbitname exact match  trgbitname (optional)
            trgbitnamepattern match trgbitname (optional)
            tableName : trgdata table name
            branchName : data version
    output
            result {run:[cmslsnum,deadfrac,deadtimecount,bitzero_count,bitzero_prescale,[(bitname,prescale,counts)]]}
    '''
    result={}
    #withprescaleblob=True
    #withtrgblob=True    
    for run in irunlsdict.keys():
        lslist=irunlsdict[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue
        trgdataid=dataidmap[run][1]
        if trgdataid is None:
            result[run]=None
            continue #run non exist
        trgdata=dataDML.trgLSById(schema,trgdataid,trgbitname=trgbitname,trgbitnamepattern=trgbitnamepattern,withL1Count=withL1Count,withPrescale=withPrescale)
        #(runnum,{cmslsnum:[deadtimecount(0),bitzerocount(1),bitzeroprescale(2),deadfrac(3),[(bitname,trgcount,prescale)](4)]})
        result[run]=[]
        if trgdata and trgdata[1]:
            lsdict={}
            for cmslsnum in sorted(trgdata[1]):
                if lslist is not None and cmslsnum not in lslist:
                    continue
                lsdata=[]
                #print trgdata[1][cmslsnum]
                deadtimecount=trgdata[1][cmslsnum][0]
                bitzerocount=trgdata[1][cmslsnum][1]
                bitzeroprescale=trgdata[1][cmslsnum][2]               
                deadfrac=trgdata[1][cmslsnum][3]
                if deadfrac<0 or deadfrac>1.0:
                    deadfrac=1.0
                allbitsinfo=trgdata[1][cmslsnum][4]
                lsdata.append(cmslsnum)
                lsdata.append(deadfrac)
                lsdata.append(deadtimecount)
                lsdata.append(bitzerocount)
                lsdata.append(bitzeroprescale)
                lsdata.append(allbitsinfo)
                result[run].append(lsdata)
    return result

def instLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap,beamstatusfilter=None,withBXInfo=False,bxAlgo=None,xingMinLum=0.0,withBeamIntensity=False,lumitype='HF'):
    '''
    FROM ROOT FILE NO CORRECTION AT ALL 
    input:
           irunlsdict: {run:[cmsls]} 
           dataidmap: {run:(lumiid,trgid,hltid)}
           runsummaryMap: {run:[l1key(0),amodetag(1),egev(2),hltkey(3),fillnum(4),fillscheme(5),starttime(6),stoptime(7)]}
           beamstatus: LS filter on beamstatus (optional)
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           lumitype: luminosity measurement source
    output:
           instlumi unit in Hz/mb
           result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),instlumi(5),instlumierr(6),startorbit(7),numorbit(8),(bxidx,bxvalues,bxerrs)(9),(bxidx,b1intensities,b2intensities)(10),fillnum(11)]}}
           lumi unit: HZ/ub
    '''
    if lumitype not in ['HF','PIXEL']:
        raise ValueError('unknown lumitype '+lumitype)
    lumitableName=''
    lumilstableName=''
    if lumitype=='HF':
        lumitableName=nameDealer.lumidataTableName()
        lumilstableName=nameDealer.lumisummaryv2TableName()
    else:
        lumitableName=nameDealer.pixellumidataTableName()
        lumilstableName=nameDealer.pixellumisummaryv2TableName()
    result={}
    for run,(lumidataid,trgid,hltid ) in dataidmap.items():
        lslist=irunlsdict[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue

        fillnum=runsummaryMap[run][4]
        runstarttimeStr=runsummaryMap[run][6]
        if lumidataid is None: #if run not found in lumidata
            result[run]=None
            continue
        (lumirunnum,perlsresult)=dataDML.lumiLSById(schema,lumidataid,beamstatus=beamstatusfilter,withBXInfo=withBXInfo,bxAlgo=bxAlgo,withBeamIntensity=withBeamIntensity,tableName=lumilstableName)
        lsresult=[]
        c=lumiTime.lumiTime()
        for lumilsnum in perlsresult.keys():
            perlsdata=perlsresult[lumilsnum]
            cmslsnum=perlsdata[0]
            if lslist is not None and lumilsnum not in lslist:
                cmslsnum=0
            numorbit=perlsdata[6]
            startorbit=perlsdata[7]
            orbittime=c.OrbitToTime(runstarttimeStr,startorbit,0)
            instlumi=perlsdata[1]
            instlumierr=perlsdata[2]
            beamstatus=perlsdata[4]
            beamenergy=perlsdata[5]
            bxidxlist=[]
            bxvaluelist=[]
            bxerrorlist=[]
            bxdata=None
            beamdata=None
            if withBXInfo:
                bxinfo=perlsdata[8]                
                bxvalueArray=None
                bxerrArray=None
                if bxinfo:
                    bxvalueArray=bxinfo[0]
                    bxerrArray=bxinfo[1]
                    for idx,bxval in enumerate(bxvalueArray):
                        if bxval>xingMinLum:
                            bxidxlist.append(idx)
                            bxvaluelist.append(bxval)
                            bxerrorlist.append(bxerrArray[idx])
                    del bxvalueArray[:]
                    del bxerrArray[:]
                bxdata=(bxidxlist,bxvaluelist,bxerrorlist)
            if withBeamIntensity:
                beaminfo=perlsdata[9]
                bxindexlist=[]
                b1intensitylist=[]
                b2intensitylist=[]
                if beaminfo[0] and beaminfo[1] and beaminfo[2]:
                    bxindexarray=beaminfo[0]
                    beam1intensityarray=beaminfo[1]
                    beam2intensityarray=beaminfo[2]                    
                    bxindexlist=bxindexarray.tolist()
                    b1intensitylist=beam1intensityarray.tolist()
                    b2intensitylist=beam2intensityarray.tolist()
                    del bxindexarray[:]
                    del beam1intensityarray[:]
                    del beam2intensityarray[:]                    
                beamdata=(bxindexlist,b1intensitylist,b2intensitylist)
            lsresult.append([lumilsnum,cmslsnum,orbittime,beamstatus,beamenergy,instlumi,instlumierr,startorbit,numorbit,bxdata,beamdata,fillnum])         
            del perlsdata[:]
        result[run]=lsresult
    return result

def instLumiForRange(schema,inputRange,lumirundataMap,beamstatusfilter=None,withBXInfo=False,bxAlgo=None,xingMinLum=0.0,withBeamIntensity=False,lumitype='HF',branchName=None):
    '''
    DIRECTLY FROM ROOT FIME NO CORRECTION AT ALL 
    lumi raw data. beofore normalization and time integral
    input:
           inputRange  {run:[cmsls]} (required)
           beamstatus: LS filter on beamstatus (optional)
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           branchName: data version
    output:
           result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),instlumi(5),instlumierr(6),startorbit(7),numorbit(8),(bxidx,bxvalues,bxerrs)(9),(bxidx,b1intensities,b2intensities)(10),fillnum(11),nbx(12)]}}
           lumi unit: HZ/ub
    '''
    if lumitype not in ['HF','PIXEL']:
        raise ValueError('unknown lumitype '+lumitype)
    lumitableName=''
    lumilstableName=''
    if lumitype=='HF':
        lumitableName=nameDealer.lumidataTableName()
        lumilstableName=nameDealer.lumisummaryv2TableName()
    else:
        lumitableName=nameDealer.pixellumidataTableName()
        lumilstableName=nameDealer.pixellumisummaryv2TableName()
        
    result={}
    for run in inputRange.keys():
        lslist=inputRange[run]
        if lslist is not None and len(lslist)==0:
            result[run]=[]#if no LS is selected for a run
            continue
        runsummary=dataDML.runsummary(schema,run)
        if len(runsummary)==0:#if run not found in runsummary
            result[run]=None
            continue
        fillnum=runsummary[4]
        runstarttimeStr=runsummary[6]
        lumidataid=dataDML.guessLumiDataIdByRun(schema,run,lumitableName)
        if lumidataid is None: #if run not found in lumidata
            result[run]=None
            continue
        (lumirunnum,perlsresult)=dataDML.lumiLSById(schema,lumidataid,beamstatusfilter,withBXInfo=withBXInfo,bxAlgo=bxAlgo,withBeamIntensity=withBeamIntensity,tableName=lumilstableName)
        lsresult=[]
        c=lumiTime.lumiTime()
        for lumilsnum in perlsresult.keys():
            perlsdata=perlsresult[lumilsnum]
            cmslsnum=perlsdata[0]
            if lslist is not None and lumilsnum not in lslist:
                cmslsnum=0
            numorbit=perlsdata[6]
            startorbit=perlsdata[7]
            orbittime=c.OrbitToTime(runstarttimeStr,startorbit,0)
            instlumi=perlsdata[1]
            instlumierr=perlsdata[2]
            beamstatus=perlsdata[4]
            beamenergy=perlsdata[5]
            bxidxlist=[]
            bxvaluelist=[]
            bxerrorlist=[]
            bxdata=None
            beamdata=None
            if withBXInfo:
                bxinfo=perlsdata[8]                
                bxvalueArray=None
                bxerrArray=None
                if bxinfo:
                    bxvalueArray=bxinfo[0]
                    bxerrArray=bxinfo[1]
                    for idx,bxval in enumerate(bxvalueArray):
                        if bxval>xingMinLum:
                            bxidxlist.append(idx)
                            bxvaluelist.append(bxval)
                            bxerrorlist.append(bxerrArray[idx])
                    del bxvalueArray[:]
                    del bxerrArray[:]
                bxdata=(bxidxlist,bxvaluelist,bxerrorlist)
            if withBeamIntensity:
                beaminfo=perlsdata[9]
                bxindexlist=[]
                b1intensitylist=[]
                b2intensitylist=[]
                if beaminfo[0] and beaminfo[1] and beaminfo[2]:
                    bxindexarray=beaminfo[0]
                    beam1intensityarray=beaminfo[1]
                    beam2intensityarray=beaminfo[2]                    
                    bxindexlist=bxindexarray.tolist()
                    b1intensitylist=beam1intensityarray.tolist()
                    b2intensitylist=beam2intensityarray.tolist()
                    del bxindexarray[:]
                    del beam1intensityarray[:]
                    del beam2intensityarray[:]                    
                beamdata=(bxindexlist,b1intensitylist,b2intensitylist)
            lsresult.append([lumilsnum,cmslsnum,orbittime,beamstatus,beamenergy,instlumi,instlumierr,startorbit,numorbit,bxdata,beamdata,fillnum])         
            del perlsdata[:]
        result[run]=lsresult
    return result

def instCalibratedLumiForRange(schema,inputRange,beamstatus=None,amodetag=None,egev=None,withBXInfo=False,bxAlgo=None,xingMinLum=0.0,withBeamIntensity=False,norm=None,finecorrections=None,driftcorrections=None,usecorrectionv2=False,lumitype='HF',branchName=None):
    '''
    Inst luminosity after calibration, not time integrated
    input:
           inputRange  {run:[cmsls]} (required)
           amodetag : accelerator mode for all the runs (optional) ['PROTPHYS','IONPHYS']
           beamstatus: LS filter on beamstatus (optional)
           amodetag: amodetag for  picking norm(optional)
           egev: beamenergy for picking norm(optional)
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           norm: if norm is a float, use it directly; if it is a string, consider it norm factor name to use (optional)
           lumitype : HF or PIXEL
           branchName: data version
           finecorrections: const and non-linear corrections
           driftcorrections: driftcorrections
    output:
           result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),calibratedlumi(5),calibratedlumierr(6),startorbit(7),numorbit(8),(bxidx,bxvalues,bxerrs)(9),(bxidx,b1intensities,b2intensities)(10),fillnum(11)]}}
           lumi unit: HZ/ub
    '''
    result = {}
    normval=None
    perbunchnormval=None
    if norm:
        normval=_getnorm(schema,norm)
        perbunchnormval=float(normval)/float(1000)
    elif amodetag and egev:
        normval=_decidenormFromContex(schema,amodetag,egev)
        perbunchnormval=float(normval)/float(1000)
    instresult=instLumiForRange(schema,inputRange,beamstatusfilter=beamstatus,withBXInfo=withBXInfo,bxAlgo=bxAlgo,xingMinLum=xingMinLum,withBeamIntensity=withBeamIntensity,lumitype=lumitype,branchName=branchName)
    for run,perrundata in instresult.items():
        if perrundata is None:
            result[run]=None
            continue
        result[run]=[]
        if not normval:#if norm cannot be decided , look for it according to context per run
            normval=_decidenormForRun(schema,run)
            perbunchnormval=float(normval)/float(1000)
        if not normval:#still not found? resort to global default (should never come here)
            normval=6370
            perbunchnormval=6.37
            print '[Warning] using default normalization '+str(normval)
        for perlsdata in perrundata:
            lumilsnum=perlsdata[0]
            cmslsnum=perlsdata[1]
            timestamp=perlsdata[2]
            bs=perlsdata[3]
            beamenergy=perlsdata[4]
            fillnum=perlsdata[11]
            avglumi=perlsdata[5]*normval
            calibratedlumi=avglumi
            bxdata=perlsdata[9]
            if lumitype=='HF' and finecorrections and finecorrections[run]:
                if usecorrectionv2:
                    if driftcorrections and driftcorrections[run]:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                    else:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                else:
                    calibratedlumi=lumiCorrections.applyfinecorrection(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
            if lumitype=='PIXEL' and finecorrections is not None:
                calibratedlumi=finecorrections[run]*avglumi
            calibratedlumierr=perlsdata[6]*normval
            startorbit=perlsdata[7]
            numorbit=perlsdata[8]
            bxidxlistResult=[]
            bxvaluelistResult=[]
            bxerrorlistResult=[]
            calibratedbxdata=None
            beamdata=None
            if withBXInfo:
                if bxdata:
                    bxidxlist=bxdata[0]
                    bxvaluelist=bxdata[1]
                    #avglumiBX=sum(bxvaluelist)*normval*1.0e-03
                    bxlumierrlist=bxdata[2]
                    for idx,bxidx in enumerate(bxidxlist):
                        bxval=bxvaluelist[idx]
                        bxlumierr=bxlumierrlist[idx]
                        if finecorrections and finecorrections[run]:
                            if usecorrectionv2:
                                if driftcorrections and driftcorrections[run]:
                                    mybxval=lumiCorrections.applyfinecorrectionBXV2(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                                else:
                                    mybxval=lumiCorrections.applyfinecorrectionBXV2(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                            else:
                                mybxval=lumiCorrections.applyfinecorrectionBX(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
                        bxidxlistResult.append(bxidx)
                        bxvaluelistResult.append(mybxval)
                        bxerrorlistResult.append(bxlumierr*perbunchnormval)#no correciton on errors
                    del bxdata[1][:]
                    del bxdata[2][:]
            calibratedbxdata=(bxidxlistResult,bxvaluelistResult,bxerrorlistResult)
            if withBeamIntensity:
                beamdata=perlsdata[10]                
            result[run].append([lumilsnum,cmslsnum,timestamp,bs,beamenergy,calibratedlumi,calibratedlumierr,startorbit,numorbit,calibratedbxdata,beamdata,fillnum])
            del perlsdata[:]
    return result

def deliveredLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap,beamstatusfilter=None,normmap=None,withBXInfo=False,bxAlgo=None,xingMinLum=0,withBeamIntensity=False,lumitype='HF'):
    '''
    delivered lumi (including calibration,time integral)
    input:
       irunlsdict:  {run:[lsnum]}, where [lsnum]==None means all ; [lsnum]==[] means selected ls
       dataidmap : {run:(lumiid,trgid,hltid)}
       runsummaryMap: {run:[l1key(0),amodetag(1),egev(2),hltkey(3),fillnum(4),fillscheme(5),starttime(6),stoptime(7)]}
       beamstatus: LS filter on beamstatus 
       normmap: {since:[corrector(0),{paramname:paramvalue}(1),amodetag(2),egev(3),comment(4)]}
       withBXInfo: get per bunch info (optional)
       bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
       xingMinLum: cut on bx lumi value (optional)
       withBeamIntensity: get beam intensity info (optional)
       lumitype: luminosity source
    output:
       result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),deliveredlumi(5),calibratedlumierr(6),(bxvalues,bxerrs)(7),(bxidx,b1intensities,b2intensities)(8),fillnum(9)]}
       lumi unit: 1/ub
    '''
    result = {}
    lumip=lumiParameters.ParametersObject()
    lumirundata=dataDML.lumiRunByIds(schema,dataidmap,lumitype=lumitype)
    instresult=instLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap,beamstatusfilter=beamstatusfilter,withBXInfo=withBXInfo,bxAlgo=bxAlgo,xingMinLum=xingMinLum,withBeamIntensity=withBeamIntensity,lumitype=lumitype)
    
    intglumimap={}
    if lumitype=='HF':
        intglumimap=dataDML.intglumiForRange(schema,irunlsdict.keys())#some runs need drift correction
        
    allsince=[]
    if normmap:
        allsince=normmap.keys()
        allsince.sort()
    correctorname='fPoly' #HF default
    correctionparams={'a0':1000.0}#default:only to convert unit Hz/mb to Hz/ub
    runfillschemeMap={}
    fillschemePatternMap={}
    if lumitype=='PIXEL':
        correctorname='fPolyScheme' #PIXEL default
        correctionparams={'a0':1.0}
        fillschemePatternMap=dataDML.fillschemePatternMap(schema,'PIXEL')
    for run,perrundata in instresult.items():
        if perrundata is None:
            result[run]=None
            continue
        intglumi=0.
        if intglumimap and intglumimap.has_key(run) and intglumimap[run]:
            intglumi=intglumimap[run]
        nBXs=0
        if lumirundata and lumirundata.has_key(run) and lumirundata[run][2]:
            nBXs=lumirundata[run][2]
        fillschemeStr=''
        if runsummaryMap and runsummaryMap.has_key(run) and runsummaryMap[run][5]:
            fillschemeStr=runsummaryMap[run][5]
        if allsince:
            lastsince=allsince[0]
            for since in allsince:
                if run>=lastsince:
                    lastsince=since
            correctorname=normmap[lastsince][0]
            correctionparams=normmap[lastsince][1]
                                
        correctioninput=[0.,intglumi,nBXs,fillschemeStr,fillschemePatternMap]
        result[run]=[]
        for perlsdata in perrundata:#loop over ls
            lumilsnum=perlsdata[0]
            cmslsnum=perlsdata[1]
            timestamp=perlsdata[2]
            bs=perlsdata[3]
            beamenergy=perlsdata[4]
            instluminonorm=perlsdata[5]
            print 'instluminonorm ',instluminonorm
            correctioninput[0]=instluminonorm
            print 'correctorname ',correctorname
            print 'correctioninput ',correctioninput
            print 'correctionparams ',correctionparams
            totcorrectionFac=normFunctors.normFunctionCaller(correctorname,*correctioninput,**correctionparams)
            print 'instluminonorm ',instluminonorm
            print 'totcorrectionFac ',totcorrectionFac
            fillnum=perlsdata[11]
            instcorrectedlumi=totcorrectionFac*instluminonorm
            numorbit=perlsdata[8]
            numbx=lumip.NBX
            lslen=lumip.lslengthsec()
            deliveredlumi=instcorrectedlumi*lslen
            calibratedbxdata=None
            beamdata=None
            if withBXInfo:
                (bxidx,bxvalues,bxerrs)=perlsdata[9]
                if lumitype=='HF':
                    totcorrection=totcorrectionFac/1000.0
                    calibratedbxdata=[totcorrection*x for x in bxvalues]
                    calibratedlumierr=[totcorrection*x for x in bxerrs]
                del bxidx[:]
                del bxvalues[:]
                del bxerrs[:]
            if withBeamIntensity:
                beamdata=perlsdata[10]
            calibratedlumierr=0.0
            result[run].append([lumilsnum,cmslsnum,timestamp,bs,beamenergy,deliveredlumi,calibratedlumierr,calibratedbxdata,beamdata,fillnum])
            del perlsdata[:]
    return result

def deliveredLumiForRange(schema,inputRange,beamstatus=None,amodetag=None,egev=None,withBXInfo=False,bxAlgo=None,xingMinLum=0.0,withBeamIntensity=False,norm=None,datatag='DATA',finecorrections=None,driftcorrections=None,usecorrectionv2=False,lumitype='HF',branchName=None):
    '''
    delivered lumi (including calibration,time integral)
    input:
           inputRange  {run:[lsnum]} (required) [lsnum]==None means all ; [lsnum]==[] means selected ls 
           amodetag : accelerator mode for all the runs (optional) ['PROTPHYS','IONPHYS']
           beamstatus: LS filter on beamstatus (optional)
           amodetag: amodetag for  picking norm(optional)
           egev: beamenergy for picking norm(optional)
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           norm: norm factor name to use: if float, apply directly, if str search norm by name (optional)
           branchName: data version or branch name
    output:
           result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),deliveredlumi(5),calibratedlumierr(6),(bxvalues,bxerrs)(7),(bxidx,b1intensities,b2intensities)(8),fillnum(9)]}
           avg lumi unit: 1/ub
    '''
    lumip=lumiParameters.ParametersObject()
    result = {}
    normval=None
    perbunchnormval=None
    if norm:
        normval=_getnorm(schema,norm)
        perbunchnormval=float(normval)/float(1000)
    elif amodetag and egev:
        normval=_decidenormFromContext(schema,amodetag,egev)
        perbunchnormval=float(normval)/float(1000)
    instresult=instLumiForRange(schema,inputRange,beamstatusfilter=beamstatus,withBXInfo=withBXInfo,bxAlgo=bxAlgo,xingMinLum=xingMinLum,withBeamIntensity=withBeamIntensity,lumitype=lumitype,branchName=branchName)
    #instLumiForRange should have aleady handled the selection,unpackblob    
    for run,perrundata in instresult.items():
        if perrundata is None:
            result[run]=None
            continue
        result[run]=[]
        if not normval:#if norm cannot be decided , look for it according to context per run
            normval=_decidenormForRun(schema,run)
            perbunchnormval=float(normval)/float(1000)
        if not normval:#still not found? resort to global default (should never come here)
            normval=6370
            perbunchnormval=6.37
            print '[Warning] using default normalization '+str(normval)
        for perlsdata in perrundata:#loop over ls
            lumilsnum=perlsdata[0]
            cmslsnum=perlsdata[1]
            timestamp=perlsdata[2]
            bs=perlsdata[3]
            beamenergy=perlsdata[4]
            calibratedlumi=perlsdata[5]*normval#inst lumi
            fillnum=perlsdata[11]
            if lumitype=='HF' and finecorrections and finecorrections[run]:
                if usecorrectionv2:
                    if driftcorrections and driftcorrections[run]:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(calibratedlumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                    else:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(calibratedlumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                else:
                    calibratedlumi=lumiCorrections.applyfinecorrection(calibratedlumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
            if lumitype=='PIXEL' and finecorrections is not None:
                calibratedlumi=finecorrections[run]*calibratedlumi
            calibratedlumierr=perlsdata[6]*normval
            numorbit=perlsdata[8]
            numbx=lumip.NBX
            lslen=lumip.lslengthsec()
            deliveredlumi=calibratedlumi*lslen
            calibratedbxdata=None
            beamdata=None
            if withBXInfo:
                bxdata=perlsdata[9]
                if bxdata:
                    calibratedbxdata=(bxdata[0],[x*perbunchnormval for x in bxdata[1]],[x*perbunchnormval for x in bxdata[2]])
                del bxdata[1][:]
                del bxdata[2][:]
            if withBeamIntensity:
                beamdata=perlsdata[10]             
            result[run].append([lumilsnum,cmslsnum,timestamp,bs,beamenergy,deliveredlumi,calibratedlumierr,calibratedbxdata,beamdata,fillnum])
            del perlsdata[:]
    return result
                       
def lumiForRange(schema,inputRange,beamstatus=None,amodetag=None,egev=None,withBXInfo=False,bxAlgo=None,xingMinLum=0.0,withBeamIntensity=False,norm=None,datatag='DATA',finecorrections=None,driftcorrections=None,usecorrectionv2=False,lumitype='HF',branchName=None):
    '''
    delivered/recorded lumi
    input:
           inputRange  {run:[cmsls]} (required)
           beamstatus: LS filter on beamstatus (optional)
           amodetag: amodetag for  picking norm(optional)
           egev: beamenergy for picking norm(optional)
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           normname: norm factor name to use (optional)
           branchName: data version
    output:
           result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),deliveredlumi(5),recordedlumi(6),calibratedlumierror(7),(bxidx,bxvalues,bxerrs)(8),(bxidx,b1intensities,b2intensities)(9),fillnum(10)]}
           lumi unit: 1/ub
    '''
    if lumitype not in ['HF','PIXEL']:
        raise ValueError('unknown lumitype '+lumitype)
    #if branchName is None:
    #    branchName='DATA'
    lumip=lumiParameters.ParametersObject()
    lumitableName=''
    lumilstableName=''
    if lumitype=='HF':
        lumitableName=nameDealer.lumidataTableName()
        lumilstableName=nameDealer.lumisummaryv2TableName()
    else:
        lumitableName=nameDealer.pixellumidataTableName()
        lumilstableName=nameDealer.pixellumisummaryv2TableName()
    numbx=lumip.NBX
    result = {}
    normval=None
    perbunchnormval=None
    if norm:
        normval=_getnorm(schema,norm)
        perbunchnormval=float(normval)/float(1000)
    elif amodetag and egev:
        normval=_decidenormFromContext(schema,amodetag,egev)
        perbunchnormval=float(normval)/float(1000)
    c=lumiTime.lumiTime()
    for run in inputRange.keys():#loop over run
        lslist=inputRange[run]
        if lslist is not None and len(lslist)==0:#no selected ls, do nothing for this run
            result[run]=[]
            continue
        cmsrunsummary=dataDML.runsummary(schema,run)
        if len(cmsrunsummary)==0:#non existing run
            result[run]=None
            continue
        startTimeStr=cmsrunsummary[6]
        fillnum=cmsrunsummary[4]
        lumidataid=None
        trgdataid=None
        lumidataid=dataDML.guessLumiDataIdByRun(schema,run,lumitableName)
        if lumidataid is None :
            result[run]=None
            continue
        trgdataid=dataDML.guessTrgDataIdByRun(schema,run)
        (lumirunnum,lumidata)=dataDML.lumiLSById(schema,lumidataid,beamstatus=beamstatus,withBXInfo=withBXInfo,bxAlgo=bxAlgo,withBeamIntensity=withBeamIntensity,tableName=lumilstableName)
        if trgdataid is None :
            trgdata={}
        else:
            (trgrunnum,trgdata)=dataDML.trgLSById(schema,trgdataid)
            
        if not normval:#if norm cannot be decided , look for it according to context per run
            normval=_decidenormForRun(schema,run)
            perbunchnormval=float(normval)/float(1000)
        if not normval:#still not found? resort to global default (should never come here)
            normval=6370
            perbunchnormval=6.37
            print '[Warning] using default normalization '+str(normval)
        
        perrunresult=[]
        for lumilsnum,perlsdata in lumidata.items():
            cmslsnum=perlsdata[0]
            triggeredls=perlsdata[0]
            if lslist is not None and cmslsnum not in lslist:
                #cmslsnum=0
                triggeredls=0
                recordedlumi=0.0
            instlumi=perlsdata[1]
            instlumierror=perlsdata[2]
            avglumi=instlumi*normval
            calibratedlumi=avglumi
            if lumitype=='HF' and finecorrections and finecorrections[run]:
                if usecorrectionv2:
                    if driftcorrections and driftcorrections[run]:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                    else:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                else:
                    calibratedlumi=lumiCorrections.applyfinecorrection(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
            if lumitype=='PIXEL' and finecorrections is not None:
                calibratedlumi=finecorrections[run]*avglumi
            calibratedlumierror=instlumierror*normval
            bstatus=perlsdata[4]
            begev=perlsdata[5]
            numorbit=perlsdata[6]
            startorbit=perlsdata[7]
            timestamp=c.OrbitToTime(startTimeStr,startorbit,0)
            lslen=lumip.lslengthsec()
            deliveredlumi=calibratedlumi*lslen
            recordedlumi=0.0
            if triggeredls!=0:
                if not trgdata.has_key(cmslsnum):                    
                   # triggeredls=0 #if no trigger, set back to non-cms-active ls
                    recordedlumi=0.0 # no trigger->nobeam recordedlumi=None
                else:
                    deadcount=trgdata[cmslsnum][0] ##subject to change !!
                    bitzerocount=trgdata[cmslsnum][1]
                    bitzeroprescale=trgdata[cmslsnum][2]
                    deadfrac=trgdata[cmslsnum][3]
                    if deadfrac<0 or deadfrac>1.0:
                        deadfrac=1.0
                    #if float(bitzerocount)*float(bitzeroprescale)==0.0:
                    #    deadfrac=1.0
                    #else:
                    #    deadfrac=float(deadcount)/(float(bitzerocount)*float(bitzeroprescale))
                    #if deadfrac>1.0:
                    #    deadfrac=1.0  #artificial correction in case of deadfrac>1
                    recordedlumi=deliveredlumi*(1.0-deadfrac)
                    del trgdata[cmslsnum][:]
            bxdata=None
            if withBXInfo:
                bxinfo=perlsdata[8]
                bxvalueArray=None
                bxerrArray=None
                bxidxlist=[]
                bxvaluelist=[]
                bxerrorlist=[]
                if bxinfo:
                    bxvalueArray=bxinfo[0]
                    bxerrArray=bxinfo[1]
                    #if cmslsnum==1:
                    #    print 'bxvalueArray ',bxvalueArray
                    for idx,bxval in enumerate(bxvalueArray):                    
                        if finecorrections and finecorrections[run]:
                            if usecorrectionv2:
                                if driftcorrections and driftcorrections[run]:
                                    mybxval=lumiCorrections.applyfinecorrectionBXV2(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                                else:
                                    mybxval=lumiCorrections.applyfinecorrectionBXV2(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                            else:
                                mybxval=lumiCorrections.applyfinecorrectionBX(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
                        else:
                            mybxval=bxval*perbunchnormval
                        if mybxval>xingMinLum:
                            bxidxlist.append(idx)
                            bxvaluelist.append(mybxval)
                            bxerrorlist.append(bxerrArray[idx]*perbunchnormval)#no correciton on errors
                    del bxvalueArray[:]
                    del bxerrArray[:]
                bxdata=(bxidxlist,bxvaluelist,bxerrorlist)
            beamdata=None
            if withBeamIntensity:
                beaminfo=perlsdata[9]
                bxindexlist=[]
                b1intensitylist=[]
                b2intensitylist=[]                
                if beaminfo:
                    bxindexarray=beaminfo[0]
                    beam1intensityarray=beaminfo[1]
                    beam2intensityarray=beaminfo[2]                    
                    bxindexlist=bxindexarray.tolist()
                    b1intensitylist=beam1intensityarray.tolist()
                    b2intensitylist=beam2intensityarray.tolist()
                    del bxindexarray[:]
                    del beam1intensityarray[:]
                    del beam2intensityarray[:]
                beamdata=(bxindexlist,b1intensitylist,b2intensitylist)
            perrunresult.append([lumilsnum,triggeredls,timestamp,bstatus,begev,deliveredlumi,recordedlumi,calibratedlumierror,bxdata,beamdata,fillnum])
            del perlsdata[:]
        result[run]=perrunresult    
    return result

def lumiForIds(schema,irunlsdict,dataidmap,runsummaryMap,beamstatusfilter=None,normmap=None,correctioncoeffs=None,withBXInfo=False,bxAlgo=None,xingMinLum=0,withBeamIntensity=False,lumitype='HF',datatag=None):
    '''
    delivered/recorded lumi  (including calibration,time integral)
    input:
       irunlsdict:  {run:[lsnum]}, where [lsnum]==None means all ; [lsnum]==[] means selected ls
       dataidmap : {run:(lumiid,trgid,hltid)}
       runsummaryMap: {run:[l1key(0),amodetag(1),egev(2),hltkey(3),fillnum(4),fillscheme(5),starttime(6),stoptime(7)]}
       beamstatus: LS filter on beamstatus 
       normmap: {run:(lumiid,trgid,hltid)}
       correctioncoeffs: {name:(alpha1,alpha2,drift)}
       withBXInfo: get per bunch info (optional)
       bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
       xingMinLum: cut on bx lumi value (optional)
       withBeamIntensity: get beam intensity info (optional)
       lumitype: luminosity source
       datatag: data version 
    output:
       result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),deliveredlumi(5),recordedlumi(6),calibratedlumierror(7),(bxidx,bxvalues,bxerrs)(8),(bxidx,b1intensities,b2intensities)(9),fillnum(10)]}
       lumi unit: 1/ub
    '''
    result = {}
    lumip=lumiParameters.ParametersObject()
    lumirundata=dataDML.lumiRunByIds(schema,dataidmap,lumitype=lumitype)
    instresult=instLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap,beamstatusfilter=beamstatusfilter,withBXInfo=withBXInfo,bxAlgo=bxAlgo,xingMinLum=xingMinLum,withBeamIntensity=withBeamIntensity,lumitype=lumitype,datatag=datatag)
    trgresult=trgForIds(schema,irunlsdict,dataidmap)
    #print 'trgresult ',trgresult
    for run in irunlsdict.keys():#loop over run
        lslist=irunlsdict[run] #selected ls
        if lslist is not None and len(lslist)==0:#no selected ls, do nothing for this run
            result[run]=[]
            continue
        if not lumirundata.has_key(run):
            result[run]=None
            continue
        for run,perrundata in instresult.items():
            if perrundata is None:
                result[run]=None
                continue
        (amodetag,normval,egev,norm_occ2,norm_et,norm_pu,constfactor)=normmap[run].values()[0]
        if not normval:
            normval=6.52e3
            occ2norm=1.0
            norm_et=1.0
            occ1constfactor=1.0
            norm_pu=0.0
            alpha1=0.0
            alpha2=0.0
            print '[Warning] using default normalization ',normval
        corrToUse=correctioncoeffs.values()[0]
        lctor=LumiCorrector.LumiCorrector(occ1norm=normval,occ2norm=norm_occ2,etnorm=norm_et,occ1constfactor=constfactor,punorm=norm_pu,alpha1=corrToUse[1],alpha2=corrToUse[2])
        drifter=corrToUse[3]
        nBXs=lumirundata[run][2]
        startTimeStr=runsummaryMap[run][6]
        perrunresult=[]
        for perlsdata in perrundata:#loop over ls
            lumilsnum=perlsdata[0]
            cmslsnum=perlsdata[1]
            triggeredls=perlsdata[1] #place holder for selected and triggered ls
            if lslist is not None and cmslsnum not in lslist:#this ls exists but not selected
                triggeredls=0
                recordedlumi=0.0
            alltrgls=[]
            if trgresult.has_key(run):
                alltrgls=[x[0] for x in trgresult[run]]
            deadfrac=1.0
            if triggeredls!=0 and triggeredls in alltrgls:
                trglsidx=alltrgls.index(triggeredls)
                deadfrac=trgresult[run][trglsidx][1]
                if deadfrac<0 or deadfrac>1.0:
                    deadfrac=1.0
            timestamp=perlsdata[2]
            bs=perlsdata[3]
            beamenergy=perlsdata[4]
            instluminonorm=perlsdata[5]
            fillnum=perlsdata[11]
            totcorrection=1.0
            if lumitype=='HF':
                totcorrection=lctor.TotalNormOcc1(instluminonorm,nBXs)
            if lumitype=='PIXEL':
                totcorrection=instluminonorm*lctor.PixelAfterglowFactor(self,nBXs)
            instcorrectedlumi=totcorrection*instluminonorm
            numorbit=perlsdata[8]
            numbx=lumip.NBX
            lslen=lumip.lslengthsec()
            deliveredlumi=instcorrectedlumi*lslen
            recordedlumi=(1.0-deadfrac)*deliveredlumi
            #print 'recordedlumi ',recordedlumi
            calibratedbxdata=None
            beamdata=None
            if withBXInfo:
                (bxidx,bxvalues,bxerrs)=perlsdata[9]
                if lumitype=='HF':
                    totcorrection=lctor.TotalNormOcc1(instluminonorm,nBXs)/1000.0
                    calibratedbxdata=[totcorrection*x for x in bxvalues]
                    calibratedlumierr=[totcorrection*x for x in bxerrs]
                del bxidx[:]
                del bxvalues[:]
                del bxerrs[:]
            if withBeamIntensity:
                beamdata=perlsdata[10]
            calibratedlumierr=0.0
            perrunresult.append([lumilsnum,triggeredls,timestamp,bs,beamenergy,deliveredlumi,recordedlumi,calibratedlumierr,calibratedbxdata,beamdata,fillnum])
            del perlsdata[:]
        result[run]=perrunresult    
    return result

def effectiveLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=None,beamstatusfilter=None,normmap=None,correctioncoeffs=None,hltpathname=None,hltpathpattern=None,withBXInfo=False,bxAlgo=None,xingMinLum=0.0,withBeamIntensity=False,lumitype='HF',datatag=None):
    '''
    input:
           irunlsdict: {run:[lsnum]}, where [lsnum]==None means all ; [lsnum]==[] means selected ls
           dataidmap : {run:(lumiid,trgid,hltid)}
           runsummaryMap: {run:[l1key(0),amodetag(1),egev(2),hltkey(3),fillnum(4),fillscheme(5),starttime(6),stoptime(7)]}
           beamstatusfilter: LS filter on beamstatus
           normmap: {run:(lumiid,trgid,hltid)}
           correctioncoeffs: {name:(alpha1,alpha2,drift)}
           hltpathname: selected hltpathname
           hltpathpattern: regex select hltpaths           
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           lumitype: luminosity source
           datatag: data version 
    output:
           result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),deliveredlumi(5),recordedlumi(6),calibratedlumierror(7),{hltpath:[l1name,l1prescale,hltprescale,efflumi]},bxdata,beamdata,fillnum]}
           lumi unit: 1/ub
    '''
    result = {}
    lumip=lumiParameters.ParametersObject()
    lumirundata=dataDML.lumiRunByIds(schema,dataidmap,lumitype=lumitype)#{runnum:(datasource(0),nominalegev(1),ncollidingbunches(2))}
    instresult=instLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap,beamstatusfilter=beamstatusfilter,withBXInfo=withBXInfo,bxAlgo=bxAlgo,xingMinLum=xingMinLum,withBeamIntensity=withBeamIntensity,lumitype=lumitype,datatag=datatag)  #{run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),instlumi(5),instlumierr(6),startorbit(7),numorbit(8),(bxidx,bxvalues,bxerrs)(9),(bxidx,b1intensities,b2intensities)(10),fillnum(11)]}
    trgresult=trgForIds(schema,irunlsdict,dataidmap,withPrescale=True) #{run:[cmslsnum,deadfrac,deadtimecount,bitzero_count,bitzero_prescale,[(bitname,prescale,counts)]]}
    hltresult=hltForIds(schema,irunlsdict,dataidmap,hltpathname=hltpathname,hltpathpattern=hltpathpattern,withL1Pass=False,withHLTAccept=False) #{runnumber:[(cmslsnum,[(hltpath,hltprescale,l1pass,hltaccept),...]),(cmslsnum,[])})}
    trgprescalemap={} #{bitname:l1prescale}
    for run in irunlsdict.keys():
        lslist=irunlsdict[run]
        if lslist is not None and len(lslist)==0:#no selected ls, do nothing for this run
            result[run]=[]
            continue
        if not lumirundata.has_key(run):
            result[run]=None
            continue        
        perrunresult=[]
        for run,perrundata in instresult.items():
            if perrundata is None:
                result[run]=None
                continue
        (amodetag,normval,egev,norm_occ2,norm_et,norm_pu,constfactor)=normmap[run].values()[0]
        hlttrgmap=dataDML.hlttrgMappingByrun(schema,run)
        if not normval:
            normval=6.52e3
            occ2norm=1.0
            norm_et=1.0
            occ1constfactor=1.0
            norm_pu=0.0
            alpha1=0.0
            alpha2=0.0
            print '[Warning] using default normalization without correction',normval
        corrToUse=correctioncoeffs.values()[0]
        lctor=LumiCorrector.LumiCorrector(occ1norm=normval,occ2norm=norm_occ2,etnorm=norm_et,occ1constfactor=constfactor,punorm=norm_pu,alpha1=corrToUse[1],alpha2=corrToUse[2])
        drifter=corrToUse[3]
        nBXs=lumirundata[run][2]
        startTimeStr=runsummaryMap[run][6]
        l1bitinfo=[]
        hltpathinfo=[]
        alltrgls=[x[0] for x in trgresult[run]]
        allhltls=[x[0] for x in hltresult[run]]
        for perlsdata in perrundata:#loop over ls
            efflumidict={}#{pathname:[[l1bitname,l1prescale,hltprescale,efflumi]]}       
            lumilsnum=perlsdata[0]
            cmslsnum=perlsdata[1]
            triggeredls=perlsdata[1]
            if lslist is not None and cmslsnum not in lslist:
                triggeredls=0
                recordedlumi=0.0
            deadfrac=1.0
            efflumi=0.0
            if triggeredls!=0 and triggeredls in alltrgls:
                trglsidx=alltrgls.index(triggeredls)
                deadfrac=trgresult[run][trglsidx][1]
                if deadfrac<0 or deadfrac>1.0:
                    deadfrac=1.0                    
                l1bitinfo=trgresult[run][trglsidx][5]
                if l1bitinfo:
                    for thisbitinfo in l1bitinfo:
                        thisbitname=thisbitinfo[0]
                        thisbitprescale=thisbitinfo[2]
                        trgprescalemap['"'+thisbitname+'"']=thisbitprescale            
            timestamp=perlsdata[2]
            bs=perlsdata[3]
            beamenergy=perlsdata[4]
            instluminonorm=perlsdata[5]
            fillnum=perlsdata[11]
            totcorrection=1.0
            if lumitype=='HF':
                totcorrection=lctor.TotalNormOcc1(instluminonorm,nBXs)
            if lumitype=='PIXEL':
                totcorrection=instluminonorm*lctor.PixelAfterglowFactor(self,nBXs)
            instcorrectedlumi=totcorrection*instluminonorm
            numorbit=perlsdata[8]
            numbx=lumip.NBX
            lslen=lumip.lslengthsec()
            deliveredlumi=instcorrectedlumi*lslen
            recordedlumi=(1.0-deadfrac)*deliveredlumi            
            calibratedbxdata=None
            beamdata=None
            calibratedlumierr=0.0
            if withBXInfo:
                (bxidx,bxvalues,bxerrs)=perlsdata[9]
                if lumitype=='HF':
                    totcorrection=lctor.TotalNormOcc1(instluminonorm,nBXs)/1000.0
                    calibratedbxdata=[totcorrection*x for x in bxvalues]
                    calibratedlumierr=[totcorrection*x for x in bxerrs]
                del bxidx[:]
                del bxvalues[:]
                del bxerrs[:]
            if withBeamIntensity:
                beamdata=perlsdata[10]
            if cmslsnum in allhltls:
                hltlsidx=allhltls.index(cmslsnum)
                hltpathdata=hltresult[run][hltlsidx][1]
                for pathidx,thispathinfo in enumerate(hltpathdata):
                    efflumi=0.0
                    thispathname=thispathinfo[0]
                    thisprescale=thispathinfo[1]
                    thisl1seed=None
                    l1bitname=None
                    l1prescale=None
                    try:
                        thisl1seed=hlttrgmap[thispathname]#no path or no seed
                    except KeyError:
                        thisl1seed=None
                    if thisl1seed:
                        try:
                            l1bitname=hltTrgSeedMapper.findUniqueSeed(thispathname,thisl1seed)
                            if l1bitname:
                                l1prescale=trgprescalemap[l1bitname]#need to match double quoted string!
                            else:
                                l1prescale=None
                        except KeyError:
                            l1prescale=None
                        if l1prescale and thisprescale :#normal both prescaled
                            efflumi=recordedlumi/(float(l1prescale)*float(thisprescale))
                            efflumidict[thispathname]=[l1bitname,l1prescale,thisprescale,efflumi]
                        elif l1prescale and thisprescale==0: #hltpath in menu but masked
                            efflumi=0.0
                            efflumidict[thispathname]=[l1bitname,l1prescale,thisprescale,efflumi]
                        else:#no path
                            efflumi=0.0
                            efflumidict[thispathname]=[None,0,thisprescale,efflumi]
                    else:
                        efflumi=0.0
                        efflumidict[thispathname]=[None,0,0,efflumi]
            else:
                efflumi=0.0
                if hltpathname:
                    efflumidict[hltpathname]=[None,0,0,efflumi]
                elif hltpathpattern:
                    efflumidict[hltpathname]=[None,0,0,efflumi]
            perrunresult.append([lumilsnum,triggeredls,timestamp,bs,beamenergy,deliveredlumi,recordedlumi,calibratedlumierr,efflumidict,calibratedbxdata,beamdata,fillnum])
        result[run]=perrunresult    
    return result

def effectiveLumiForRange(schema,inputRange,hltpathname=None,hltpathpattern=None,amodetag=None,beamstatus=None,egev=None,withBXInfo=False,xingMinLum=0.0,bxAlgo=None,withBeamIntensity=False,norm=None,finecorrections=None,driftcorrections=None,usecorrectionv2=False,lumitype='HF',branchName=None):
    '''
    input:
           inputRange  {run:[cmsls]} (required)
           hltpathname: selected hltpathname
           hltpathpattern: regex select hltpaths           
           amodetag: amodetag for  picking norm(optional)
           egev: beamenergy for picking norm(optional)
           withBXInfo: get per bunch info (optional)
           bxAlgo: algoname for bx values (optional) ['OCC1','OCC2','ET']
           xingMinLum: cut on bx lumi value (optional)
           withBeamIntensity: get beam intensity info (optional)
           normname: norm factor name to use (optional)
           branchName: data version
    output:
    result {run:[lumilsnum(0),cmslsnum(1),timestamp(2),beamstatus(3),beamenergy(4),deliveredlumi(5),recordedlumi(6),calibratedlumierror(7),{hltpath:[l1name,l1prescale,hltprescale,efflumi]},bxdata,beamdata,fillnum]}
           lumi unit: 1/ub
    '''
    if lumitype not in ['HF','PIXEL']:
        raise ValueError('unknown lumitype '+lumitype)
    if branchName is None:
        branchName='DATA'
    lumitableName=''
    lumilstableName=''
    if lumitype=='HF':
        lumitableName=nameDealer.lumidataTableName()
        lumilstableName=nameDealer.lumisummaryv2TableName()
    else:
        lumitableName=nameDealer.pixellumidataTableName()
        lumilstableName=nameDealer.pixellumisummaryv2TableName()
    numbx=3564
    result = {}
    normval=None
    perbunchnormval=None
    if norm:
        normval=_getnorm(schema,norm)
        perbunchnormval=float(normval)/float(1000)
    elif amodetag and egev:
        normval=_decidenormFromContext(schema,amodetag,egev)
        perbunchnormval=float(normval)/float(1000)
    c=lumiTime.lumiTime()
    lumip=lumiParameters.ParametersObject()
    for run in inputRange.keys():
        lslist=inputRange[run]
        if lslist is not None and len(lslist)==0:#no selected ls, do nothing for this run
            result[run]=[]
            continue
        cmsrunsummary=dataDML.runsummary(schema,run)
        if len(cmsrunsummary)==0:#non existing run
            result[run]=None
            continue
        startTimeStr=cmsrunsummary[6]
        fillnum=cmsrunsummary[4]
        lumidataid=None
        trgdataid=None
        hltdataid=None
        lumidataid=dataDML.guessLumiDataIdByRun(schema,run,lumitableName)
        trgdataid=dataDML.guessTrgDataIdByRun(schema,run)
        hltdataid=dataDML.guessHltDataIdByRun(schema,run)
        if lumidataid is None or trgdataid is None or hltdataid is None:
            result[run]=None
            continue
        (lumirunnum,lumidata)=dataDML.lumiLSById(schema,lumidataid,beamstatus,tableName=lumilstableName)
        (trgrunnum,trgdata)=dataDML.trgLSById(schema,trgdataid,withPrescale=True)
        (hltrunnum,hltdata)=dataDML.hltLSById(schema,hltdataid,hltpathname=hltpathname,hltpathpattern=hltpathpattern)
        hlttrgmap=dataDML.hlttrgMappingByrun(schema,run)
        if not normval:#if norm cannot be decided , look for it according to context per run
            normval=_decidenormForRun(schema,run)
            perbunchnormval=float(normval)/float(1000)
        if not normval:#still not found? resort to global default (should never come here)
            normval=6370
            perbunchnormval=6.37
            print '[Warning] using default normalization '+str(normval)
        perrunresult=[]
        for lumilsnum,perlsdata in lumidata.items():
            cmslsnum=perlsdata[0]            
            triggeredls=perlsdata[0] 
            if lslist is not None and cmslsnum not in lslist:
                #cmslsnum=0
                triggeredls=0
                recordedlumi=0.0
            instlumi=perlsdata[1]
            instlumierror=perlsdata[2]
            avglumi=instlumi*normval
            calibratedlumi=avglumi 
            if lumitype=='HF' and finecorrections and finecorrections[run]:
                if usecorrectionv2:
                    if driftcorrections and driftcorrections[run]:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                    else:
                        calibratedlumi=lumiCorrections.applyfinecorrectionV2(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                else:
                    calibratedlumi=lumiCorrections.applyfinecorrection(avglumi,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
            if lumitype=='PIXEL' and finecorrections is not None:
                calibratedlumi=finecorrections[run]*avglumi
            calibratedlumierror=instlumierror*normval
            bstatus=perlsdata[4]
            begev=perlsdata[5]
            numorbit=perlsdata[6]
            startorbit=perlsdata[7]
            timestamp=c.OrbitToTime(startTimeStr,startorbit,0)
            lslen=lumip.lslengthsec()
            deliveredlumi=calibratedlumi*lslen
            recordedlumi=0.0
            trgprescalemap={}#trgprescalemap for this ls
            efflumidict={}
            if triggeredls!=0:
                if not trgdata.has_key(cmslsnum):
                    #triggeredls=0 #if no trigger, set back to non-cms-active ls
                    recordedlumi=0.0 # no trigger->nobeam recordedlumi=None
                else:
                    deadcount=trgdata[cmslsnum][0] ##subject to change !!
                    bitzerocount=trgdata[cmslsnum][1]
                    bitzeroprescale=trgdata[cmslsnum][2]
                    deadfrac=trgdata[cmslsnum][3]
                    if deadfrac<0 or deadfrac>1.0:
                        deadfrac=1.0
                    #if float(bitzerocount)*float(bitzeroprescale)==0.0:
                    #    deadfrac=1.0
                    #else:
                    #    deadfrac=float(deadcount)/(float(bitzerocount)*float(bitzeroprescale))
                    #if deadfrac>1.0:
                    #    deadfrac=1.0  #artificial correction in case of deadfrac>1
                    recordedlumi=deliveredlumi*(1.0-deadfrac)
                    l1bitinfo=trgdata[cmslsnum][4]
                    if l1bitinfo:
                        for thisbitinfo in l1bitinfo:
                            thisbitname=thisbitinfo[0]
                            thisbitprescale=thisbitinfo[2]
                            #trgprescalemap['"'+thisbitname+'"']=thisbitprescale#note:need to double quote bit name!
                            trgprescalemap['"'+thisbitname+'"']=thisbitprescale

                    del trgdata[cmslsnum][:]
                if hltdata.has_key(cmslsnum):                
                    hltpathdata=hltdata[cmslsnum]
                    #print 'hltpathdata ',hltpathdata
                    for pathidx,thispathinfo in enumerate(hltpathdata):
                        efflumi=0.0                    
                        thispathname=thispathinfo[0]
                        thisprescale=thispathinfo[1]
                        thisl1seed=None
                        l1bitname=None
                        l1prescale=None
                        try:
                            thisl1seed=hlttrgmap[thispathname]
                        except KeyError:
                            thisl1seed=None
                        #print 'hltpath, l1seed, hltprescale ',thispathname,thisl1seed,thisprescale
                        if thisl1seed:                            
                            try:
                                l1bitname=hltTrgSeedMapper.findUniqueSeed(thispathname,thisl1seed)
                                if l1bitname :
                                    l1prescale=trgprescalemap[l1bitname]#need to match double quoted string!
                                else:
                                    l1prescale=None
                            except KeyError:
                                l1prescale=None                           
                        if l1prescale and thisprescale :#normal both prescaled
                            efflumi=recordedlumi/(float(l1prescale)*float(thisprescale))
                            efflumidict[thispathname]=[l1bitname,l1prescale,thisprescale,efflumi]
                        elif l1prescale and thisprescale==0: #hltpath in menu but masked
                            efflumi=0.0
                            efflumidict[thispathname]=[l1bitname,l1prescale,thisprescale,efflumi]
                        else:
                            efflumi=0.0
                            efflumidict[thispathname]=[None,0,thisprescale,efflumi]
                
            bxvaluelist=[]
            bxerrorlist=[]
            bxdata=None
            beamdata=None
            if withBXInfo:
                bxinfo=lumidata[8]
                bxvalueArray=None
                bxerrArray=None
                if bxinfo:
                    bxvalueArray=bxinfo[0]
                    bxerrArray=bxinfo[1]
                    for idx,bxval in enumerate(bxvalueArray):
                        if finecorrections and finecorrections[run]:
                            if usecorrectionv2:
                                if driftcorrections and driftcorrections[run]:
                                    mybxval=lumiCorrections.applyfinecorrectionBXV2(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],driftcorrections[run])
                                else:
                                    mybxval=lumiCorrections.applyfinecorrectionBXV2(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2],finecorrections[run][3],finecorrections[run][4],1.0)
                            else:
                                mybxval=lumiCorrections.applyfinecorrectionBX(bxval,avglumi,perbunchnormval,finecorrections[run][0],finecorrections[run][1],finecorrections[run][2])
                        else:
                            mybxval=bxval*perbunchnormval
                        if mybxval>xingMinLum:
                            bxidxlist.append(idx)
                            bxvaluelist.append(bxval)
                            bxerrorlist.append(bxerrArray[idx])
                    del bxvalueArray[:]
                    del bxerrArray[:]
                bxdata=(bxidxlist,bxvaluelist,bxerrorlist)    
            if withBeamIntensity:
                beaminfo=perlsdata[9]
                bxindexlist=[]
                b1intensitylist=[]
                b2intensitylist=[]
                if beaminfo:
                    bxindexarray=beaminfo[0]
                    beam1intensityarray=beaminfo[1]
                    beam2intensityarray=beaminfo[2]                    
                    bxindexlist=bxindexarray.tolist()
                    b1intensitylist=beam1intensityarray.tolist()
                    b2intensitylist=beam2intensityarray.tolist()
                    del bxindexarray[:]
                    del beam1intensityarray[:]
                    del beam2intensityarray[:]
                beamdata=(bxindexlist,b1intensitylist,b2intensitylist)
#            print cmslsnum,deliveredlumi,recordedlumi,efflumidict
            perrunresult.append([lumilsnum,triggeredls,timestamp,bstatus,begev,deliveredlumi,recordedlumi,calibratedlumierror,efflumidict,bxdata,beamdata,fillnum])
            del perlsdata[:]
        result[run]=perrunresult
    #print result
    return result

def validation(schema,run=None,cmsls=None):
    '''retrieve validation data per run or all
    input: run. if not run, retrive all; if cmslsnum selection list pesent, filter out unselected result
    output: {run:[[cmslsnum,status,comment]]}
    '''
    result={}
    qHandle=schema.newQuery()
    queryHandle.addToTableList(nameDealer.lumivalidationTableName())
    queryHandle.addToOutputList('RUNNUM','runnum')
    queryHandle.addToOutputList('CMSLSNUM','cmslsnum')
    queryHandle.addToOutputList('FLAG','flag')
    queryHandle.addToOutputList('COMMENT','comment')
    if run:
        queryCondition='RUNNUM=:runnum'
        queryBind=coral.AttributeList()
        queryBind.extend('runnum','unsigned int')
        queryBind['runnum'].setData(run)
        queryHandle.setCondition(queryCondition,queryBind)
    queryResult=coral.AttributeList()
    queryResult.extend('runnum','unsigned int')
    queryResult.extend('cmslsnum','unsigned int')
    queryResult.extend('flag','string')
    queryResult.extend('comment','string')
    queryHandle.defineOutput(queryResult)
    cursor=queryHandle.execute()
    while cursor.next():
        runnum=cursor.currentRow()['runnum'].data()
        if not result.has_key(runnum):
            result[runnum]=[]
        cmslsnum=cursor.currentRow()['cmslsnum'].data()
        flag=cursor.currentRow()['flag'].data()
        comment=cursor.currentRow()['comment'].data()
        result[runnum].append([cmslsnum,flag,comment])
    if run and cmsls and len(cmsls)!=0:
        selectedresult={}
        for runnum,perrundata in result.items():
            for lsdata in perrundata:
                if lsdata[0] not in cmsls:
                    continue
                if not selectedresult.has_key(runnum):
                    selectedresult[runnum]=[]
                selectedresult[runnum].append(lsdata)
        return selectedresult
    else:
        return result
##===printers
    

