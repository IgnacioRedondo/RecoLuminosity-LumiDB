#!/usr/bin/env python
VERSION='1.00'
import os,sys,datetime
import coral
from RecoLuminosity.LumiDB import lumiTime,argparse,nameDealer,selectionParser,hltTrgSeedMapper,connectstrParser,cacheconfigParser,matplotRender,lumiQueryAPI,inputFilesetParser,CommonUtil
from matplotlib.figure import Figure
class constants(object):
    def __init__(self):
        self.NORM=1.0
        self.LUMIVERSION='0001'
        self.BEAMMODE='stable' #possible choices stable,quiet,either
        self.VERBOSE=False
    def defaultfrontierConfigString(self):
        return """<frontier-connect><proxy url="http://cmst0frontier.cern.ch:3128"/><proxy url="http://cmst0frontier.cern.ch:3128"/><proxy url="http://cmst0frontier1.cern.ch:3128"/><proxy url="http://cmst0frontier2.cern.ch:3128"/><server url="http://cmsfrontier.cern.ch:8000/FrontierInt"/><server url="http://cmsfrontier.cern.ch:8000/FrontierInt"/><server url="http://cmsfrontier1.cern.ch:8000/FrontierInt"/><server url="http://cmsfrontier2.cern.ch:8000/FrontierInt"/><server url="http://cmsfrontier3.cern.ch:8000/FrontierInt"/><server url="http://cmsfrontier4.cern.ch:8000/FrontierInt"/></frontier-connect>"""

def getInstLumiPerLS(dbsession,c,runList,selectionDict,beamstatus=None,beamenergy=None,beamenergyfluctuation=0.09):
    '''
    input: runList[runnum], selectionDict{runnum:[ls]}
    output:[[runnumber,lsnumber,deliveredInst,recordedInst,norbit,startorbit,runstarttime]]
    '''
    result=[]
    selectedRunlist=runList
    if len(selectionDict)!=0:
        selectedRunlist=[]
        allruns=runlist+selectionDict.keys()
        dups=CommonUtil.count_dups(allruns)
        for runnum,dupcount in dups:
            if dupcount==2:
                selectedRunlist.append(runnum)
                
    dbsession.transaction().start(True)
    for run in selectedRunlist:
        q=dbsession.nominalSchema().newQuery()
        runsummary=lumiQueryAPI.runsummaryByrun(q,run)
        del q
        runstarttime=runsummary[3]
        runstoptime=runsummary[4]
        q=dbsession.nominalSchema().newQuery()
        lumiperrun=lumiQueryAPI.lumisummaryByrun(q,run,c.LUMIVERSION,beamstatus,beamenergy,beamenergyfluctuation)
        del q
        for lumiperls in lumiperrun:
            cmslsnum=lumiperls[0]
            instlumi=lumiperls[1]
            recordedlumi=0.0
            numorbit=lumiperls[2]
            startorbit=lumiperls[3]
            deadcount=0
            bitzero=0
            result.append([run,cmslsnum,instlumi,recordedlumi,numorbit,startorbit,runstarttime,runstoptime])
    dbsession.transaction().commit()
    if c.VERBOSE:
        print result
    return result              

def getLumiPerRun(dbsession,c,run,beamstatus=None,beamenergy=None,beamenergyfluctuation=0.09):
    '''
    input: run
    output:{runnumber:[[lsnumber,deliveredInst,recordedInst,norbit,startorbit,runstarttime,runstoptime]]}
    '''
    result=[]
    dbsession.transaction().start(True)
    q=dbsession.nominalSchema().newQuery()
    runsummary=lumiQueryAPI.runsummaryByrun(q,run)
    del q
    runstarttime=runsummary[3]
    runstoptime=runsummary[4]
    q=dbsession.nominalSchema().newQuery()
    lumiperrun=lumiQueryAPI.lumisummaryByrun(q,run,c.LUMIVERSION,beamstatus,beamenergy,beamenergyfluctuation)
    del q
    q=dbsession.nominalSchema().newQuery()
    trgperrun=lumiQueryAPI.trgbitzeroByrun(q,run) # {cmslsnum:[trgcount,deadtime,bitname,prescale]}
    del q
        
    for lumiperls in lumiperrun:
        cmslsnum=lumiperls[0]
        instlumi=lumiperls[1]
        recordedlumi=0.0
        numorbit=lumiperls[2]
        startorbit=lumiperls[3]
        deadcount=0
        bitzero=0
        if trgperrun.has_key(cmslsnum):
            bitzero=trgperrun[cmslsnum][0]
            deadcount=trgperrun[cmslsnum][1]
            try:
                recordedlumi=instlumi*(1.0-float(deadcount)/float(bitzero))
            except ZeroDivisionError:
                pass
        result.append([cmslsnum,instlumi,recordedlumi,numorbit,startorbit,runstarttime,runstoptime])
    dbsession.transaction().commit()
    if c.VERBOSE:
        print result
    return result          
    
def main():
    c=constants()
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),description="Plot integrated luminosity as function of the time variable of choice")
    # add required arguments
    parser.add_argument('-c',dest='connect',action='store',required=True,help='connect string to lumiDB')
    # add optional arguments
    parser.add_argument('-P',dest='authpath',action='store',help='path to authentication file')
    parser.add_argument('-n',dest='normfactor',action='store',help='normalization factor (optional, default to 1.0)')
    parser.add_argument('-i',dest='inputfile',action='store',help='lumi range selection file (optional)')
    parser.add_argument('-o',dest='outputfile',action='store',help='csv outputfile name (optional)')
    parser.add_argument('-b',dest='beammode',action='store',help='beam mode, optional, no default')
    parser.add_argument('-lumiversion',dest='lumiversion',action='store',help='lumi data version, optional for all, default 0001')
    parser.add_argument('-begin',dest='begin',action='store',help='begin xvalue (required)')
    parser.add_argument('-end',dest='end',action='store',help='end xvalue(optional). Default to the maximum exists DB')
    parser.add_argument('-batch',dest='batch',action='store',help='graphical mode to produce PNG file. Specify graphical file here. Default to lumiSum.png')
    parser.add_argument('--interactive',dest='interactive',action='store_true',help='graphical mode to draw plot in a TK pannel.')
    parser.add_argument('-timeformat',dest='timeformat',action='store',help='specific python timeformat string (optional).  Default mm/dd/yy hh:min:ss.00')
    parser.add_argument('-siteconfpath',dest='siteconfpath',action='store',help='specific path to site-local-config.xml file, default to $CMS_PATH/SITECONF/local/JobConfig, if path undefined, fallback to cern proxy&server')
    parser.add_argument('action',choices=['peakperday','run'],help='plot type of choice')
    #graphical mode options
    parser.add_argument('--verbose',dest='verbose',action='store_true',help='verbose mode, print result also to screen')
    parser.add_argument('--debug',dest='debug',action='store_true',help='debug')
    # parse arguments
    args=parser.parse_args()
    connectstring=args.connect
    connectparser=connectstrParser.connectstrParser(connectstring)
    connectparser.parse()
    usedefaultfrontierconfig=False
    cacheconfigpath=''
    if connectparser.needsitelocalinfo():
        if not args.siteconfpath:
            cacheconfigpath=os.environ['CMS_PATH']
            if cacheconfigpath:
                cacheconfigpath=os.path.join(cacheconfigpath,'SITECONF','local','JobConfig','site-local-config.xml')
            else:
                usedefaultfrontierconfig=True
        else:
            cacheconfigpath=args.siteconfpath
            cacheconfigpath=os.path.join(cacheconfigpath,'site-local-config.xml')
        p=cacheconfigParser.cacheconfigParser()
        if usedefaultfrontierconfig:
            p.parseString(c.defaultfrontierConfigString)
        else:
            p.parse(cacheconfigpath)
        connectstring=connectparser.fullfrontierStr(connectparser.schemaname(),p.parameterdict())
    runnumber=0
    svc = coral.ConnectionService()
    if args.debug :
        msg=coral.MessageStream('')
        msg.setMsgVerbosity(coral.message_Level_Debug)
    ifilename=''
    ofilename='instlumi.csv'
    beammode='stable'
    timeformat=''
    selectionDict={}
    if args.authpath and len(args.authpath)!=0:
        os.environ['CORAL_AUTH_PATH']=args.authpath
    if args.normfactor:
        c.NORM=float(args.normfactor)
    if args.lumiversion:
        c.LUMIVERSION=args.lumiversion
    if args.beammode:
        c.BEAMMODE=args.beammode
    if args.verbose:
        c.VERBOSE=True
    if args.inputfile:
        ifilename=args.inputfile
    if args.batch:
        opicname=args.batch
    if args.outputfile:
        ofilename=args.outputfile
    if args.timeformat:
        timeformat=args.timeformat
    session=svc.connect(connectstring,accessMode=coral.access_Update)
    session.typeConverter().setCppTypeForSqlType("unsigned int","NUMBER(10)")
    session.typeConverter().setCppTypeForSqlType("unsigned long long","NUMBER(20)")
    if ifilename:
        ifparser=inputFilesetParser(ifilename)
        runsandls=ifparser.runsandls()
        keylist=runsandls.keys()
        keylist.sort()
        for run in keylist:
            if selectionDict.has_key(run):
                lslist=runsandls[run]
                lslist.sort()
                selectionDict[run]=lslist
    if args.action == 'run':
        minRun=int(args.begin)
        if not args.end:
            maxRun=minRun 
        else:
            maxRun=int(args.end)            
        runList=range(minRun,maxRun+1)
    elif args.action == 'peakperday':
        session.transaction().start(True)
        t=lumiTime.lumiTime()
        minTime=t.StrToDatetime(args.begin,timeformat)
        if not args.end:
            maxTime=datetime.datetime.now() #to now
        else:
            maxTime=t.StrToDatetime(args.end,timeformat)
        #print minTime,maxTime
        qHandle=session.nominalSchema().newQuery()
        runDict=lumiQueryAPI.runsByTimerange(qHandle,minTime,maxTime)#xrawdata
        session.transaction().commit()
        runList=runDict.keys()
        del qHandle
        runList.sort()
    else:
        print 'unsupported action ',args.action
        exit
    #print 'runList ',runList
    #print 'runDict ', runDict               
    fig=Figure(figsize=(7,4),dpi=100)
    m=matplotRender.matplotRender(fig)    
    if args.action == 'peakperday':
        l=lumiTime.lumiTime()
        #lumiperls=getLumiPerLS(session,c,runList,selectionDict,beamstatus='STABLE BEAMS',beamenergy=3.5e3,beamenergyfluctuation=0.09)
        lumiperls=getInstLumiPerLS(session,c,runList,selectionDict)
        #print 'lumiperls ',lumiperls 
        xdata=[]#[lsstarttime]
        ydata={}#{label:[instlumi]}
        ydata['Max Inst']=[]
        #ydata['Recorded']=[]
        for lsdata in lumiperls:
            runstarttimeStr=lsdata[-1]#note: it is a string!!
            startorbit=lsdata[-2]
            deliveredInst=lsdata[2]
            #recordedInst=lsdata[3]
            lsstarttime=l.OrbitToTime(runstarttimeStr,startorbit)
            xdata.append(lsstarttime)
            ydata['Max Inst'].append(deliveredInst)
            #ydata['Recorded'].append(recordedInst)
        m.plotPeakPerday_Time(xdata,ydata,minTime,maxTime)
    if args.action == 'run':
        runnumber=runList[0]
        lumiperrun=getLumiPerRun(session,c,runnumber)#[[lsnumber,deliveredInst,recordedInst,norbit,startorbit,runstarttime,runstoptime]]
        #print 'lumiperrun ',lumiperrun
        xdata=[]#[stattime,stoptime,totalls,ncmsls]
        ydata={}#{label:[instlumi]}
        ydata['Delivered']=[]
        ydata['Recorded']=[]
        starttime=lumiperrun[0][-2]
        stoptime=lumiperrun[0][-1]
        ncmsls=0
        totalls=len(lumiperrun)
        for lsdata in lumiperrun:
            lsnumber=lsdata[0]
            if lsnumber!=0:
                ncmsls+=1
            deliveredInst=lsdata[1]
            recordedInst=lsdata[2]
            ydata['Delivered'].append(deliveredInst)
            ydata['Recorded'].append(recordedInst)
        xdata=[runnumber,starttime,stoptime,totalls,ncmsls]
        print 'ydata ',ydata
        m.plotInst_RunLS(xdata,ydata)
    del session
    del svc
    if args.batch:
        m.drawPNG(args.batch)
    if args.interactive:
        m.drawInteractive()
    
if __name__=='__main__':
    main()