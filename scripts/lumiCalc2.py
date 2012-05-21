#!/usr/bin/env python

########################################################################
# Command to calculate luminosity from HF measurement stored in lumiDB #
#                                                                      #
# Author:      Zhen Xie                                                #
########################################################################

VERSION='2.00'
import os,sys,time
import coral
from RecoLuminosity.LumiDB import sessionManager,lumiTime,inputFilesetParser,csvSelectionParser,selectionParser,csvReporter,argparse,CommonUtil,revisionDML,lumiCalcAPI,lumiReport,RegexValidator,normDML
        
beamChoices=['PROTPHYS','IONPHYS','PAPHYS']

def parseInputFiles(inputfilename,dbrunlist,optaction):
    '''
    output ({run:[cmsls,cmsls,...]},[[resultlines]])
    '''
    selectedrunlsInDB={}
    resultlines=[]
    p=inputFilesetParser.inputFilesetParser(inputfilename)
    runlsbyfile=p.runsandls()
    selectedProcessedRuns=p.selectedRunsWithresult()
    selectedNonProcessedRuns=p.selectedRunsWithoutresult()
    resultlines=p.resultlines()
    for runinfile in selectedNonProcessedRuns:
        if runinfile not in dbrunlist:
            continue
        if optaction=='delivered':#for delivered we care only about selected runs
            selectedrunlsInDB[runinfile]=None
        else:
            selectedrunlsInDB[runinfile]=runlsbyfile[runinfile]
    return (selectedrunlsInDB,resultlines)

##############################
## ######################## ##
## ## ################## ## ##
## ## ## Main Program ## ## ##
## ## ################## ## ##
## ######################## ##
##############################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),description = "Lumi Calculation",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    allowedActions = ['overview', 'delivered', 'recorded', 'lumibyls','lumibylsXing']
    beamModeChoices = [ "stable", "quiet", "either"]
    amodetagChoices = [ "PROTPHYS","IONPHYS",'PAPHYS' ]
    xingAlgoChoices =[ "OCC1","OCC2","ET"]

    #
    # parse arguments
    #  
    ################################################
    # basic arguments
    ################################################
    parser.add_argument('action',choices=allowedActions,
                        help='command actions')
    parser.add_argument('-c',dest='connect',action='store',
                        required=False,
                        help='connect string to lumiDB,optional',
                        default='frontier://LumiCalc/CMS_LUMI_PROD')
    parser.add_argument('-P',dest='authpath',action='store',
                        required=False,
                        help='path to authentication file')
    parser.add_argument('-r',dest='runnumber',action='store',
                        type=int,
                        required=False,
                        help='run number')
    parser.add_argument('-o',dest='outputfile',action='store',
                        required=False,
                        help='output to csv file' )
    
    #################################################
    #arg to select exact run and ls
    #################################################
    parser.add_argument('-i',dest='inputfile',action='store',
                        required=False,
                        help='lumi range selection file')
    #################################################
    #arg to select exact hltpath or pattern
    #################################################
    parser.add_argument('--hltpath',dest='hltpath',action='store',
                        default=None,required=False,
                        help='specific hltpath or hltpath pattern to calculate the effectived luminosity')
    #################################################
    #versions control
    #################################################
    parser.add_argument('--correctiontag',dest='correctiontag',action='store',
                        required=False,
                        help='version of lumi correction coefficients')
    parser.add_argument('--datatag',dest='datatag',action='store',
                        required=False,
                        help='version of lumi/trg/hlt data')

    ###############################################
    # run filters
    ###############################################
    parser.add_argument('-f','--fill',dest='fillnum',action='store',
                        default=None,required=False,
                        help='fill number (optional) ')
    parser.add_argument('--amodetag',dest='amodetag',action='store',
                        choices=amodetagChoices,
                        required=False,
                        help='specific accelerator mode choices [PROTOPHYS,IONPHYS,PAPHYS] (optional)')
    parser.add_argument('--beamenergy',dest='beamenergy',action='store',
                        type=float,
                        default=None,
                        help='nominal beam energy in GeV')
    parser.add_argument('--beamfluctuation',dest='beamfluctuation',
                        type=float,action='store',
                        default=0.2,
                        required=False,
                        help='fluctuation in fraction allowed to nominal beam energy, default 0.2, to be used together with -beamenergy  (optional)')
                        
    parser.add_argument('--begin',dest='begin',action='store',
                        default=None,
                        required=False,
                        type=RegexValidator.RegexValidator("^\d\d/\d\d/\d\d \d\d:\d\d:\d\d$","must be form mm/dd/yy hh:mm:ss"),
                        help='min run start time, mm/dd/yy hh:mm:ss (optional)' )
                        
    parser.add_argument('--end',dest='end',action='store',
                        default=None,
                        required=False,
                        type=RegexValidator.RegexValidator("^\d\d/\d\d/\d\d \d\d:\d\d:\d\d$","must be form mm/dd/yy hh:mm:ss"),
                        help='max run start time, mm/dd/yy hh:mm:ss (optional)' )
                            
    #############################################
    #ls filter 
    #############################################
    parser.add_argument('-b',dest='beammode',action='store',
                        choices=beamModeChoices,
                        required=False,
                        help='beam mode choices [stable]')

    parser.add_argument('--xingMinLum', dest = 'xingMinLum',
                        type=float,
                        default=1e-03,
                        required=False,
                        help='Minimum luminosity considered for lumibylsXing action, default=1e-03')
    
    parser.add_argument('--xingAlgo', dest = 'xingAlgo',
                        default='OCC1',
                        required=False,
                        help='algorithm name for per-bunch lumi ')
    
    #############################################
    #global scale factor
    #############################################        
    parser.add_argument('-n',dest='scalefactor',action='store',
                        type=float,
                        default=1.0,
                        required=False,
                        help='user defined global scaling factor on displayed lumi values,optional')

    #################################################
    #command configuration 
    #################################################
    parser.add_argument('--siteconfpath',dest='siteconfpath',action='store',
                        default=None,
                        required=False,
                        help='specific path to site-local-config.xml file, optional. If path undefined, fallback to cern proxy&server')
    #################################################
    #switches
    #################################################
    parser.add_argument('--without-correction',dest='withoutFineCorrection',action='store_true',
                        help='without any correction/calibration' )
    parser.add_argument('--without-checkforupdate',dest='withoutCheckforupdate',action='store_true',
                        help='without check for update' )                    
    parser.add_argument('--verbose',dest='verbose',action='store_true',
                        help='verbose mode for printing' )
    parser.add_argument('--nowarning',dest='nowarning',action='store_true',
                        help='suppress bad for lumi warnings' )
    parser.add_argument('--debug',dest='debug',action='store_true',
                        help='debug')

    options=parser.parse_args()
    #
    # check working environment
    #
    workingversion='UNKNOWN'
    updateversion='NONE'
    thiscmmd=sys.argv[0]
    if not options.withoutCheckforupdate:
        from RecoLuminosity.LumiDB import checkforupdate
        cmsswWorkingBase=os.environ['CMSSW_BASE']
        if not cmsswWorkingBase:
            print 'Please check out RecoLuminosity/LumiDB from CVS,scram b,cmsenv'
            sys.exit(0)
        c=checkforupdate.checkforupdate()
        workingversion=c.runningVersion(cmsswWorkingBase,'lumiCalc2.py',isverbose=False)
        if workingversion:
            updateversionList=c.checkforupdate(workingversion,isverbose=False)
            if updateversionList:
                updateversion='#'.join(updateversionList)
    #
    # check DB environment
    #
    if options.authpath:
        os.environ['CORAL_AUTH_PATH'] = options.authpath
    svc=sessionManager.sessionManager(options.connect,
                                      authpath=options.authpath,
                                      siteconfpath=options.siteconfpath,
                                      debugON=options.debug)
    session=svc.openSession(isReadOnly=True,cpp2sqltype=[('unsigned int','NUMBER(10)'),('unsigned long long','NUMBER(20)')])
    
        
    #
    # check datatag
    #
    datatagname=options.datatag
    if not datatagname:
        session.transaction().start(True)
        (datatagid,datatagname)=revisionDML.currentDataTag(session.nominalSchema())
        session.transaction().commit()
    
    #
    # check correctiontag
    #
    session.transaction().start(True)
    normname=options.correctiontag
    normid=0
    if not normname:
        normmap=normDML.normIdByType(session.nominalSchema(),lumitype='HF',defaultonly=True)
        if len(normmap):
            normname=normmap.keys()[0]
            normid=normmap[normname]
    else:
        normid=normDML.normIdByname(session.nominalSchema(),lumitype='HF',defaultonly=False)
    session.transaction().commit()
    lumiReport.toScreenHeader(thiscmmd,datatagname,normname,workingversion,updateversion)
    sys.exit(0)
        
    pbeammode = None
    normfactor=options.normfactor
    if options.beammode=='stable':
        pbeammode    = 'STABLE BEAMS'
    if options.verbose:
        print 'General configuration'
        print '\tconnect: ',options.connect
        print '\tauthpath: ',options.authpath
        print '\tcorrection tag: ',options.correctiontag
        print '\tlumi data tag: ',options.datatag
        print '\tsiteconfpath: ',options.siteconfpath
        print '\toutputfile: ',options.outputfile
        print '\tscalefactor: ',options.scalefactor        
        if options.action=='recorded' and options.hltpath:
            print 'Action: effective luminosity in hltpath: ',options.hltpath
        else:
            print 'Action: ',options.action
        if options.normfactor:
            if CommonUtil.is_floatstr(normfactor):
                print '\tuse norm factor value ',normfactor                
            else:
                print '\tuse specific norm factor name ',normfactor
        else:
            print '\tuse norm factor in context (amodetag,beamenergy)'
        if options.runnumber: # if runnumber specified, do not go through other run selection criteria
            print '\tselect specific run== ',options.runnumber
        else:
            print '\trun selections == '
            print '\tinput selection file: ',options.inputfile
            print '\tbeam mode: ',options.beammode
            print '\tfill: ',options.fillnum
            print '\tamodetag: ',options.amodetag
            print '\tbegin: ',options.begin
            print '\tend: ',options.end
            print '\tbeamenergy: ',options.beamenergy 
            if options.beamenergy:
                print '\tbeam energy: ',str(options.beamenergy)+'+/-'+str(options.beamfluctuation*options.beamenergy)+'(GeV)'
        if options.action=='lumibylsXing':
            print '\tLS filter for lumibylsXing xingMinLum: ',options.xingMinLum
        

 
    irunlsdict={}
    iresults=[]
    reqTrg=False
    reqHlt=False
    if options.action=='overview' or options.action=='lumibyls' or options.action=='lumibylsXing':
        reqTrg=True
    if options.action=='recorded':
        reqTrg=True
        reqHlt=True
        
    session.transaction().start(True)
    schema=session.nominalSchema()
    if options.runnumber: # if runnumber specified, do not go through other run selection criteria
        irunlsdict[options.runnumber]=None
    else:
        runlist=lumiCalcAPI.runList(schema,options.fillnum,runmin=None,runmax=None,startT=options.begin,stopT=options.end,l1keyPattern=None,hltkeyPattern=None,amodetag=options.amodetag,nominalEnergy=options.beamenergy,energyFlut=options.beamfluctuation,requiretrg=reqTrg,requirehlt=reqHlt)

        if options.inputfile:
            (irunlsdict,iresults)=parseInputFiles(options.inputfile,runlist,options.action)
        else:
            for run in runlist:
                irunlsdict[run]=None
    if options.verbose:
        print 'Selected run:ls'
        for run in sorted(irunlsdict):
            if irunlsdict[run] is not None:
                print '\t%d : %s'%(run,','.join([str(ls) for ls in irunlsdict[run]]))
            else:
                print '\t%d : all'%run
                
    ##################
    # run level      #
    ##################
    #resolve data/correction/norm versions, if not specified use default or guess
    normmap={}       #{run:(norm1,occ2norm,etnorm,punorm,constfactor)}
    correctionCoeffMap={} #{name:(alpha1,alpha2,drift)}just coefficient, not including drift intglumi
    datatagidMap={}  #{run:(lumiid,trgid,hltid)}
    rruns=irunlsdict.keys()
    print 'rruns ',rruns
    GrunsummaryData=lumiCalcAPI.runsummaryMap(schema,irunlsdict)
    if len(GrunsummaryData)==0:
        print 'required runs not found in db,do nothing'
        session.transaction().commit()
        del session
        del svc
        sys.exit(-1)
    if not normfactor:#if no specific norm,decide from context
        runcontextMap={}
        for rdata in sorted(GrunsummaryData):
            mymodetag=GrunsummaryData[rdata][1]
            myegev=GrunsummaryData[rdata][2]
            runcontextMap[rdata]=(mymodetag,myegev)
            normmap=lumiCalcAPI.normForRange(schema,runcontextMap)            
    else:
        normvalue=lumiCalcAPI.normByName(schema,normfactor)
        normmap=dict.fromkeys(rruns,normvalue)
    if not options.withoutFineCorrection:
        correctionCoeffs=lumiCalcAPI.correctionByName(schema,tagname=options.correctiontag)
        driftcoeff=0.0
        driftcorrectionMap=lumiCalcAPI.driftCorrectionForRange(schema,rruns,driftcoeff)
    dataidmap={}     #{run:(lumiid,trgid,hltid)}
    currenttagname=datatagname
    if not datatagname:
        print 'rruns ',rruns
        (currenttagname,dataidmap)=revisionDML.dataIdsByTagId(schema,currenttagid,runlist=rruns,withcomment=False)
    else:
        dataidmap=revisionDML.dataIdsByTagName(schema,datatagname,runlist=rruns,withcomment=False)

    ##################
    # ls level       #
    ##################
    if options.action == 'delivered':
        result=lumiCalcAPI.deliveredLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=GrunsummaryData,beamstatusfilter=pbeammode,normmap=normmap,correctioncoeffs=correctionCoeffs,lumitype='HF')
        if not options.outputfile:
            lumiReport.toScreenTotDelivered(result,iresults,options.scalefactor,options.verbose)
        else:
            lumiReport.toCSVTotDelivered(result,options.outputfile,iresults,options.scalefactor,options.verbose)
    if options.action == 'overview':
        result=lumiCalcAPI.lumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=GrunsummaryData,beamstatusfilter=pbeammode,normmap=normmap,correctioncoeffs=correctionCoeffs,lumitype='HF')
        if not options.outputfile:
            lumiReport.toScreenOverview(result,iresults,options.scalefactor,options.verbose)
        else:
            lumiReport.toCSVOverview(result,options.outputfile,iresults,options.scalefactor,options.verbose)
    if options.action == 'lumibyls':
        if not options.hltpath:
            result=lumiCalcAPI.lumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=GrunsummaryData,beamstatusfilter=pbeammode,normmap=normmap,correctioncoeffs=correctionCoeffs,lumitype='HF')
            if not options.outputfile:
                lumiReport.toScreenLumiByLS(result,iresults,options.scalefactor,options.verbose)
            else:
                lumiReport.toCSVLumiByLS(result,options.outputfile,iresults,options.scalefactor,options.verbose)
        else:
            hltname=options.hltpath
            hltpat=None
            if hltname=='*' or hltname=='all':
                hltname=None
            elif 1 in [c in hltname for c in '*?[]']: #is a fnmatch pattern
                hltpat=hltname
                hltname=None
            result=lumiCalcAPI.effectiveLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=GrunsummaryData,beamstatusfilter=pbeammode,normmap=normmap,correctioncoeffs=correctionCoeffs,hltpathname=hltname,hltpathpattern=hltpat,withBXInfo=False,bxAlgo=None,xingMinLum=options.xingMinLum,withBeamIntensity=False,lumitype='HF',datatag=None)
            if not options.outputfile:
                lumiReport.toScreenLSEffective(result,iresults,options.scalefactor,options.verbose)
            else:
                lumiReport.toCSVLSEffective(result,options.outputfile,iresults,options.scalefactor,options.verbose)
    if options.action == 'recorded':#recorded actually means effective because it needs to show all the hltpaths...
        hltname=options.hltpath
        hltpat=None
        if hltname is not None:
            if hltname=='*' or hltname=='all':
                hltname=None
            elif 1 in [c in hltname for c in '*?[]']: #is a fnmatch pattern
                hltpat=hltname
                hltname=None
        result=lumiCalcAPI.effectiveLumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=GrunsummaryData,beamstatusfilter=pbeammode,normmap=normmap,correctioncoeffs=correctionCoeffs,hltpathname=hltname,hltpathpattern=hltpat,withBXInfo=False,bxAlgo=None,xingMinLum=options.xingMinLum,withBeamIntensity=False,lumitype='HF',datatag=None)
        if not options.outputfile:
            lumiReport.toScreenTotEffective(result,iresults,options.scalefactor,options.verbose)
        else:
            lumiReport.toCSVTotEffective(result,options.outputfile,iresults,options.scalefactor,options.verbose)
    if options.action == 'lumibylsXing':
        result=lumiCalcAPI.lumiForIds(schema,irunlsdict,dataidmap,runsummaryMap=GrunsummaryData,beamstatusfilter=pbeammode,normmap=normmap,correctioncoeffs=correctionCoeffs,lumitype='HF')
        if not options.outputfile:
            lumiReport.toScreenLumiByLS(result,iresults,options.scalefactor,options.verbose)
        else:
            lumiReport.toCSVLumiByLSXing(result,options.scalefactor,options.outputfile)
    session.transaction().commit()
    del session
    del svc 
