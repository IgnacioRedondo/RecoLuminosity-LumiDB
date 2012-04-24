#!/usr/bin/env python
import os,sys
from RecoLuminosity.LumiDB import dataDML,revisionDML,argparse,sessionManager,lumiReport

##############################
## ######################## ##
## ## ################## ## ##
## ## ## Main Program ## ## ##
## ## ################## ## ##
## ######################## ##
##############################


if __name__ == '__main__':
    parser=argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),description="Lumi Normalization factor",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    allowedActions=['add','list','createbranch']
    parser.add_argument('action',choices=allowedActions,help='command actions')
    parser.add_argument('-c',dest='connect',action='store',required=False,help='connect string to lumiDB,optional',default='frontier://LumiCalc/CMS_LUMI_PROD')
    parser.add_argument('-P',dest='authpath',action='store',help='path to authentication file,optional')
    parser.add_argument('--name',dest='name',action='store',help='correction name')
    parser.add_argument('--a1',dest='a1',action='store',type=float,required=False,help='a1 coeff')
    parser.add_argument('--a2',dest='a2',action='store',type=float,default=0.0,help='a2 coeff')
    parser.add_argument('--drift',dest='drift',action='store',type=float,default=0.0,help='drift coeff')
    parser.add_argument('--siteconfpath',dest='siteconfpath',action='store',help='specific path to site-local-config.xml file, optional. If path undefined, fallback to cern proxy&server')
    parser.add_argument('--debug',dest='debug',action='store_true',help='debug')
    options=parser.parse_args()
    if options.authpath:
        os.environ['CORAL_AUTH_PATH']=options.authpath
    #
    #pre-check
    #
    if options.action=='add':
        if not options.authpath:
            raise RuntimeError('argument -P authpath is required for add action')
        if not options.name:
            raise RuntimeError('argument --name name is required for add action')
        if not options.a1:
            raise RuntimeError('argument --a1 a1 is required for add action')
    svc=sessionManager.sessionManager(options.connect,authpath=options.authpath,siteconfpath=options.siteconfpath)
    session=None
    if options.action=='createbranch':
        dbsession=svc.openSession(isReadOnly=False,cpp2sqltype=[('unsigned int','NUMBER(10)'),('unsigned long long','NUMBER(20)')])      
        dbsession.transaction().start(False)
        (branchid,parentid,parentname)=revisionDML.createBranch(dbsession.nominalSchema(),'CORR','TRUNK','hold lumi correction coefficient')
        dbsession.transaction().commit()
    if options.action=='add':
        dbsession=svc.openSession(isReadOnly=False,cpp2sqltype=[('unsigned int','NUMBER(10)'),('unsigned long long','NUMBER(20)')])        
        #
        # add corr factor
        #
        dbsession.transaction().start(False)
        schema=dbsession.nominalSchema()
        (revision_id,branch_id)=revisionDML.branchInfoByName(schema,'CORR')
        optionalcorrdata={'a2':options.a2,'drift':options.drift}
        dataDML.addCorrToBranch(schema,options.name,options.a1,optionalcorrdata,(revision_id,'CORR'))
        dbsession.transaction().commit()
    elif options.action=='list':
        dbsession=svc.openSession(isReadOnly=True,cpp2sqltype=[('unsigned int','NUMBER(10)'),('unsigned long long','NUMBER(20)')])
        dbsession.transaction().start(True)
        schema=dbsession.nominalSchema()
        if options.name is None:
            branchfilter=revisionDML.revisionsInBranchName(schema,'CORR')
            allcorrs=dataDML.mostRecentLumicorrs(schema,branchfilter)
            lumiReport.toScreenCorr(allcorrs)
        elif options.name is not None:
            corrdataid=dataDML.guesscorrIdByName(schema,options.name)
            corr=dataDML.lumicorrById(schema,corrdataid)
            lumiReport.toScreenCorr(corr)
        dbsession.transaction().commit()
    del dbsession
    del svc
