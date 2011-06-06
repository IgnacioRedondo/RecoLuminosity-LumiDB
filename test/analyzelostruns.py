import csv,os,sys,coral,array
from RecoLuminosity.LumiDB import CommonUtil,idDealer,dbUtil,dataDML
conn='oracle://cms_orcoff_prep/cms_lumi_dev_offline'
beamenergy=3.5e03
beamstatus='STABLE BEAMS'
lumiversion='0001'
dtnorm=1.0
lhcnorm=1.0
cmsalive=0
numorbit=262144
startorbit=0
lslength=23.357
cmslsnum=0
def insertLumiSummarydata(dbsession,runnumber,perlsrawdata):
    '''
    input: perlsrawdata {cmslsnum:instlumi}
    insert into lumisummary(lumisummary_id,runnum,cmslsnum,lumilsnum,lumiversion,dtnorm,lhcnorm,instlumi,instlumierror,instlumiquality,cmsalive,startorbit,numorbit,lumisectionquality,beamenergy,beamstatus) values()
    '''
    summaryidlsmap={}
    dataDef=[]
    dataDef.append(('LUMISUMMARY_ID','unsigned long long'))
    dataDef.append(('RUNNUM','unsigned int'))
    dataDef.append(('CMSLSNUM','unsigned int'))
    dataDef.append(('LUMILSNUM','unsigned int'))
    dataDef.append(('LUMIVERSION','string'))
    dataDef.append(('DTNORM','float'))
    dataDef.append(('LHCNORM','float'))
    dataDef.append(('INSTLUMI','float'))
    dataDef.append(('INSTLUMIERROR','float'))
    dataDef.append(('INSTLUMIQUALITY','short'))
    dataDef.append(('CMSALIVE','short'))
    dataDef.append(('STARTORBIT','unsigned int'))
    dataDef.append(('NUMORBIT','unsigned int'))
    dataDef.append(('LUMISECTIONQUALITY','short'))
    dataDef.append(('BEAMENERGY','float'))
    dataDef.append(('BEAMSTATUS','string'))
    
    perlsiData=[]
    dbsession.transaction().start(False)
    iddealer=idDealer.idDealer(dbsession.nominalSchema())
    db=dbUtil.dbUtil(dbsession.nominalSchema())
    lumisummary_id=0
    for (lumilsnum,instlumi) in perlsrawdata:
        mystartorbit=startorbit+numorbit*(lumilsnum-1)
        lumisummary_id=iddealer.generateNextIDForTable('LUMISUMMARY')
        #print lumisummary_id
        summaryidlsmap[lumilsnum]=lumisummary_id
        perlsiData.append([('LUMISUMMARY_ID',lumisummary_id),('RUNNUM',int(runnumber)),('CMSLSNUM',cmslsnum),('LUMILSNUM',int(lumilsnum)),('LUMIVERSION',lumiversion),('DTNORM',dtnorm),('LHCNORM',lhcnorm),('INSTLUMI',instlumi),('INSTLUMIERROR',0.0),('CMSALIVE',cmsalive),('STARTORBIT',mystartorbit),('NUMORBIT',numorbit),('LUMISECTIONQUALITY',1),('BEAMENERGY',beamenergy),('BEAMSTATUS',beamstatus)])
    db.bulkInsert('LUMISUMMARY',dataDef,perlsiData)
    dbsession.transaction().commit()
    print 'lumisummary to insert : ',perlsiData
    #print 'summaryidlsmap ',summaryidlsmap
    #return summaryidlsmap
    
def insertLumiSummaryv2(dbsession,runnumber,perlsrawdata):
    '''
    input: perlsrawdata {cmslsnum:instlumi}
    update lumisummaryv2 set () where runnum=:runnumber
    '''
    data_id=0
    summaryidlsmap={}
    dataDef=[]
    dataDef.append(('DATA_ID','unsigned long long'))
    dataDef.append(('RUNNUM','unsigned int'))
    dataDef.append(('CMSLSNUM','unsigned int'))
    dataDef.append(('LUMILSNUM','unsigned int'))
    dataDef.append(('INSTLUMI','float'))
    dataDef.append(('INSTLUMIERROR','float'))
    dataDef.append(('INSTLUMIQUALITY','short'))
    dataDef.append(('BEAMSTATUS','string'))
    dataDef.append(('BEAMENERGY','float'))    
    dataDef.append(('NUMORBIT','unsigned int'))
    dataDef.append(('STARTORBIT','unsigned int'))
        
    perlsiData=[]
    dbsession.transaction().start(False)
    schema=dbsession.nominalSchema()
    db=dbUtil.dbUtil(schema)
    data_id=dataDML.guessLumiDataIdByRun(schema,int(runnumber))
    for (lumilsnum,instlumi) in perlsrawdata:
        mystartorbit=startorbit+numorbit*(lumilsnum-1)
        myinstlumi=instlumi/float(6370)
        perlsiData.append([('DATA_ID',data_id),('RUNNUM',int(runnumber)),('CMSLSNUM',cmslsnum),('LUMILSNUM',int(lumilsnum)),('INSTLUMI',myinstlumi),('INSTLUMIERROR',0.0),('INSTLUMIQUALITY',1),('BEAMSTATUS',beamstatus),('BEAMENERGY',beamenergy),('NUMORBIT',numorbit),('STARTORBIT',mystartorbit)])
    print 'lumisummaryv2 to insert : ',perlsiData
    db.bulkInsert('LUMISUMMARYV2',dataDef,perlsiData)
    dbsession.transaction().commit()

    
def parseLSFile(ifilename):
    result=[]
    try:
        csvfile=open(ifilename,'rb')
        reader=csv.reader(csvfile,skipinitialspace=True)
        for row in reader:
            if len(row)==2:
                ls=int(row[0])
                instlum=float(row[1])
                result.append((ls,instlum))
        return result
    except Exception,e:
        raise RuntimeError(str(e))
    
def main(*args):
    fname=args[1]
    runnum=fname.split('.')[0]
    perlsrawdata=parseLSFile(fname)
    #print perlsrawdata
    #print sum([x[1] for x in perlsrawdata])
    #print sum([x[1] for x in perlsrawdata])*23.35
    #perbunchrawdata=parsebunchFile(ibunchfilename)
    #print perbunchrawdata
    msg=coral.MessageStream('')
    msg.setMsgVerbosity(coral.message_Level_Error)
    os.environ['CORAL_AUTH_PATH']='/afs/cern.ch/user/x/xiezhen'
    svc = coral.ConnectionService()
    dbsession=svc.connect(conn,accessMode=coral.access_Update)
    if args[2] and args[2]=='--v2':
        insertLumiSummaryv2(dbsession,runnum,perlsrawdata)
    else:
        insertLumiSummarydata(dbsession,runnum,perlsrawdata)
    del dbsession
    del svc
if __name__=='__main__':
    sys.exit(main(*sys.argv))
