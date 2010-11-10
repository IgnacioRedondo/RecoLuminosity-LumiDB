#!/usr/bin/env python
VERSION='1.02'
import os,sys,array
import coral
from RecoLuminosity.LumiDB import argparse,idDealer,CommonUtil,dbUtil

class newSchemaNames():
    def __init__(self):
        self.revisiontable='REVISIONS'
        self.luminormtable='LUMINORMS'
        self.lumidatatable='LUMIDATA'
        self.lumisummarytable='LUMISUMMARY'
        self.runsummarytable='CMSRUNSUMMARY'
        self.lumidetailtable='LUMIDETAIL'
        self.trgdatatable='TRGDATA'
        self.lstrgtable='LSTRG'
        self.hltdatatable='HLTDATA'
        self.lshlttable='LSHLT'
        self.trghltmaptable='TRGHLTMAP'
        self.validationtable='LUMIVALIDATION'
    def idtablename(self,datatablename):
        return datatablename.upper()+'_ID'
    def entrytablename(self,datatablename):
        return datatablename.upper()+'_ENTRIES'
    def revtablename(self,datatablename):
        return datatablename.upper()+'_REV'
    
class oldSchemaNames(object):
    def __init__(self):
        self.lumisummarytable='LUMISUMMARY'
        self.lumidetailtable='LUMIDETAIL'
        self.runsummarytable='CMSRUNSUMMARY'
        self.trgtable='TRG'
        self.hlttable='HLT'
        self.trghltmaptable='TRGHLTMAP'
        
def isOldSchema(dbsession):
    '''
    if there is no lumidata table, then it is old schema
    '''
    n=newSchemaNames()
    result=False
    dbsession.transaction().start(True)
    db=dbUtil.dbUtil(dbsession.nominalSchema())
    result=db.tableExists(n.lumidatatable)
    dbsession.transaction().commit()
    return not result

def createNewTables(dbsession):
    '''
    create new tables if not exist
    revisions,revisions_id,luminorms,luminorms_entries,luminorms_id,
    '''
    n=newSchemaNames()
    try:        
        dbsession.transaction().start(False)
        dbsession.typeConverter().setSqlTypeForCppType('NUMBER(10)','unsigned int')
        dbsession.typeConverter().setSqlTypeForCppType('NUMBER(20)','unsigned long long')
        schema=dbsession.nominalSchema()
        db=dbUtil.dbUtil(schema)
        lumidataTab=coral.TableDescription()
        lumidataTab.setName( n.lumidatatable )
        lumidataTab.insertColumn( 'LUMIDATA_ID','unsigned long long')
        lumidataTab.insertColumn( 'ENTRY_ID','unsigned long long')
        lumidataTab.insertColumn( 'SOURCE', 'string')
        lumidataTab.insertColumn( 'RUNNUM', 'unsigned int')
        lumidataTab.setPrimaryKey( 'LUMIDATA_ID' )
        db.createTable(lumidataTab,withIdTable=True)
        trgdataTab=coral.TableDescription()
        trgdataTab.setName( n.trgdatatable )
        trgdataTab.insertColumn( 'TRGDATA_ID','unsigned long long')
        trgdataTab.insertColumn( 'ENTRY_ID','unsigned long long')
        trgdataTab.insertColumn( 'SOURCE', 'string')
        trgdataTab.insertColumn( 'RUNNUM', 'unsigned int')
        trgdataTab.insertColumn( 'BITZERONAME', 'string')
        trgdataTab.insertColumn( 'BITNAMECLOB', 'string',6000)
        trgdataTab.setPrimaryKey( 'TRGDATA_ID' )
        db.createTable(trgdataTab,withIdTable=True)
        hltdataTab=coral.TableDescription()
        hltdataTab.setName( n.hltdatatable )
        hltdataTab.insertColumn( 'HLTDATA_ID','unsigned long long')
        hltdataTab.insertColumn( 'ENTRY_ID','unsigned long long')
        hltdataTab.insertColumn( 'RUNNUM', 'unsigned int')
        hltdataTab.insertColumn( 'SOURCE', 'string')
        hltdataTab.insertColumn( 'NPATH', 'unsigned int')
        hltdataTab.insertColumn( 'PATHNAMECLOB', 'string',6000)
        hltdataTab.setPrimaryKey( 'HLTDATA_ID' )
        db.createTable(hltdataTab,withIdTable=True)
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.createNewTables:'+str(e))
    

def modifyOldTables(dbsession):
    '''
    modify old tables:lumisummary,lumidetail
    alter table lumisummary add column(lumidata_id unsigned long long)
    alter table lumidetail add column(lumidata_id unsigned long long,runnum unsigned int,cmslsnum unsigned int)
    '''
    n=newSchemaNames()
    try:
        dbsession.transaction().start(False)
        dbsession.typeConverter().setCppTypeForSqlType('unsigned int','NUMBER(10)')
        dbsession.typeConverter().setCppTypeForSqlType('unsigned long long','NUMBER(20)')
        tableHandle=dbsession.nominalSchema().tableHandle(n.lumisummarytable)
        tableHandle.schemaEditor().insertColumn('LUMIDATA_ID','unsigned long long')
        tableHandle=dbsession.nominalSchema().tableHandle(n.lumidetailtable)
        tableHandle.schemaEditor().insertColumn('LUMIDATA_ID','unsigned long long')
        tableHandle.schemaEditor().insertColumn('RUNNUM','unsigned int')
        tableHandle.schemaEditor().insertColumn('CMSLSNUM','unsigned int')
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.modifyOldTables:'+str(e))
    
def dropNewTables(dbsession):
    n=newSchemaNames()
    try:
        dbsession.transaction().start(False)
        schema=dbsession.nominalSchema()
        db=dbUtil.dbUtil(schema)
        db.dropTable( n.lumidatatable )
        db.dropTable( n.lumidatatable+'_ID' )
        db.dropTable( n.trgdatatable )
        db.dropTable( n.trgdatatable+'_ID' )
        db.dropTable( n.hltdatatable )
        db.dropTable( n.hltdatatable+'_ID' )
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.dropNewTables:'+str(e))
    
def restoreOldTables(dbsession):
    n=newSchemaNames()
    try:
        dbsession.transaction().start(False)
        schema=dbsession.nominalSchema()
        dbsession.transaction().start(False)
        tableHandle=dbsession.nominalSchema().tableHandle(n.lumisummarytable)
        tableHandle.schemaEditor().dropColumn('LUMIDATA_ID')
        tableHandle=dbsession.nominalSchema().tableHandle(n.lumidetailtable)
        tableHandle.schemaEditor().dropColumn('LUMIDATA_ID')
        tableHandle.schemaEditor().dropColumn('RUNNUM')
        tableHandle.schemaEditor().dropColumn('CMSLSNUM')
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.restoreOldTables:'+str(e))
    
def createNewSchema(dbsession):
    '''
    create extra new tables+old unchanged tables
    '''
    createNewTables(dbsession)
    modifyOldTables(dbsession)

def dropNewSchema(dbsession):
    '''
    drop extra new tables+undo column changes
    '''
    dropNewTables(dbsession)
    restoreOldTables(dbsession)
    
def createIndices(dbsession):
    '''
    '''
    pass

def createEntry():
    pass
def createRevision():
    pass
def createBranch():
    pass

def getOldTrgData(dbsession,runnum):
    '''
    generate new trgdata_id for trgdata
    select cmslsnum,deadtime,bitname,trgcount,prescale from trg where runnum=:runnum and bitnum=0 order by cmslsnum;
    select cmslsnum,bitnum,trgcount,deadtime,prescale from trg where runnum=:runnum order by cmslsnum
    output [bitnames,databuffer]
    '''
    bitnames=''
    databuffer={} #{cmslsnum:[deadtime,bitzeroname,bitzerocount,bitzeroprescale,trgcountBlob,trgprescaleBlob]}
    dbsession.typeConverter().setCppTypeForSqlType('unsigned int','NUMBER(10)')
    dbsession.typeConverter().setCppTypeForSqlType('unsigned long long','NUMBER(20)')
    try:
        dbsession.transaction().start(True)
        qHandle=dbsession.nominalSchema().newQuery()
        n=oldSchemaNames()
        qHandle.addToTableList(n.trgtable)
        qHandle.addToOutputList('CMSLSNUM','cmslsnum')
        qHandle.addToOutputList('DEADTIME','deadtime')
        qHandle.addToOutputList('BITNAME','bitname')
        qHandle.addToOutputList('TRGCOUNT','trgcount')
        qHandle.addToOutputList('PRESCALE','prescale')
        qCondition=coral.AttributeList()
        qCondition.extend('runnum','unsigned int')
        qCondition.extend('bitnum','unsigned int')
        qCondition['runnum'].setData(int(runnum))
        qCondition['bitnum'].setData(int(0))
        qResult=coral.AttributeList()
        qResult.extend('cmslsnum','unsigned int')
        qResult.extend('deadtime','unsigned long long')
        qResult.extend('bitname','string')
        qResult.extend('trgcount','unsigned int')
        qResult.extend('prescale','unsigned int')
        qHandle.defineOutput(qResult)
        qHandle.setCondition('RUNNUM=:runnum AND BITNUM=:bitnum',qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            deadtime=cursor.currentRow()['deadtime'].data()
            bitname=cursor.currentRow()['bitname'].data()
            bitcount=cursor.currentRow()['trgcount'].data()
            prescale=cursor.currentRow()['prescale'].data()
            if not databuffer.has_key(cmslsnum):
                databuffer[cmslsnum]=[]
            databuffer[cmslsnum].append(deadtime)
            databuffer[cmslsnum].append(bitname)
            databuffer[cmslsnum].append(bitcount)
            databuffer[cmslsnum].append(prescale)
        del qHandle
        qHandle=dbsession.nominalSchema().newQuery()
        qHandle.addToTableList(n.trgtable)
        qHandle.addToOutputList('CMSLSNUM','cmslsnum')
        qHandle.addToOutputList('BITNUM','bitnum')
        qHandle.addToOutputList('BITNAME','bitname')
        qHandle.addToOutputList('TRGCOUNT','trgcount')
        qHandle.addToOutputList('PRESCALE','prescale')
        qCondition=coral.AttributeList()
        qCondition.extend('runnum','unsigned int')
        qCondition['runnum'].setData(int(runnum))
        qHandle.setCondition('RUNNUM=:runnum',qCondition)
        qHandle.addToOrderList('cmslsnum')
        qHandle.addToOrderList('bitnum')
        qResult=coral.AttributeList()
        qResult.extend('cmslsnum','unsigned int')
        qResult.extend('bitnum','unsigned int')
        qResult.extend('bitname','string')
        qResult.extend('trgcount','unsigned int')
        qResult.extend('prescale','unsigned int')
        qHandle.defineOutput(qResult)
        cursor=qHandle.execute()
        bitnameList=[]
        trgcountArray=array.array('l')
        prescaleArray=array.array('l')
        counter=0
        previouscmslsnum=0
        cmslsnum=-1
        while cursor.next():
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            bitnum=cursor.currentRow()['bitnum'].data()
            bitname=cursor.currentRow()['bitname'].data()
            trgcount=cursor.currentRow()['trgcount'].data()        
            prescale=cursor.currentRow()['prescale'].data()
            
            if bitnum==0 and counter!=0:
                trgcountBlob=CommonUtil.packArraytoBlob(trgcountArray)
                prescaleBlob=CommonUtil.packArraytoBlob(prescaleArray)
                #databuffer[previouslsnum].append(bitnames)
                databuffer[previouslsnum].append(trgcountBlob)
                databuffer[previouslsnum].append(prescaleBlob)
                bitnameList=[]
                trgcountArray=array.array('l')
                prescaleArray=array.array('l')
            else:
                previouslsnum=cmslsnum
            bitnameList.append(bitname)
            trgcountArray.append(trgcount)
            prescaleArray.append(prescale)
            counter+=1
        if cmslsnum>0:
            bitnames=','.join(bitnameList)
            trgcountBlob=CommonUtil.packArraytoBlob(trgcountArray)
            prescaleBlob=CommonUtil.packArraytoBlob(prescaleArray)

            databuffer[cmslsnum].append(trgcountBlob)
            databuffer[cmslsnum].append(prescaleBlob)
        #print CommonUtil.unpackBlobtoArray(databuffer[377][4],'l')
        del qHandle
        dbsession.transaction().commit()
        return [bitnames,databuffer]
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.getOldTrgData:'+str(e))
    
def transfertrgData(dbsession,runnumber,trgrawdata):
    '''
    input: trgdata [bitnames,databuffer], databuffer {cmslsnum:[deadtime,bitzeroname,bitzerocount,bitzeroprescale,trgcountBlob,trgprescaleBlob]}
    '''
    n=newSchemaNames()
    m=oldSchemaNames()
    bulkvalues=[]
    bitzeroname=trgrawdata[0].split(',')[0]
    perlsrawdatadict=trgrawdata[1]
    try:
        dbsession.transaction().start(False)
        iddealer=idDealer.idDealer(dbsession.nominalSchema())
        iddealer.generateNextIDForTable(n.trgdatatable)
        trgdata_id=iddealer.getIDforTable(n.trgdatatable)
        tabrowDefDict={'TRGDATA_ID':'unsigned long long','RUNNUM':'unsigned int','BITZERONAME':'string','BITNAMECLOB':'string'}
        tabrowValueDict={'TRGDATA_ID':trgdata_id,'RUNNUM':int(runnumber),'BITZERONAME':bitzeroname,'BITNAMECLOB':trgrawdata[0]}
        db=dbUtil.dbUtil(dbsession.nominalSchema())
        db.insertOneRow(n.trgdatatable,tabrowDefDict,tabrowValueDict)
        lstrgDefDict={'TRGDATA_ID':'unsigned long long','RUNNUM':'unsigned int','CMSLSNUM':'unsigned int','DEADTIMECOUNT':'unsigned long long','BITZEROCOUNT':'unsigned int','BITZEROPRESCALE':'unsigned int','PRESCALEBLOB':'blob','TRGCOUNTBLOB':'blob'}
        for cmslsnum,perlstrg in perlsrawdatadict.items():
            deadtimecount=perlstrg[0]
            bitzeroname=perlstrg[1]
            bitzerocount=perlstrg[2]
            bitzeroprescale=perlstrg[3]
            trgcountblob=perlstrg[4]
            trgprescaleblob=perlstrg[5]
            bulkvalues.append([('TRGDATA_ID',trgdata_id),('RUNNUM',runnumber),('CMSLSNUM',cmslsnum),('DEADTIMECOUNT',deadtimecount),('BITZEROCOUNT',bitzerocount),('BITZEROPRESCALE',bitzeroprescale),('PRESCALEBLOB',trgprescaleblob),('TRGCOUNTBLOB',trgcountblob)])
        db.bulkInsert(n.lstrgtable,lstrgDefDict,bulkvalues)
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.transfertrgData:'+str(e))

def transferhltData(dbsession,runnumber,hltrawdata):
    '''
    input: hltdata[pathnames,databuffer] #databuffer{cmslsnum:[inputcountBlob,acceptcountBlob,prescaleBlob]}
    '''
    n=newSchemaNames()
    m=oldSchemaNames()
    npath=len(hltrawdata[0].split(','))
    pathnames=hltrawdata[0]
    perlsrawdatadict=hltrawdata[1]
    bulkvalues=[]
    try:
        dbsession.transaction().start(False)
        iddealer=idDealer.idDealer(dbsession.nominalSchema())
        iddealer.generateNextIDForTable(n.hltdatatable)
        hltdata_id=iddealer.getIDforTable(n.hltdatatable)
        tabrowDefDict={'HLTDATA_ID':'unsigned long long','RUNNUM':'unsigned int','NPATH':'unsigned int','PATHNAMECLOB':'string'}
        tabrowValueDict={'HLTDATA_ID':hltdata_id,'RUNNUM':int(runnumber),'NPATH':npath,'PATHNAMECLOB':pathnames}
        db=dbUtil.dbUtil(dbsession.nominalSchema())
        db.insertOneRow(n.hltdatatable,tabrowDefDict,tabrowValueDict)
        lshltDefDict={'HLTDATA_ID':'unsigned long long','RUNNUM':'unsigned int','CMSLSNUM':'unsigned int','PRESCALEBLOB':'blob','HLTCOUNTBLOB':'blob','HLTACCEPTBLOB':'blob'}
        for cmslsnum,perlshlt in perlsrawdatadict.items():
            inputcountblob=perlshlt[0]
            acceptcountblob=perlshlt[1]
            prescaleblob=perlshlt[2]
            bulkvalues.append([('HLTDATA_ID',hltdata_id),('RUNNUM',runnumber),('CMSLSNUM',cmslsnum),('PRESCALEBLOB',prescaleblob),('HLTCOUNTBLOB',inputcountblob),('HLTACCEPTBLOB',acceptcountblob),('PRESCALEBLOB',prescaleblob)])
        db.bulkInsert(n.lshlttable,lshltDefDict,bulkvalues)
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.transferhltData:'+str(e))
    
def transferLumiData(dbsession,runnum):
    '''
    select LUMISUMMARY_ID as lumisummary_id,CMSLSNUM as cmslsnum from LUMISUMMARY where RUNNUM=:runnum order by cmslsnum
    generate new lumidata_id for lumidata
    insert into lumidata_id , runnum into lumidata
    insert into lumidata_id into lumisummary
    insert into lumidata_id into lumidetail
    '''
    n=newSchemaNames()
    m=oldSchemaNames()
    lumisummarydata=[]
    try:
        dbsession.transaction().start(True)
        #find lumi_summaryid of given run
        qHandle=dbsession.nominalSchema().newQuery()
        qHandle.addToTableList(n.lumisummarytable)
        qHandle.addToOutputList('LUMISUMMARY_ID','lumisummary_id')
        qHandle.addToOutputList('CMSLSNUM','cmslsnum')
        qCondition=coral.AttributeList()
        qCondition.extend('runnum','unsigned int')
        qCondition['runnum'].setData(int(runnum))
        qResult=coral.AttributeList()
        qResult.extend('lumisummary_id','unsigned long long')
        qResult.extend('cmslsnum','unsigned int')
        qHandle.defineOutput(qResult)
        qHandle.setCondition('RUNNUM=:runnum',qCondition)
        qHandle.addToOrderList('cmslsnum')
        cursor=qHandle.execute()
        while cursor.next():
            lumisummary_id=cursor.currentRow()['lumisummary_id'].data()
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            lumisummarydata.append((lumisummary_id,cmslsnum))
        del qHandle
        dbsession.transaction().commit()
        
        dbsession.transaction().start(False)
        iddealer=idDealer.idDealer(dbsession.nominalSchema())
        iddealer.generateNextIDForTable(n.lumidatatable)
        lumidata_id=iddealer.getIDforTable(n.lumidatatable)
        #insert in lumidata table
        print 'insert in lumidata table'
        tabrowDefDict={'LUMIDATA_ID':'unsigned long long','RUNNUM':'unsigned int'}
        tabrowValueDict={'LUMIDATA_ID':lumidata_id,'RUNNUM':int(runnum)}
        db=dbUtil.dbUtil(dbsession.nominalSchema())
        db.insertOneRow(n.lumidatatable,tabrowDefDict,tabrowValueDict)
        
        #update in lumisummary table
        print 'insert in lumisummary table'
        #updateAction='LUMIDATA_ID=:lumidata_id'
        setClause='LUMIDATA_ID=:lumidata_id'
        updateCondition='RUNNUM=:runnum'
        updateData=coral.AttributeList()
        updateData.extend('lumidata_id','unsigned long long')
        updateData.extend('runnum','unsigned int')
        updateData['lumidata_id'].setData(lumidata_id)
        updateData['runnum'].setData(int(runnum))
        nrows=db.singleUpdate(n.lumisummarytable,setClause,updateCondition,updateData)
        #updates in lumidetail table
        for (lumisummary_id,cmslsnum) in lumisummarydata:
            print 'update to lumidata_id,lumisummary_id,cmslsnum ',lumidata_id,lumisummary_id,cmslsnum
            updateAction='LUMIDATA_ID=:lumidata_id,RUNNUM=:runnum,CMSLSNUM=:cmslsnum'
            updateCondition='LUMISUMMARY_ID=:lumisummary_id'
            inputData=coral.AttributeList()
            inputData.extend('lumidata_id','unsigned long long')
            inputData.extend('runnum','unsigned int')
            inputData.extend('cmslsnum','unsigned int')
            inputData.extend('lumisummary_id','unsigned long long')
            inputData['lumidata_id'].setData(lumidata_id)
            inputData['runnum'].setData(int(runnum))
            inputData['cmslsnum'].setData(cmslsnum)
            inputData['lumisummary_id'].setData(lumisummary_id)           
            nupdates=db.singleUpdate(n.lumidetailtable,updateAction,updateCondition,inputData)
            print 'nupdates ',nupdates
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise RuntimeError('migrateSchema.transferLumiData:'+str(e))
    return lumidata_id

def getOldHLTData(dbsession,runnum):
    '''
    select count(distinct pathname) from hlt where runnum=:runnum
    select cmslsnum,pathname,inputcount,acceptcount,prescale from hlt where runnum=:runnum order by cmslsnum,pathname
    [pathnames,databuffer]
    '''
    
    databuffer={} #{cmslsnum:[inputcountBlob,acceptcountBlob,prescaleBlob]}
    dbsession.typeConverter().setCppTypeForSqlType('unsigned int','NUMBER(10)')
    dbsession.typeConverter().setCppTypeForSqlType('unsigned long long','NUMBER(20)')
    pathnames=''
    try:
        npath=0
        dbsession.transaction().start(True)
        qHandle=dbsession.nominalSchema().newQuery()
        n=oldSchemaNames()
        qHandle.addToTableList(n.hlttable)
        qHandle.addToOutputList('COUNT(DISTINCT PATHNAME)','npath')
        qCondition=coral.AttributeList()
        qCondition.extend('runnum','unsigned int')
        qCondition['runnum'].setData(int(runnum))
        qResult=coral.AttributeList()
        qResult.extend('npath','unsigned int')
        qHandle.defineOutput(qResult)
        qHandle.setCondition('RUNNUM=:runnum',qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            npath=cursor.currentRow()['npath'].data()
        del qHandle
        #print 'npath ',npath
        dbsession.transaction().start(True)
        qHandle=dbsession.nominalSchema().newQuery()
        n=oldSchemaNames()
        qHandle.addToTableList(n.hlttable)
        qHandle.addToOutputList('CMSLSNUM','cmslsnum')
        qHandle.addToOutputList('PATHNAME','pathname')
        qHandle.addToOutputList('INPUTCOUNT','inputcount')
        qHandle.addToOutputList('ACCEPTCOUNT','acceptcount')
        qHandle.addToOutputList('PRESCALE','prescale')
        qCondition=coral.AttributeList()
        qCondition.extend('runnum','unsigned int')
        qCondition['runnum'].setData(int(runnum))
        qResult=coral.AttributeList()
        qResult.extend('cmslsnum','unsigned int')
        qResult.extend('pathname','string')
        qResult.extend('inputcount','unsigned int')
        qResult.extend('acceptcount','unsigned int')
        qResult.extend('prescale','unsigned int')
        qHandle.defineOutput(qResult)
        qHandle.setCondition('RUNNUM=:runnum',qCondition)
        qHandle.addToOrderList('cmslsnum')
        qHandle.addToOrderList('pathname')
        cursor=qHandle.execute()
        pathnameList=[]
        inputcountArray=array.array('l')
        acceptcountArray=array.array('l')
        prescaleArray=array.array('l')
        ipath=0
        while cursor.next():
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            pathname=cursor.currentRow()['pathname'].data()
            ipath+=1
            inputcount=cursor.currentRow()['inputcount'].data()
            acceptcount=cursor.currentRow()['acceptcount'].data()
            prescale=cursor.currentRow()['prescale'].data()
            pathnameList.append(pathname)
            inputcountArray.append(inputcount)
            acceptcountArray.append(acceptcount)
            prescaleArray.append(prescale)
            if ipath==npath:
                pathnames=','.join(pathnameList)
                inputcountBlob=CommonUtil.packArraytoBlob(inputcountArray)
                acceptcountBlob=CommonUtil.packArraytoBlob(acceptcountArray)
                prescaleBlob=CommonUtil.packArraytoBlob(prescaleArray)
                databuffer[cmslsnum]=[inputcountBlob,acceptcountBlob,prescaleBlob]
                pathnameList=[]
                inputcountArray=array.array('l')
                acceptcountArray=array.array('l')
                prescaleArray=array.array('l')
                ipath=0
        del qHandle
        dbsession.transaction().commit()
    except Exception,e :
        dbsession.transaction().rollback()
        del dbsession
        raise Exception,'migrateSchema.getOldTrgData:'+str(e)
    return [pathnames,databuffer]

def main():
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),description="migrate lumidb schema",formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-c',dest='connect',action='store',required=False,default='oracle://cms_orcoff_prep/CMS_LUMI_DEV_OFFLINE',help='connect string to trigger DB(required)')
    parser.add_argument('-P',dest='authpath',action='store',required=False,default='/afs/cern.ch/user/x/xiezhen',help='path to authentication file')
    parser.add_argument('-r',dest='runnumber',action='store',required=True,help='run number')
    parser.add_argument('--debug',dest='debug',action='store_true',help='debug')
    args=parser.parse_args()
    runnumber=int(args.runnumber)
    print 'processing run ',runnumber
    os.environ['CORAL_AUTH_PATH']=args.authpath
    svc=coral.ConnectionService()
    dbsession=svc.connect(args.connect,accessMode=coral.access_Update)
    if args.debug:
        msg=coral.MessageStream('')
        msg.setMsgVerbosity(coral.message_Level_Debug)
    if isOldSchema(dbsession):
        print 'is old schema'
        createNewSchema(dbsession)
    else:
        print 'is new schema'
        dropNewSchema(dbsession)
        print 'creating new schema'
        createNewSchema(dbsession)
        print 'done'
    trgresult=getOldTrgData(dbsession,runnumber)
    hltresult=getOldHLTData(dbsession,runnumber)
    transferLumiData(dbsession,runnumber)
    transfertrgData(dbsession,runnumber,trgresult)
    transferhltData(dbsession,runnumber,hltresult)
    del dbsession
    del svc
    #print trgresult[0]
    #print len(trgresult[0].split(','))
    #print trgresult[1]
    #print '==========='
    #print hltresult[0]
    #print len(hltresult[0].split(','))
    #print hltresult[1]
    #print result
if __name__=='__main__':
    main()
    
