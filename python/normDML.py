import os,coral
from RecoLuminosity.LumiDB import nameDealer,dbUtil,revisionDML

########################################################################
# Norm/Correction/version DML API                                      #
#                                                                      #
# Author:      Zhen Xie                                                #
########################################################################
    
#==============================
# SELECT
#==============================
def allNorms(schema):
    '''
    list all lumi norms
    select DATA_ID,ENTRY_NAME,LUMITYPE,ISTYPEDEFAULT,COMMENT,CTIME FROM LUMINORMSV2
    output:
    {normname:[data_id,lumitype,istypedefault,comment,creationtime]}
    '''
    result={}
    qHandle=schema.newQuery()
    try:
        qHandle.addToTableList( nameDealer.luminormv2TableName() )
        qHandle.addToOutputList('DATA_ID')
        qHandle.addToOutputList('ENTRY_NAME')
        qHandle.addToOutputList('LUMITYPE')
        qHandle.addToOutputList('ISTYPEDEFAULT')
        qHandle.addToOutputList('COMMENT')
        qHandle.addToOutputList('TO_CHAR(CTIME,\'MM/DD/YY HH24:MI\')','creationtime')
        qResult=coral.AttributeList()
        qResult.extend('DATA_ID','unsigned long long')
        qResult.extend('ENTRY_NAME','string')
        qResult.extend('LUMITYPE','string')
        qResult.extend('ISTYPEDEFAULT','unsigned int')
        qResult.extend('COMMENT','string')
        qResult.extend('creationtime','string')        
        qHandle.defineOutput(qResult)
        cursor=qHandle.execute()
        while cursor.next():
            normname=cursor.currentRow()['ENTRY_NAME'].data()
            if not result.has_key(normname):
                result[normname]=[]
            dataid=cursor.currentRow()['DATA_ID'].data()
            lumitype=cursor.currentRow()['LUMITYPE'].data()
            istypedefault=cursor.currentRow()['ISTYPEDEFAULT'].data()
            comment=''
            if not cursor.currentRow().isNull():
                comment==cursor.currentRow()['COMMENT'].data()
            creationtime=cursor.currentRow()['creationtime'].data()
            if len(result[normname])==0:
                result[normname]=[data_id,lumitype,istypedefault,comment,creationtime]
            elif len(result[normname])!=0 and data_id>result[normname][0]:
                result[normname]=[data_id,lumitype,iscontypedefault,comment,creationtime]
    except :
        del qHandle
        raise
    del qHandle
    return result

def normIdByName(schema,normname):
    '''
    select max(DATA_ID) FROM LUMINORMSV2 WHERE ENTRY_NAME=:normname
    '''
    luminormids=[]
    result=None
    qHandle=schema.newQuery()
    try:
        qHandle.addToTableList( nameDealer.luminormv2TableName() )
        qHandle.addToOutputList('DATA_ID')
        if tagname:
            qConditionStr='ENTRY_NAME=:normname '
            qCondition=coral.AttributeList()
            qCondition.extend('normname','string')
            qCondition['normname'].setData(normname)
        qResult=coral.AttributeList()
        qResult.extend('DATA_ID','unsigned long long')
        qHandle.defineOutput(qResult)
        if tagname:
            qHandle.setCondition(qConditionStr,qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            dataid=cursor.currentRow()['DATA_ID'].data()
            luminormids.append(dataid)
    except :
        del qHandle
        raise
    del qHandle
    if len(luminormids) !=0:
        return max(luminormids)    
    return result

def normIdByType(schema,lumitype='HF',defaultonly=True):
    '''
    select max(DATA_ID) FROM LUMINORMSV2 WHERE LUMITYPE=:lumitype
    output:
        luminormidmap {normname:normid}
    '''
    luminormidmap={}
    istypedefault=0
    if defaultonly:
        istypedefault=1
    qHandle=schema.newQuery()
    try:
        qHandle.addToTableList( nameDealer.luminormv2TableName() )
        qHandle.addToOutputList('DATA_ID')
        qHandle.addToOutputList('ENTRY_NAME')
        qConditionStr='LUMITYPE=:lumitype AND ISTYPEDEFAULT=:istypedefault'
        qCondition=coral.AttributeList()
        qCondition.extend('lumitype','string')
        qCondition.extend('istypedefault','unsigned int')
        qCondition['lumitype'].setData(lumitype)
        qCondition['istypedefault'].setData(istypedefault)
        qResult=coral.AttributeList()
        qResult.extend('DATA_ID','unsigned long long')
        qResult.extend('ENTRY_NAME','string')
        qHandle.defineOutput(qResult)
        qHandle.setCondition(qConditionStr,qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            if not cursor.currentRow()['DATA_ID'].isNull():
                dataid=cursor.currentRow()['DATA_ID'].data()
                normname=cursor.currentRow()['ENTRY_NAME'].data()
                if not luminormidmap.has_key(normname):
                    luminormidmap[normname]=dataid
                else:
                    if dataid>luminormidmap[normname]:
                        luminormidmap[normname]=dataid
    except :
        del qHandle
        raise
    del qHandle
    return result

def normInfoByName(schema,normname):
    '''
    select DATA_ID,LUMITYPE,ISTYPEDEFAULT,COMMENT,TO_CHAR(CTIME,\'MM/DD/YY HH24:MI\') FROM LUMINORMS WHERE ENTRY_NAME=:normname
    output:
        [data_id[0],lumitype[1],istypedefault[2],comment[3],creationtime[4]]
    '''
    result=[]
    qHandle=schema.newQuery()
    try:
        qHandle.addToTableList( nameDealer.luminormv2TableName() )
        qHandle.addToOutputList('DATA_ID')
        qHandle.addToOutputList('LUMITYPE')
        qHandle.addToOutputList('ISTYPEDEFAULT')
        qHandle.addToOutputList('COMMENT')
        qHandle.addToOutputList('TO_CHAR(CTIME,\'MM/DD/YY HH24:MI\')','ctime')
        qConditionStr='ENTRY_NAME=:normname'
        qCondition=coral.AttributeList()
        qCondition.extend('normname','string')
        qCondition['normname'].setData(normname)
        qResult=coral.AttributeList()
        qResult.extend('DATA_ID','unsigned long long')
        qResult.extend('LUMITYPE','string')
        qResult.extend('ISTYPEDEFAULT','unsigned int')
        qResult.extend('COMMENT','string')
        qResult.extend('ctime','string')
        qHandle.defineOutput(qResult)
        qHandle.setCondition(qConditionStr,qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            if not cursor.currentRow()['DATA_ID'].isNull():
                dataid=cursor.currentRow()['DATA_ID'].data()
            else:
                continue
            lumitype=cursor.currentRow()['LUMITYPE'].data()
            istypedefault=cursor.currentRow()['ISTYPEDEFAULT'].data()
            if not cursor.currentRow()['COMMENT'].isNull():
                comment=cursor.currentRow()['COMMENT'].data()
            creationtime=cursor.currentRow()['ctime'].data()
            if not result.has_key(dataid):
                result[dataid]=[dataid,lumitype,istypedefault,comment,creationtime]
    if len(result)>0:
        maxdataid=max(result.keys())
        return result[maxdataid]
    return result
    except :
        del qHandle
        raise
    
def normValueById(schema,normid):
    '''
    select l.*,d.* from luminormsv2 l,luminormsv2data d where d.data_id=l.data_id and l.data_id=normid
    output:
        {since:[corrector,{paramname:paramvalue},context]}
    '''
    result={}
    d=nameDealer.luminormv2TableName()
    l=nameDealer.luminormv2dataTableName()
    paramdict={}
    try:
        qHandle.addToTableList(d)
        qHandle.addToTableList(l)
        qConditionStr=d+'.DATA_ID='+l+'.DATA_ID AND '+l+'.DATA_ID=:normid'
        qCondition=coral.AttributeList()
        qCondition.extend('normid','unsigned long long')
        qCondition['normid'].setData(normid)
        qResult=coral.AttributeList()
        qHandle.setCondition(qConditionStr,qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            since=cursor.currentRow()['SINCE'].data()
            corrector=cursor.currentRow()['CORRECTOR'].data()
            amodetag=cursor.currentRow()['AMODETAG'].data()
            nominalegev=cursor.currentRow()['NOMINALEGEV'].data()
            context=amodetag+'_'+str(nominalegev)
            (correctorfunc,params)=CommonUtil.parselumicorrector(corrector)
            for param in params:
                paramvalue=0.0
                if not cursor.currentRow()[param].isNull():
                    paramvalue=cursor.currentRow()[param].data()
                    paramdict[param]=paramvalue
            result[since]=[correctorfunc,paramdict,context]
    return result
    except:
        raise

#=======================================================
#   INSERT/UPDATE requires in update transaction
#=======================================================
def createNorm(schema,normname,lumitype,istypedefault,branchinfo,comment=''):
    '''
    branchinfo(normrevisionid,branchname)    
    '''
    try:
        entry_id=revisionDML.entryInBranch(schema,nameDealer.luminormv2TableName(),normname,branchinfo[1])
        if entry_id is None:
            (revision_id,entry_id,data_id)=revisionDML.bookNewEntry(schema,nameDealer.luminormv2TableName())
            entryinfo=(revision_id,entry_id,normname,data_id)
            revisionDML.addEntry(schema,nameDealer.luminormv2TableName(),entryinfo,branchinfo)
        else:
            (revision_id,data_id)=revisionDML.bookNewRevision( schema,nameDealer.luminormv2TableName() )
            revisionDML.addRevision(schema,nameDealer.luminormv2TableName(),(revision_id,data_id),branchinfo)
        tabrowDefDict={'DATA_ID':'unsigned long long','ENTRY_ID':'unsigned long long','ENTRY_NAME':'string','LUMITYPE':'string','ISTYPEDEFAULT':'unsigned int','COMMENT':'string','CTIME':'time stamp'}
        tabrowValueDict={'DATA_ID':data_id,'ENTRY_ID':entry_id,'ENTRY_NAME':normname,'LUMITYPE':lumitype,'ISTYPEDEFAULT':istypedefault,'COMMENT':comment,'CTIME':coral.coral.TimeStamp()}
        db=dbUtil.dbUtil(schema)
        db.insertOneRow(nameDealer.luminormv2TableName(),tabrowDefDict,tabrowValueDict)
        return (revision_id,entry_id,data_id)
    except :
        raise
    
def demoteNormFromTypeDefault(schema,normname,lumitype):
    '''
    demote norm from typedefault to non default
    '''
    try:
        thisnormid=normIdByName(schema,normname)
        if not thisnormid:
            raise ValueError(normname+' does not exist, nothing to update')
        setClause='ISTYPEDEFAULT=0'
        updateCondition='DATA_ID=:thisnormid AND LUMITYPE=:lumitype'
        inputData=coral.AttributeList()
        inputData.extend('thisnormid','unsigned long long')
        inputData.extend('LUMITYPE','string')
        inputData['thisnormid'].setData(thisnormid)
        inputData['LUMITYPE'].setData(lumitype)
        db=dbUtil.dbUtil(schema)
        db.singleUpdate(nameDealer.luminormv2Table(),setClause,updateCondition,inputData)
    except :
        raise
    
def promoteNormToTypeDefault(schema,normname,lumitype):
    '''
    set the named norm as default for a given type,reset the old default if any
    thisnormid=normIdByName(schema,normname)
    olddefaultid=normIdByType(schema,lumitype=lumitype,defaultonly=True)
    if thisnormid:
        update LUMINORMSV2 set ISTYPEDEFAULT=1 where DATA_ID=:thisnormid
    else:
        raise ValueError('normname does not exist, nothing to update')
    if olddefaultid and olddefaultid!=thisnormid:
        update LUMINORMSV2 set ISTYPEDEFAULT=0 where DATA_ID=:olddefaultid
    '''
    try:
        thisnormid=normIdByName(schema,normname)
        olddefaultid=normIdByContext(schema,amodetag,minegev,maxegev,defaultonly=True)
        if not thisnormid:
            raise ValueError(normname+' does not exist, nothing to update')
        setClause='ISTYPEDEFAULT=1'
        updateCondition='DATA_ID=:thisnormid'
        inputData=coral.AttributeList()
        inputData.extend('thisnormid','unsigned long long')
        inputData['thisnormid'].setData(thisnormid)
        db=dbUtil.dbUtil(schema)
        db.singleUpdate(nameDealer.luminormTable(),setClause,updateCondition,inputData)
        if olddefaultid:
            setClause='ISTYPEDEFAULT=0'
            updateCondition='DATA_ID=:olddefaultid'
            inputData=coral.AttributeList()
            inputData.extend('olddefaultid','unsigned long long')
            inputData['olddefaultid'].setData(olddefaultid)
            db=dbUtil.dbUtil(schema)
            db.singleUpdate(nameDealer.luminormTable(),setClause,updateCondition,inputData)
    except :
        raise
def insertValueToNormId(schema,normdataid,sincerun,corrector,amodetag,egev,parameters):
    '''
    insert into LUMINORM_DATA(DATA_ID,SINCERUN,CORRECTOR,...) values(normdataid,)sincerun,corrector,...);
    require len(parameters)>=1.
    input:
      parameterDict {'NORM_OCC1':normocc1,'NORM_OCC2':normocc2,'NORM_ET':normet,'NORM_PU':normpu,'DRIFT':drift,'A1':a1,...}
    output:
    '''
    if len(parameters)==0:
        raise ValueError('appendValueToNormId: at least one value is required')
    try:
        db=dbUtil.dbUtil(schema)
        tabrowDefDict={}
        tabrowDefDict['DATA_ID']='unsigned long long'
        tabrowDefDict['CORRECTOR']='string'
        tabrowDefDict['SINCE']='unsigned long long'
        tabrowDefDict['AMODETAG']='string'
        tabrowDefDict['NOMINALEGEV']='unsigned int'
        tabrowValueDict={}
        tabrowValueDict['DATA_ID']=normdataid
        tabrowValueDict['CORRECTOR']=corrector
        tabrowValueDict['SINCE']=sincerun
        tabrowValueDict['AMODETAG']=amodetag
        tabrowValueDict['EGEV']=egev
        for paramname,paramvalue in parameters.items():
            tabrowValueDict[paramname.upper()]=paramvalue
        db.insertOneRow(revisiontableName,tabrowDefDict,tabrowValueDict)
    except:
        raise
################################################
#todo: need copy/export/import functionalities 
################################################
    
    
