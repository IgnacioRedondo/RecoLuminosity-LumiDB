import os,coral
from RecoLuminosity.LumiDB import nameDealer,dbUtil,revisionDML,CommonUtil

########################################################################
# Norm/Correction/version DML API                                      #
#                                                                      #
# Author:      Zhen Xie                                                #
########################################################################

#==============================
# SELECT
#==============================
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
    if len(luminormids) !=0:return max(luminormids)
    return result

def normIdByContext(schema,amodetag,minegev,maxegev,defaultonly=True):
    '''
    select max(DATA_ID) FROM LUMINORMS WHERE AMODETAG=:amodetag and NOMINALEGEV>=:minegev and NOMINALEGEV<=:maxegev and ISCONTEXTDEFAULT=1;
    '''
    luminormids=[]
    iscontextdefault=0
    if defaultonly:
        iscontextdefault=1
    qHandle=schema.newQuery()
    try:
        qHandle.addToTableList( nameDealer.luminormv2TableName() )
        qHandle.addToOutputList('DATA_ID')
        qConditionStr='AMODETAG=:amodetag AND NOMINALEGEV>=:minegev AND NOMINALEGEV<=maxegev AND ISCONTEXTDEFAULT=:iscontextdefault'
        qCondition=coral.AttributeList()
        qCondition.extend('amodetag','string')
        qCondition.extend('minegev','float')
        qCondition.extend('maxegev','float')
        qCondition.extend('iscontextdefault','unsigned int')
        qCondition['amodetag'].setData(amodetag)
        qCondition['minegev'].setData(minegev)
        qCondition['maxegev'].setData(maxegev)
        qCondition['iscontextdefault'].setData(iscontextdefault)
        qResult=coral.AttributeList()
        qResult.extend('DATA_ID','unsigned long long')
        qHandle.defineOutput(qResult)
        qHandle.setCondition(qConditionStr,qCondition)
        cursor=qHandle.execute()
        while cursor.next():
            if not cursor.currentRow()['DATA_ID'].isNull():
                dataid=cursor.currentRow()['DATA_ID'].data()
                luminormids.append(dataid)
    except :
        del qHandle
        raise
    del qHandle
    if len(luminormids) !=0:return max(luminormids)
    return result

def normInfoByName(schema,normname):
    '''
    select DATA_ID,AMODETAG,NOMINALEGEV,LUMITYPE,ISCONTEXTDEFAULT,COMMENT,TO_CHAR(CTIME,\'MM/DD/YY HH24:MI\') FROM LUMINORMS WHERE ENTRY_NAME=:normname
    output:
        [data_id[0],amodetag[1],nominalegev[2],lumitype[3],iscontextdefault[4],comment[5],creationtime[6]]
    '''
    result={}
    qHandle=schema.newQuery()
    try:
        qHandle.addToTableList( nameDealer.luminormv2TableName() )
        qHandle.addToOutputList('DATA_ID')
        qHandle.addToOutputList('AMODETAG')
        qHandle.addToOutputList('NOMINALEGEV')
        qHandle.addToOutputList('LUMITYPE')
        qHandle.addToOutputList('ISCONTEXTDEFAULT')
        qHandle.addToOutputList('COMMENT')
        qHandle.addToOutputList('TO_CHAR(CTIME,\'MM/DD/YY HH24:MI\')','ctime')
        qConditionStr='ENTRY_NAME=:normname'
        qCondition=coral.AttributeList()
        qCondition.extend('entry_name','string')
        qCondition['entry_name'].setData(normname)
        qResult=coral.AttributeList()
        qResult.extend('DATA_ID','unsigned long long')
        qResult.extend('AMODETAG','string')
        qResult.extend('NOMINALEGEV','unsigned int')
        qResult.extend('LUMITYPE','string')
        qResult.extend('ISCONTEXTDEFAULT','unsigned int')
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
            amodetag=cursor.currentRow()['AMODETAG'].data()
            nominalegev=cursor.currentRow()['NOMINALEGEV'].data()
            lumitype=cursor.currentRow()['LUMITYPE'].data()
            iscontextdefault=cursor.currentRow()['ISCONTEXTDEFAULT'].data()
            if not cursor.currentRow()['COMMENT'].isNull():
                comment=cursor.currentRow()['COMMENT'].data()
            creationtime=cursor.currentRow()['ctime'].data()
            if not result.has_key(dataid):
                result[dataid]=[dataid,amodetag,nominalegev,lumitype,iscontextdefault,comment,creationtime]
    if len(result)>0:
        maxdataid=max(result.keys())
        return result[maxdataid]
    return []
    except :
        del qHandle
        raise
    
def normValueById(schema,normid):
    '''
    select l.*,d.* from luminormsv2 l,luminormsv2data d where d.data_id=l.data_id and l.data_id=normid
    output:
        {since:[corrector,{paramname:paramvalue}]}
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
            (correctorfunc,params)=CommonUtil.parselumicorrector(corrector)
            for param in params:
                paramvalue=0.0
                if not cursor.currentRow()[param].isNull():
                    paramvalue=cursor.currentRow()[param].data()
                    paramdict[param]=paramvalue
            result[since]=[correctorfunc,paramdict]
    return result
    except:
        raise

#=======================================================
#   INSERT/UPDATE requires in update transaction
#=======================================================
def createNorm(schema,normname,amodetag,egev,lumitype,iscontextdefault,branchinfo,comment=''):
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
        tabrowDefDict={'DATA_ID':'unsigned long long','ENTRY_ID':'unsigned long long','ENTRY_NAME':'string','AMODETAG':'string','NOMINALEGEV':'unsigned int','LUMITYPE':'string','ISCONTEXTDEFAULT':'unsigned int','COMMENT':'string','CTIME':'time stamp'}
        tabrowValueDict={'DATA_ID':data_id,'ENTRY_ID':entry_id,'ENTRY_NAME':normname,'AMODETAG':amodetag,'NOMINALEGEV':egev,'LUMITYPE':lumitype,'ISCONTEXTDEFAULT':iscontextdefault,'COMMENT':comment,'CTIME':coral.coral.TimeStamp()}
        db=dbUtil.dbUtil(schema)
        db.insertOneRow(nameDealer.luminormv2TableName(),tabrowDefDict,tabrowValueDict)
        return (revision_id,entry_id,data_id)
    except :
        raise

def promoteNormToContextDefault(schema,normname,amodetag,minegev,maxegev,lumitype):
    '''
    set the named norm as default for a given context,reset the old default if any
    thisnormid=normIdByName(schema,normname)
    olddefaultid=normIdByContext(schema,amodetag,minegev,maxegev,defaultonly=True)
    if thisnormid:
        update LUMINORMSV2 set ISCONTEXTDEFAULT=1 where DATA_ID=:thisnormid
    else:
        raise ValueError('normname does not exist, nothing to update')
    if olddefaultid and olddefaultid!=thisnormid:
        update LUMINORMSV2 set ISCONTEXTDEFAULT=0 where DATA_ID=:olddefaultid
    '''
    try:
        thisnormid=normIdByName(schema,normname)
        olddefaultid=normIdByContext(schema,amodetag,minegev,maxegev,defaultonly=True)
        if not thisnormid:
            raise ValueError(normname+' does not exist, nothing to update')
        setClause='ISCONTEXTDEFAULT=1'
        updateCondition='DATA_ID=:thisnormid'
        inputData=coral.AttributeList()
        inputData.extend('thisnormid','unsigned long long')
        inputData['thisnormid'].setData(thisnormid)
        db=dbUtil.dbUtil(schema)
        db.singleUpdate(nameDealer.luminormTable(),setClause,updateCondition,inputData)
        if olddefaultid:
            setClause='ISCONTEXTDEFAULT=0'
            updateCondition='DATA_ID=:olddefaultid'
            inputData=coral.AttributeList()
            inputData.extend('olddefaultid','unsigned long long')
            inputData['olddefaultid'].setData(olddefaultid)
            db=dbUtil.dbUtil(schema)
            db.singleUpdate(nameDealer.luminormTable(),setClause,updateCondition,inputData)
    except :
        raise
def appendValueToNormId(schema,normdataid,sincerun,corrector,parameters):
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
        tabrowDefDict['SINCE']='unsigned long long'
        tabrowDefDict['CORRECTOR']='string'
        tabrowValueDict={}
        for paramname,paramvalue in parameters.items():
            tabrowValueDict[paramname.upper()]=paramvalue
        db.insertOneRow(revisiontableName,tabrowDefDict,tabrowValueDict)
    except:
        raise

    
    
