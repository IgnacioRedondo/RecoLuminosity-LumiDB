import coral
import os,binascii
import array
class constants(object):
    def __init__(self):
        self.debug=False
        self.nbx=3564
        self.lumischema='CMS_LUMI_PROD'
        self.lumidb='sqlite_file:///afs/cern.ch/user/x/xiezhen/w1/luminewschema/CMSSW_3_8_0/src/RecoLuminosity/LumiProducer/test/lumi.db'
        self.lumisummaryname='LUMISUMMARY'
        self.lumidetailname='LUMIDETAIL'
def beamIntensityForRun(dbsession,c,runnum):
    '''
    select CMSBXINDEXBLOB,BEAMINTENSITYBLOB_1,BEAMINTENSITYBLOB_2 from LUMISUMMARY where runnum=146315 and LUMIVERSION='0001'
    '''
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
        result=[]
        while cursor.next():
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            startorbit=cursor.currentRow()['startorbit'].data()
            if not cursor.currentRow()["bxindexblob"].isNull():
                bxindexblob=cursor.currentRow()['bxindexblob'].data()
                bxidx=array.array('h')
                bxidx.fromstring(bxindexblob.readline())
                beamintensityblob1=cursor.currentRow()['beamintensityblob1'].data()
                beamintensityblob2=cursor.currentRow()['beamintensityblob2'].data()
                bb1=array.array('f')
                bb1.fromstring(beamintensityblob1.readline())
                bb2=array.array('f')
                bb2.fromstring(beamintensityblob2.readline())
                result.append((startorbit,cmslsnum,bxidx,bb1,bb2))
        sorted(result,key=lambda x:x[0])
        print result
        for perlsdata in result:
            print perlsdata[1]
            bb1=perlsdata[3]
            bb2=perlsdata[4]
            for index,bxidxvalue in enumerate(perlsdata[2]):
                print "  %2d,%.3e,%.3e"%(bxidxvalue,bb1[index],bb2[index])
        del query
        dbsession.transaction().commit()
    except Exception,e:
        print str(e)
        dbsession.transaction().rollback()
        del dbsession
def detailForRun(dbsession,c,runnum,algos=['OCC1']):
    '''select 
    s.cmslsnum,d.bxlumivalue,d.bxlumierror,d.bxlumiquality,d.algoname from LUMIDETAIL d,LUMISUMMARY s where s.runnum=133885 and d.algoname='OCC1' and s.lumisummary_id=d.lumisummary_id order by s.startorbit,s.cmslsnum
    '''
    try:
        dbsession.transaction().start(True)
        schema=dbsession.schema(c.lumischema)
        if not schema:
            raise 'cannot connect to schema ',c.lumischema
        detailOutput=coral.AttributeList()
        detailOutput.extend('cmslsnum','unsigned int')
        detailOutput.extend('bxlumivalue','blob')
        detailOutput.extend('bxlumierror','blob')
        detailOutput.extend('bxlumiquality','blob')
        detailOutput.extend('algoname','string')

        detailCondition=coral.AttributeList()
        detailCondition.extend('runnum','unsigned int')
        detailCondition.extend('algoname','string')
        detailCondition['runnum'].setData(runnum)
        detailCondition['algoname'].setData(algos[0])
        query=schema.newQuery()
        query.addToTableList(c.lumisummaryname,'s')
        query.addToTableList(c.lumidetailname,'d')
        query.addToOutputList('s.CMSLSNUM','cmslsnum')
        query.addToOutputList('d.BXLUMIVALUE','bxlumivalue')
        query.addToOutputList('d.BXLUMIERROR','bxlumierror')
        query.addToOutputList('d.BXLUMIQUALITY','bxlumiquality')
        query.addToOutputList('d.ALGONAME','algoname')
        query.setCondition('s.RUNNUM=:runnum and d.ALGONAME=:algoname and s.LUMISUMMARY_ID=d.LUMISUMMARY_ID',detailCondition)
        query.addToOrderList('s.STARTORBIT')
        query.addToOrderList('s.CMSLSNUM')
        query.defineOutput(detailOutput)
        cursor=query.execute()
        
        while cursor.next():
            cmslsnum=cursor.currentRow()['cmslsnum'].data()
            algoname=cursor.currentRow()['algoname'].data()
            bxlumivalue=cursor.currentRow()['bxlumivalue'].data()
            print 'cmslsnum , algoname'
            print cmslsnum,algoname
            print '===='
            #print 'bxlumivalue starting address ',bxlumivalue.startingAddress()
            #bxlumivalue float[3564]
            #print 'bxlumivalue size ',bxlumivalue.size()
            #
            #convert bxlumivalue to byte string, then unpack??
            #binascii.hexlify(bxlumivalue.readline()) 
            #
            a=array.array('f')
            a.fromstring(bxlumivalue.readline())
            print '   bxindex, bxlumivalue'
            for index,lum in enumerate(a):
                print "  %4d,%.3e"%(index,lum)
            #realvalue=a.tolist()
            #print len(realvalue)
            #print realvalue
        del query
        dbsession.transaction().commit()
        
    except Exception,e:
        print str(e)
        dbsession.transaction().rollback()
        del dbsession

def main():
    c=constants()
    os.environ['CORAL_AUTH_PATH']='/afs/cern.ch/cms/DB/lumi'
    svc=coral.ConnectionService()
    session=svc.connect(c.lumidb,accessMode=coral.access_ReadOnly)
    session.typeConverter().setCppTypeForSqlType("unsigned int","NUMBER(10)")
    session.typeConverter().setCppTypeForSqlType("unsigned long long","NUMBER(20)")
    msg=coral.MessageStream('')
    msg.setMsgVerbosity(coral.message_Level_Error)
    runnum=146315
    ##here arg 4 is default to ['OCC1'], if you want to see all the algorithms do
    ##  detailForRun(session,c,runnum,['OCC1','OCC2','ET']) then modify detailForRun adding an outer loop on algos argument. I'm lazy
    #detailForRun(session,c,runnum)

    beamIntensityForRun(session,c,runnum)
if __name__=='__main__':
    main()
