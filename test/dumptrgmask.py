import coral
from RecoLuminosity.LumiDB import sessionManager
'''
select tt, algo bit masks and pack them into 3 unsigned long long:
algomask_hi, algomask_lo,ttmask

select gt_rs_key,run_number from cms_gt_mon.global_runs where run_number>=132440;

select tt.finor_tt_*, from cms_gt.gt_partition_finor_tt tt,cms_gt.gt_run_settings r where tt.id=r.finor_tt_fk and r.id=:gt_rs_key ;

select algo.finor_algo_*, from cms_gt.gt_partition_finor_algo algo,cms_gt.gt_run_settings r where tt.id=r.finor_algo_fk and r.id=:gt_rs_key;

output:{run:[algomask_hi,algomask_lo,ttmask]}

algomask_high,      algomask_low
127,126,.....,64,;  63,62,0
ttmask
63,62,...0
'''

if __name__ == '__main__':
    pth='/afs/cern.ch/user/l/lumipro/'
    sourcestr='oracle://cms_orcon_adg/cms_gt'
    sourcesvc=sessionManager.sessionManager(sourcestr,authpath=pth,debugON=False)
    sourcesession=sourcesvc.openSession(isReadOnly=True,cpp2sqltype=[('short','NUMBER(1)'),('unsigned int','NUMBER(10)'),('unsigned long long','NUMBER(20)')])
    sourcesession.transaction().start(True)
    gtschema=sourcesession.schema('CMS_GT')
    gtmonschema=sourcesession.schema('CMS_GT_MON')
    runkeymap={}
    runkeyquery=gtmonschema.newQuery()
    try:
        runkeyquery.addToTableList('GLOBAL_RUNS')
        qCondition=coral.AttributeList()
        qCondition.extend('runnum','unsigned int')
        qCondition['runnum'].setData(int(132440))
        runkeyquery.addToOutputList('GT_RS_KEY')
        runkeyquery.addToOutputList('RUN_NUMBER')
        qResult=coral.AttributeList()
        qResult.extend('GT_RS_KEY','string')
        qResult.extend('RUN_NUMBER','unsigned int')
        runkeyquery.defineOutput(qResult)
        runkeyquery.setCondition('RUN_NUMBER>=:runnum',qCondition)
        cursor=runkeyquery.execute()
        while cursor.next():
            gtrskey=cursor.currentRow()['GT_RS_KEY'].data()
            runnum=cursor.currentRow()['RUN_NUMBER'].data()
            runkeymap[runnum]=gtrskey
        del runkeyquery
    except:
        if runkeyquery:del runkeyquery
        raise
    uniquegtkeys=set(runkeymap.values())
    keymaskmap={}#{gtkey:[algomask_hi,algomask_lo,ttmask]}
    ttTab='GT_PARTITION_FINOR_TT'
    algoTab='GT_PARTITION_FINOR_ALGO'
    runsetTab='GT_RUN_SETTINGS'        
    try:
        for k in uniquegtkeys:
            algomask_hi=0
            algomask_lo=0
            ttmask=0
            keymaskmap[k]=[algomask_hi,algomask_lo,ttmask]
            
            ttquery=gtschema.newQuery()
            ttquery.addToTableList(ttTab)
            ttquery.addToTableList(runsetTab)
            ttResult=coral.AttributeList()
            for i in range(0,64):
                ttquery.addToOutputList(ttTab+'.FINOR_TT_%03d'%(i),'tt_%03d'%(i))
                ttResult.extend('tt_%03d'%(i),'short')
            ttConditionStr=ttTab+'.ID='+runsetTab+'.FINOR_TT_FK AND '+runsetTab+'.ID=:gt_rs_key'
            ttCondition=coral.AttributeList()
            ttCondition.extend('gt_rs_key','string')
            ttCondition['gt_rs_key'].setData(k)
            ttquery.defineOutput(ttResult)
            ttquery.setCondition(ttConditionStr,ttCondition)
            cursor=ttquery.execute()
            while cursor.next():
                for ttidx in range(0,64):
                    kvalue=cursor.currentRow()['tt_%03d'%(ttidx)].data()
                    if kvalue!=0:
                        ttdefaultval=keymaskmap[k][2]
                        keymaskmap[k][2]=ttdefaultval|1<<ttidx
            del ttquery
            
            algoquery=gtschema.newQuery()
            algoquery.addToTableList(algoTab)
            algoquery.addToTableList(runsetTab)
            algoResult=coral.AttributeList()
            for i in range(0,128):
                algoquery.addToOutputList(algoTab+'.FINOR_ALGO_%03d'%(i),'algo_%03d'%(i))
                algoResult.extend('algo_%03d'%(i),'short')
            algoConditionStr=algoTab+'.ID='+runsetTab+'.FINOR_ALGO_FK AND '+runsetTab+'.ID=:gt_rs_key'
            algoCondition=coral.AttributeList()
            algoCondition.extend('gt_rs_key','string')
            algoCondition['gt_rs_key'].setData(k)
            algoquery.defineOutput(algoResult)
            algoquery.setCondition(algoConditionStr,algoCondition)
            cursor=algoquery.execute()
            while cursor.next():
                for algoidx in range(0,128):
                    kvalue=cursor.currentRow()['algo_%03d'%(algoidx)].data()
                    if kvalue!=0:
                        if algoidx<64:
                            #if k=='gtrs_2012_9e33_v5':
                            #    print algoidx,kvalue
                            algodefaultval=keymaskmap[k][1]#low 63-0
                            keymaskmap[k][1]=algodefaultval|1<<algoidx
                        else:
                            #if k=='gtrs_2012_9e33_v5':
                            #    print algoidx,(algoidx-64),kvalue
                            algodefaultval=keymaskmap[k][0]#high 127-64
                            keymaskmap[k][0]=algodefaultval|1<<(algoidx-64)
            del algoquery
        ahi=keymaskmap['gtrs_2012_9e33_v5'][0]
        #print 'algo 84 ',ahi>>(84-64)&1
        #print 'algo 126 ',ahi>>(126-64)&1
        alo=keymaskmap['gtrs_2012_9e33_v5'][1]
        #print 'algo 0 ',alo>>0&1
    except:
        raise
    sourcesession.transaction().commit()
    


