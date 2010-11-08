import csv,sys
from RecoLuminosity.LumiDB import CommonUtil
ilsfilename='Run148829-ls.txt'
ibunchfilename='Run148829-bunch.txt'
conn='oracle://cms_orcoff_prep/cms_lumi_prod'
beamenergy=3.5e03
beamstatus='STABLE BEAMS'
beamintensity=0
def convertlist(l):
    '''yield successive pairs for l
    '''
    for i in xrange(0,len(l),2):
        idx=int(l[i])
        val=float(l[i+1])
        yield (idx,val)
                    
def parsebunchFile(ifilename):
    perbunchdata=[]
    result=[]
    try:
        csvfile=open(ifilename,'rb')
        reader=csv.reader(csvfile,delimiter=' ',skipinitialspace=True)
        for row in reader:
            result+=row
        for i in convertlist(result):
            perbunchdata.append(i)
        return perbunchdata
    except Exception,e:
        raise RuntimeError(str(e))
def parseLSFile(ifilename):
    perlsdata=[]
    result=[]
    try:
        csvfile=open(ifilename,'rb')
        reader=csv.reader(csvfile,delimiter=' ',skipinitialspace=True)
        for row in reader:
            result+=row
        for i in convertlist(result):
            perlsdata.append(i)
        return perlsdata
    except Exception,e:
        raise RuntimeError(str(e))
    
def main(*args):
    perlsrawdata=parseLSFile(ilsfilename)
    print perlsrawdata

    perbunchrawdata=parsebunchFile(ibunchfilename)
    print perbunchrawdata
if __name__=='__main__':
    sys.exit(main(*sys.argv))
