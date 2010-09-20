import array
from ROOT import gSystem
gSystem.Load('libRecoLuminosityLumiDB.so')
from ROOT import TFile,TTree,HCAL_HLX
def read(fn):
    print 'read ',fn
    try:
        #f=TFile(fn)
        treenames=[]
        f=TFile.Open(fn)
        keys=f.GetListOfKeys()
        treenames=[key.GetName() for key in keys]
        if 'HLXData' not in treenames or 'DIPCombined' not in treenames:
            print 'this file does not have both HLXData and DIPCombined'
            return
        if not f:
            print 'cannot open file'
            return
        hlxtree=f.Get('HLXData')
        hlxresult={}#[cmsalive,instlumi,instlumierror,startorbit,norbit]
        diptree=f.Get('DIPCombined')
        dipresult={}
        nhlxentry=hlxtree.GetEntries()
        #print nhlxentry
        lumisection=HCAL_HLX.LUMI_SECTION()
        lumiheader=lumisection.hdr
        lumisummary=lumisection.lumiSummary
        lumidetail=lumisection.lumiDetail
        hlxtree.SetBranchAddress('Header.',lumiheader)
        hlxtree.SetBranchAddress('Summary.',lumisummary)
        hlxtree.SetBranchAddress('Detail.',lumidetail)
        for i in range(nhlxentry):
            hlxtree.GetEntry(i)
            bcmslive=lumiheader.bCMSLive
            cmslsnum=lumiheader.sectionNumber
            if not bcmslive and i!=0:
                cmsalive=1
            else:
                cmsalive=0
            instlumi=lumisummary.InstantLumi
            instlumierror=lumisummary.InstantLumiErr
            startorbit=lumiheader.startOrbit
            numorbit=lumiheader.numOrbits
            hlxresult[cmslsnum]=[cmsalive,instlumi,instlumierror,startorbit,numorbit]
        #print 'hlxresult ',hlxresult
        ndipentry=diptree.GetEntries()
        print ndipentry
        dipdata=HCAL_HLX.DIP_COMBINED_DATA()
        diptree.SetBranchAddress('DIPCombined.',dipdata)
        for i in range(ndipentry):
            diptree.GetEntry(i)
            diplsnum=dipdata.sectionNumber
            beammode='N/A'
            if dipdata.beamMode and len(dipdata.beamMode)!=0:
                beammode=dipdata.beamMode
            beamenergy=0.0
            totalbeamintensity=0.0
            if dipdata.Energy:
                beamenergy=dipdata.Energy
            print beamenergy
           
            beam1=HCAL_HLX.BEAM_INFO()
            beam2=HCAL_HLX.BEAM_INFO()
            print dipdata.Beam
            #print beam1.averageBeamIntensity
            #    totalbeamintensity_1=dipdata.Beam[0].averageBeamIntensity
            #dipresult[diplsnum]=[beammode,beamenergy,totalbeamintensity_1]
        print dipresult
        f.Close()
    except Exception,e:
        print str(e)
if __name__=='__main__':
    #read('rfio:/castor/cern.ch/cms/store/lumi/201009/CMS_LUMI_RAW_20100906_000145042_0001_1.root')
    #read('rfio:/castor/cern.ch/cms/store/lumi/201007/CMS_LUMI_RAW_20100713_000140070_0001_1.root')
    read('file:/build1/zx/tmpdata/CMS_LUMI_RAW_20100826_000143956_0001_1.root')
