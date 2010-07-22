#!/usr/bin/env python
VERSION = '2.00'
import os, sys
import coral
import array
import optparse
from RecoLuminosity.LumiDB import argparse, nameDealer, selectionParser, hltTrgSeedMapper, \
     connectstrParser, cacheconfigParser, tablePrinter, csvReporter, csvSelectionParser
from RecoLuminosity.LumiDB.wordWrappers import wrap_always, wrap_onspace, wrap_onspace_strict
from pprint import pprint

class ParametersObject (object):

    def __init__ (self):
        self.norm            = 1.0
        self.lumiversion     = '0001'
        self.NBX             = 3564
        self.normFactor      = 6.37
        self.beammode        = 'stable' #possible choices stable, quiet, either
        self.verbose         = False
        self.noWarnings      = False
        self.lumischema      = 'CMS_LUMI_PROD'
        self.lumidb          = 'oracle://cms_orcoff_prod/cms_lumi_prod'
        self.lumisummaryname = 'LUMISUMMARY'
        self.lumidetailname  = 'LUMIDETAIL'

    def defaultfrontierConfigString (self):
        return '''<frontier-connect><proxy url = "http://cmst0frontier.cern.ch:3128"/><proxy url = "http://cmst0frontier.cern.ch:3128"/><proxy url = "http://cmst0frontier1.cern.ch:3128"/><proxy url = "http://cmst0frontier2.cern.ch:3128"/><server url = "http://cmsfrontier.cern.ch:8000/FrontierInt"/><server url = "http://cmsfrontier.cern.ch:8000/FrontierInt"/><server url = "http://cmsfrontier1.cern.ch:8000/FrontierInt"/><server url = "http://cmsfrontier2.cern.ch:8000/FrontierInt"/><server url = "http://cmsfrontier3.cern.ch:8000/FrontierInt"/><server url = "http://cmsfrontier4.cern.ch:8000/FrontierInt"/></frontier-connect>'''
    
def lslengthsec (numorbit, numbx):
    #print numorbit, numbx
    l = numorbit*numbx*25e-09
    return l

def lsBylsLumi (deadtable):
    """
    input: {lsnum:[deadtime, instlumi, bit_0, norbits]}
    output: {lsnum:[instlumi, recordedlumi]}
    """
    result = {}
    for myls, d in deadtable.items():
        lstime = lslengthsec (d[3], 3564)
        instlumi = d[1] * lstime
        if float( d[2] ) ==  0.0:
            deadfrac = 1.0
        else:
            deadfrac = float (d[0]) / float (d[2])
        recordedLumi = instlumi * (1.0 - deadfrac)
        result[myls] = [instlumi, recordedLumi]
    return result


def deliveredLumiForRun (dbsession, parameters, runnum):    
    """select sum (INSTLUMI), count (INSTLUMI) from lumisummary where
    runnum = 124025 and lumiversion = '0001'; apply norm factor and ls
    length in sec on the query result unit E27cm^-2"""    
    #if parameters.verbose:
    #    print 'deliveredLumiForRun : norm : ', parameters.norm, ' : run : ', runnum
    #output ['run', 'totalls', 'delivered', 'beammode']
    delivered = 0.0
    totalls = 0
    try:
        dbsession.transaction().start (True)
        schema = dbsession.nominalSchema()
        query = schema.tableHandle (nameDealer.lumisummaryTableName()).newQuery()
        query.addToOutputList ("sum (INSTLUMI)", "totallumi")
        query.addToOutputList ("count (INSTLUMI)", "totalls")
        query.addToOutputList ("NUMORBIT", "norbits")
        queryBind = coral.AttributeList()
        queryBind.extend ("runnum", "unsigned int")
        queryBind.extend ("lumiversion", "string")
        queryBind["runnum"].setData (int (runnum))
        queryBind["lumiversion"].setData (parameters.lumiversion)
        result = coral.AttributeList()
        result.extend ("totallumi", "float")
        result.extend ("totalls", "unsigned int")
        result.extend ("norbits", "unsigned int")
        query.defineOutput (result)
        query.setCondition ("RUNNUM = :runnum AND LUMIVERSION = :lumiversion", queryBind)
        query.limitReturnedRows (1)
        query.groupBy ('NUMORBIT')
        cursor = query.execute()
        while cursor.next():
            delivereddata = cursor.currentRow()['totallumi'].data()
            totallsdata = cursor.currentRow()['totalls'].data()
            norbitsdata = cursor.currentRow()['norbits'].data()
            if delivereddata:
                totalls = totallsdata
                norbits = norbitsdata
                lstime = lslengthsec (norbits, parameters.NBX)
                delivered = delivereddata*parameters.norm*lstime
        del query
        dbsession.transaction().commit()
        lumidata = []

        if delivered == 0.0:
            lumidata = [str (runnum), 'N/A', 'N/A', 'N/A']
        else:
            lumidata = [str (runnum), str (totalls), '%.3f'%delivered, parameters.beammode]
        return lumidata
    except Exception, e:
        print str (e)
        dbsession.transaction().rollback()
        del dbsession


def deliveredLumiForRange (dbsession, parameters, fileparsingResult):
    """ in this case, only take run numbers from theinput file """
    lumidata = []
    for run in sorted( fileparsingResult.runs() ):
        lumidata.append( deliveredLumiForRun (dbsession, parameters, run) )
    return lumidata

def recordedLumiForRun (dbsession, parameters, runnum, lslist = None):
    """
    lslist = None means to take all in the db
    output: ['runnumber', 'trgtable{}', 'deadtable{}']
    """
    recorded = 0.0
    lumidata = [] #[runnumber, trgtable, deadtable]
    trgtable = {} #{hltpath:[l1seed, hltprescale, l1prescale]}
    deadtable = {} #{lsnum:[deadtime, instlumi, bit_0, norbits]}
    lumidata.append (runnum)
    lumidata.append (trgtable)
    lumidata.append (deadtable)
    collectedseeds = [] #[ (hltpath, l1seed)]
    try:
        dbsession.transaction().start (True)
        schema = dbsession.nominalSchema()
        query = schema.newQuery()
        query.addToTableList (nameDealer.cmsrunsummaryTableName(), 'cmsrunsummary')
        query.addToTableList (nameDealer.trghltMapTableName(), 'trghltmap')#small table first
        queryCondition = coral.AttributeList()
        queryCondition.extend ("runnumber", "unsigned int")
        queryCondition["runnumber"].setData (int (runnum))
        query.setCondition ("trghltmap.HLTKEY = cmsrunsummary.HLTKEY AND cmsrunsummary.RUNNUM = :runnumber",
                            queryCondition)
        query.addToOutputList ("trghltmap.HLTPATHNAME", "hltpathname")
        query.addToOutputList ("trghltmap.L1SEED", "l1seed")
        result = coral.AttributeList()
        result.extend ("hltpathname", "string")
        result.extend ("l1seed", "string")
        query.defineOutput (result)
        cursor = query.execute()
        while cursor.next():
            hltpathname = cursor.currentRow()["hltpathname"].data()
            l1seed = cursor.currentRow()["l1seed"].data()
            collectedseeds.append ( (hltpathname, l1seed))
        #print 'collectedseeds ', collectedseeds
        del query
        dbsession.transaction().commit()
        #loop over hltpath
        for (hname, sname) in collectedseeds:
            l1bitname = hltTrgSeedMapper.findUniqueSeed (hname, sname)
            #print 'found unque seed ', hname, l1bitname
            if l1bitname:
                lumidata[1][hname] = []
                lumidata[1][hname].append (l1bitname.replace ('\"', ''))
        dbsession.transaction().start (True)
        schema = dbsession.nominalSchema()
        hltprescQuery = schema.tableHandle (nameDealer.hltTableName()).newQuery()
        hltprescQuery.addToOutputList ("PATHNAME", "hltpath")
        hltprescQuery.addToOutputList ("PRESCALE", "hltprescale")
        hltprescCondition = coral.AttributeList()
        hltprescCondition.extend ('runnumber', 'unsigned int')
        hltprescCondition.extend ('cmslsnum', 'unsigned int')
        hltprescCondition.extend ('inf', 'unsigned int')
        hltprescResult = coral.AttributeList()
        hltprescResult.extend ('hltpath', 'string')
        hltprescResult.extend ('hltprescale', 'unsigned int')
        hltprescQuery.defineOutput (hltprescResult)
        hltprescCondition['runnumber'].setData (int (runnum))
        hltprescCondition['cmslsnum'].setData (1)
        hltprescCondition['inf'].setData (0)
        hltprescQuery.setCondition ("RUNNUM = :runnumber and CMSLSNUM = :cmslsnum and PRESCALE != :inf",
                                    hltprescCondition)
        cursor = hltprescQuery.execute()
        while cursor.next():
            hltpath = cursor.currentRow()['hltpath'].data()
            hltprescale = cursor.currentRow()['hltprescale'].data()
            if lumidata[1].has_key (hltpath):
                lumidata[1][hltpath].append (hltprescale)
                
        cursor.close()
        del hltprescQuery
        dbsession.transaction().commit()
        
        dbsession.transaction().start (True)
        schema = dbsession.nominalSchema()
        query = schema.newQuery()
        query.addToTableList (nameDealer.trgTableName(), 'trg')
        query.addToTableList (nameDealer.lumisummaryTableName(), 'lumisummary')#small table first--right-most
        queryCondition = coral.AttributeList()
        queryCondition.extend ("runnumber", "unsigned int")
        queryCondition.extend ("lumiversion", "string")
        #queryCondition.extend ("alive", "bool")
        queryCondition["runnumber"].setData (int (runnum))
        queryCondition["lumiversion"].setData (parameters.lumiversion)
        #queryCondition["alive"].setData (True)
        query.setCondition ("lumisummary.RUNNUM = :runnumber and lumisummary.LUMIVERSION = :lumiversion AND lumisummary.CMSLSNUM = trg.CMSLSNUM and lumisummary.RUNNUM = trg.RUNNUM", queryCondition)
        #query.setCondition ("trg.RUNNUM = :runnumber AND lumisummary.RUNNUM = :runnumber and lumisummary.LUMIVERSION = :lumiversion AND lumisummary.CMSLSNUM = trg.CMSLSNUM AND lumisummary.cmsalive = :alive AND trg.BITNUM = :bitnum", queryCondition)
        #query.addToOutputList ("sum (lumisummary.INSTLUMI* (1-trg.DEADTIME/ (lumisummary.numorbit*3564)))", "recorded")
        query.addToOutputList ("lumisummary.CMSLSNUM", "cmsls")
        query.addToOutputList ("lumisummary.INSTLUMI", "instlumi")
        query.addToOutputList ("lumisummary.NUMORBIT", "norbits")
        query.addToOutputList ("trg.TRGCOUNT",         "trgcount")
        query.addToOutputList ("trg.BITNAME",          "bitname")
        query.addToOutputList ("trg.DEADTIME",         "trgdeadtime")
        query.addToOutputList ("trg.PRESCALE",         "trgprescale")
        query.addToOutputList ("trg.BITNUM",           "trgbitnum")
        #query.addToOrderList ("trg.BITNAME")
        #query.addToOrderList ("trg.CMSLSNUM")

        result = coral.AttributeList()
        result.extend ("cmsls",       "unsigned int")
        result.extend ("instlumi",    "float")
        result.extend ("norbits",     "unsigned int")
        result.extend ("trgcount",    "unsigned int")
        result.extend ("bitname",     "string")
        result.extend ("trgdeadtime", "unsigned long long")
        result.extend ("trgprescale", "unsigned int")
        result.extend ("trgbitnum",   "unsigned int")
        trgprescalemap = {}
        query.defineOutput (result)
        cursor = query.execute()
        while cursor.next():
            cmsls       = cursor.currentRow()["cmsls"].data()
            instlumi    = cursor.currentRow()["instlumi"].data()*parameters.norm
            norbits     = cursor.currentRow()["norbits"].data()
            trgcount    = cursor.currentRow()["trgcount"].data()
            trgbitname  = cursor.currentRow()["bitname"].data()
            trgdeadtime = cursor.currentRow()["trgdeadtime"].data()
            trgprescale = cursor.currentRow()["trgprescale"].data()
            trgbitnum   = cursor.currentRow()["trgbitnum"].data()
            if cmsls == 1:
                if not trgprescalemap.has_key (trgbitname):
                    trgprescalemap[trgbitname] = trgprescale
            if trgbitnum == 0:
                if not deadtable.has_key (cmsls):
                    deadtable[cmsls] = []
                    deadtable[cmsls].append (trgdeadtime)
                    deadtable[cmsls].append (instlumi)
                    deadtable[cmsls].append (trgcount)
                    deadtable[cmsls].append (norbits)
        cursor.close()
        del query
        dbsession.transaction().commit()
        
        #
        #consolidate results
        #
        #trgtable
        #print 'trgprescalemap', trgprescalemap
        #print lumidata[1]
        for hpath, trgdataseq in lumidata[1].items():   
            bitn = trgdataseq[0]
            if trgprescalemap.has_key (bitn) and len (trgdataseq) == 2:
                lumidata[1][hpath].append (trgprescalemap[bitn])                
        #filter selected cmsls
        lumidata[2] = filterDeadtable (deadtable, lslist)
        if not parameters.noWarnings:
            for lumi, deaddata in lumidata[2].items():
                if deaddata[1] == 0.0:
                    print '[Warning] : run %d :ls %d has 0 instlumi ' % (runnum, lumi)
                if deaddata[2] == 0 or deaddata[0] == 0:
                    print '[Warning] : run %d :ls %d has 0 deadtime and/or 0 zerobias bit counts' % (runnum, lumi)
        #print 'lumidata[2] ', lumidata[2]
    except Exception, e:
        print str (e)
        dbsession.transaction().rollback()
        del dbsession
    #print 'before return lumidata ', lumidata
    return lumidata


def filterDeadtable (inTable, lslist):
    result = {}
    if lslist is None:
        return inTable
    if len (lslist) == 0: #if request no ls, then return nothing
        return result
    for existingLS in inTable.keys():
        if existingLS in lslist:
            result[existingLS] = inTable[existingLS]
    return result


def recordedLumiForRange (dbsession, parameters, fileparsingResult):
    '''in this case, only take run numbers from theinput file'''
    lumidata = []
    for (run, lslist) in sorted (fileparsingResult.runsandls().items() ):
        #print 'processing run ', run
        #print 'valid ls list ', lslist
        lumidata.append( recordedLumiForRun (dbsession, parameters, run, lslist) )
    return lumidata


def printDeliveredLumi (lumidata, mode):
    labels = [ ('Run', 'Delivered LS', 'Delivered'+u' (/\u03bcb)'.encode ('utf-8'), 'Beam Mode')]
    print tablePrinter.indent (labels+lumidata, hasHeader = True, separateRows = False,
                               prefix = '| ', postfix = ' |', justify = 'right',
                               delim = ' | ', wrapfunc = lambda x: wrap_onspace (x, 20) )

def dumpData (lumidata, filename):
    """
    input params: lumidata [{'fieldname':value}]
                  filename csvname
    """
    
    r = csvReporter.csvReporter (filename)
    r.writeRows (lumidata)


def calculateTotalRecorded (deadtable):
    """
    input: {lsnum:[deadtime, instlumi, bit_0, norbits]}
    output: recordedLumi
    """
    recordedLumi = 0.0
    for myls, d in deadtable.items():
        instLumi = d[1]
        #deadfrac = float (d[0])/float (d[2]*3564)
        #print myls, float (d[2])
        if float (d[2]) == 0.0:
            deadfrac = 1.0
        else:
            deadfrac = float (d[0])/float (d[2])
        lstime = lslengthsec (d[3], 3564)
        recordedLumi += instLumi* (1.0-deadfrac)*lstime
    return recordedLumi


def splitlistToRangeString (inPut):
    result = []
    first = inPut[0]
    last = inPut[0]
    result.append ([inPut[0]])
    counter = 0
    for i in inPut[1:]:
        if i == last+1:
            result[counter].append (i)
        else:
            counter += 1
            result.append ([i])
        last = i
    return ', '.join (['['+str (min (x))+'-'+str (max (x))+']' for x in result])


def calculateEffective (trgtable, totalrecorded):
    """
    input: trgtable{hltpath:[l1seed, hltprescale, l1prescale]}, totalrecorded (float)
    output:{hltpath, recorded}
    """
    #print 'inputtrgtable', trgtable
    result = {}
    for hltpath, data in trgtable.items():
        if len (data) ==  3:
            result[hltpath] = totalrecorded/ (data[1]*data[2])
        else:
            result[hltpath] = 0.0
    return result


def getDeadfractions (deadtable):
    """
    inputtable: {lsnum:[deadtime, instlumi, bit_0, norbits]}
    output: {lsnum:deadfraction}
    """
    result = {}
    for myls, d in deadtable.items():
        #deadfrac = float (d[0])/ (float (d[2])*float (3564))
        if float (d[2]) == 0.0: ##no beam
            deadfrac = -1.0
        else:
            deadfrac = float (d[0])/ (float (d[2]))
        result[myls] = deadfrac
    return result

def printPerLSLumi (lumidata, isVerbose = False):
    '''
    input lumidata  [['runnumber', 'trgtable{}', 'deadtable{}']]
    deadtable {lsnum:[deadtime, instlumi, bit_0, norbits]}
    '''
    datatoprint = []
    totalrow = []
    labels = [ ('Run', 'LS', 'Delivered', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'))]
    lastrowlabels = [ ('Selected LS', 'Delivered'+u' (/\u03bcb)'.encode ('utf-8'), 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'))]
    totalDeliveredLS = 0
    totalSelectedLS = 0
    totalDelivered = 0.0
    totalRecorded = 0.0
    
    for perrundata in lumidata:
        runnumber = perrundata[0]
        deadtable = perrundata[2]
        lumiresult = lsBylsLumi (deadtable)
        totalSelectedLS = totalSelectedLS+len (deadtable)
        for lsnum, dataperls in lumiresult.items():
            rowdata = []
            if len (dataperls) == 0:
                rowdata  +=  [str (runnumber), str (lsnum), 'N/A', 'N/A']
            else:
                rowdata  +=  [str (runnumber), str (lsnum), '%.3f' % (dataperls[0]), '%.3f' % (dataperls[1])]
                totalDelivered = totalDelivered+dataperls[0]
                totalRecorded = totalRecorded+dataperls[1]
            datatoprint.append (rowdata)
    totalrow.append ([str (totalSelectedLS), '%.3f'% (totalDelivered), '%.3f'% (totalRecorded)])
    print ' ==  = '
    print tablePrinter.indent (labels+datatoprint, hasHeader = True, separateRows = False, prefix = '| ',
                               postfix = ' |', justify = 'right', delim = ' | ',
                               wrapfunc = lambda x: wrap_onspace_strict (x, 22))
    print ' ==  =  Total : '
    print tablePrinter.indent (lastrowlabels+totalrow, hasHeader = True, separateRows = False, prefix = '| ',
                               postfix = ' |', justify = 'right', delim = ' | ',
                               wrapfunc = lambda x: wrap_onspace (x, 20))    

    
def dumpPerLSLumi (lumidata):
    datatodump = []
    for perrundata in lumidata:
        runnumber = perrundata[0]
        deadtable = perrundata[2]
        lumiresult = lsBylsLumi (deadtable)
        for lsnum, dataperls in lumiresult.items():
            rowdata = []
            if len (dataperls) == 0:
                rowdata += [str (runnumber), str (lsnum), 'N/A', 'N/A']
            else:
                rowdata += [str (runnumber), str (lsnum), dataperls[0], dataperls[1]]
            datatodump.append (rowdata)
    return datatodump


def printRecordedLumi (lumidata, isVerbose = False, hltpath = ''):
    datatoprint = []
    totalrow = []
    labels = [ ('Run', 'HLT path', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'))]
    lastrowlabels = [ ('Selected LS', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'))]
    if len (hltpath) != 0 and hltpath != 'all':
        lastrowlabels = [ ('Selected LS', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'),
                           'Effective '+u' (/\u03bcb) '.encode ('utf-8')+hltpath)]
    if isVerbose:
        labels = [ ('Run', 'HLT-path', 'L1-bit', 'L1-presc', 'HLT-presc', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'))]
    totalSelectedLS = 0
    totalRecorded = 0.0
    totalRecordedInPath = 0.0
    
    for dataperRun in lumidata:
        runnum = dataperRun[0]
        if len (dataperRun[1]) == 0:
            rowdata = []
            rowdata += [str (runnum)]+2*['N/A']
            datatoprint.append (rowdata)
            continue
        perlsdata = dataperRun[2]
        totalSelectedLS = totalSelectedLS+len (perlsdata)
        recordedLumi = 0.0
        #norbits = perlsdata.values()[0][3]
        recordedLumi = calculateTotalRecorded (perlsdata)
        totalRecorded = totalRecorded+recordedLumi
        trgdict = dataperRun[1]
        effective = calculateEffective (trgdict, recordedLumi)
        if trgdict.has_key (hltpath) and effective.has_key (hltpath):
            rowdata = []
            l1bit = trgdict[hltpath][0]
            if len (trgdict[hltpath]) !=  3:
                if not isVerbose:
                    rowdata += [str (runnum), hltpath, 'N/A']
                else:
                    rowdata += [str (runnum), hltpath, l1bit, 'N/A', 'N/A', 'N/A']
            else:
                if not isVerbose:
                    rowdata += [str (runnum), hltpath, '%.3f'% (effective[hltpath])]
                else:
                    hltprescale = trgdict[hltpath][1]
                    l1prescale = trgdict[hltpath][2]
                    rowdata += [str (runnum), hltpath, l1bit, str (l1prescale), str (hltprescale),
                                '%.3f'% (effective[hltpath])]
                totalRecordedInPath = totalRecordedInPath+effective[hltpath]
            datatoprint.append (rowdata)
            continue
        
        for trg, trgdata in trgdict.items():
            #print trg, trgdata
            rowdata = []                    
            if trg == trgdict.keys()[0]:
                rowdata += [str (runnum)]
            else:
                rowdata += ['']
            l1bit = trgdata[0]
            if len (trgdata) == 3:
                if not isVerbose:
                    rowdata += [trg, '%.3f'% (effective[trg])]
                else:
                    hltprescale = trgdata[1]
                    l1prescale = trgdata[2]
                    rowdata += [trg, l1bit, str (l1prescale), str (hltprescale), '%.3f'% (effective[trg])]
            else:
                if not isVerbose:
                    rowdata += [trg, 'N/A']
                else:
                    rowdata += [trg, l1bit, 'N/A', 'N/A', '%.3f'% (effective[trg])]
            datatoprint.append (rowdata)
    #print datatoprint
    print ' ==  = '
    print tablePrinter.indent (labels+datatoprint, hasHeader = True, separateRows = False, prefix = '| ',
                               postfix = ' |', justify = 'right', delim = ' | ',
                               wrapfunc = lambda x: wrap_onspace_strict (x, 22))

    if len (hltpath) != 0 and hltpath != 'all':
        totalrow.append ([str (totalSelectedLS), '%.3f'% (totalRecorded), '%.3f'% (totalRecordedInPath)])
    else:
        totalrow.append ([str (totalSelectedLS), '%.3f'% (totalRecorded)])
    print ' ==  =  Total : '
    print tablePrinter.indent (lastrowlabels+totalrow, hasHeader = True, separateRows = False, prefix = '| ',
                               postfix = ' |', justify = 'right', delim = ' | ',
                               wrapfunc = lambda x: wrap_onspace (x, 20))    
    if isVerbose:
        deadtoprint = []
        deadtimelabels = [ ('Run', 'Lumi section : Dead fraction')]

        for dataperRun in lumidata:
            runnum = dataperRun[0]
            if len (dataperRun[1]) == 0:
                deadtoprint.append ([str (runnum), 'N/A'])
                continue
            perlsdata = dataperRun[2]
            #print 'perlsdata 2 : ', perlsdata
            deadT = getDeadfractions (perlsdata)
            t = ''
            for myls, de in deadT.items():
                if de<0:
                    t += str (myls)+':nobeam '
                else:
                    t += str (myls)+':'+'%.5f'% (de)+' '
            deadtoprint.append ([str (runnum), t])
        print ' ==  = '
        print tablePrinter.indent (deadtimelabels+deadtoprint, hasHeader = True, separateRows = True, prefix = '| ',
                                   postfix = ' |', justify = 'right', delim = ' | ',
                                   wrapfunc = lambda x: wrap_onspace (x, 80))


def dumpRecordedLumi (lumidata, hltpath = ''):
    #labels = ['Run', 'HLT path', 'Recorded']
    datatodump = []
    for dataperRun in lumidata:
        runnum = dataperRun[0]
        if len (dataperRun[1]) == 0:
            rowdata = []
            rowdata += [str (runnum)]+2*['N/A']
            datatodump.append (rowdata)
            continue
        perlsdata = dataperRun[2]
        recordedLumi = 0.0
        #norbits = perlsdata.values()[0][3]
        recordedLumi = calculateTotalRecorded (perlsdata)
        trgdict = dataperRun[1]
        effective = calculateEffective (trgdict, recordedLumi)
        if trgdict.has_key (hltpath) and effective.has_key (hltpath):
            rowdata = []
            l1bit = trgdict[hltpath][0]
            if len (trgdict[hltpath]) !=  3:
                rowdata += [str (runnum), hltpath, 'N/A']
            else:
                hltprescale = trgdict[hltpath][1]
                l1prescale = trgdict[hltpath][2]
                rowdata += [str (runnum), hltpath, effective[hltpath]]
            datatodump.append (rowdata)
            continue
        
        for trg, trgdata in trgdict.items():
            #print trg, trgdata
            rowdata = []                    
            rowdata += [str (runnum)]
            l1bit = trgdata[0]
            if len (trgdata) == 3:
                rowdata += [trg, effective[trg]]
            else:
                rowdata += [trg, 'N/A']
            datatodump.append (rowdata)
    return datatodump


def printOverviewData (delivered, recorded, hltpath = ''):
    if len (hltpath) == 0 or hltpath == 'all':
        toprowlabels = [ ('Run', 'Delivered LS', 'Delivered'+u' (/\u03bcb)'.encode ('utf-8'), 'Selected LS', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8') )]
        lastrowlabels = [ ('Delivered LS', 'Delivered'+u' (/\u03bcb)'.encode ('utf-8'), 'Selected LS', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8') ) ]
    else:
        toprowlabels = [ ('Run', 'Delivered LS', 'Delivered'+u' (/\u03bcb)'.encode ('utf-8'), 'Selected LS', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'), 'Effective'+u' (/\u03bcb) '.encode ('utf-8')+hltpath )]
        lastrowlabels = [ ('Delivered LS', 'Delivered'+u' (/\u03bcb)'.encode ('utf-8'), 'Selected LS', 'Recorded'+u' (/\u03bcb)'.encode ('utf-8'), 'Effective '+u' (/\u03bcb) '.encode ('utf-8')+hltpath)]
    datatable = []
    totaldata = []
    totalDeliveredLS = 0
    totalSelectedLS = 0
    totalDelivered = 0.0
    totalRecorded = 0.0
    totalRecordedInPath = 0.0
    totaltable = []
    for runidx, deliveredrowdata in enumerate (delivered):
        rowdata = []
        rowdata += [deliveredrowdata[0], deliveredrowdata[1], deliveredrowdata[2]]
        if deliveredrowdata[1] == 'N/A': #run does not exist
            if  hltpath != '' and hltpath != 'all':
                rowdata += ['N/A', 'N/A', 'N/A']
            else:
                rowdata += ['N/A', 'N/A']
            datatable.append (rowdata)
            continue
        totalDeliveredLS += int (deliveredrowdata[1])
        totalDelivered += float (deliveredrowdata[2])
        
        selectedls = recorded[runidx][2].keys()
        #print 'runidx ', runidx, deliveredrowdata
        #print 'selectedls ', selectedls
        if len (selectedls) == 0:
            selectedlsStr = '[]'
            recordedLumi = 0
            if  hltpath != '' and hltpath != 'all':
                rowdata += [selectedlsStr, 'N/A', 'N/A']
            else:
                rowdata += [selectedlsStr, 'N/A']
        else:
            selectedlsStr = splitlistToRangeString (selectedls)
            recordedLumi = calculateTotalRecorded (recorded[runidx][2])
            lumiinPaths = calculateEffective (recorded[runidx][1], recordedLumi)
            if hltpath != '' and hltpath != 'all':
                if lumiinPaths.has_key (hltpath):
                    rowdata += [selectedlsStr, '%.3f'% (recordedLumi), '%.3f'% (lumiinPaths[hltpath])]
                    totalRecordedInPath += lumiinPaths[hltpath]
                else:
                    rowdata += [selectedlsStr, '%.3f'% (recordedLumi), 'N/A']
            else:
                #rowdata += [selectedlsStr, '%.3f'% (recordedLumi), '%.3f'% (recordedLumi)]
                rowdata += [selectedlsStr, '%.3f'% (recordedLumi)]
        totalSelectedLS += len (selectedls)
        totalRecorded += recordedLumi
        datatable.append (rowdata)
    if hltpath != '' and hltpath != 'all':
        totaltable = [[str (totalDeliveredLS), '%.3f'% (totalDelivered), str (totalSelectedLS),
                       '%.3f'% (totalRecorded), '%.3f'% (totalRecordedInPath)]]
    else:
        totaltable = [[str (totalDeliveredLS), '%.3f'% (totalDelivered), str (totalSelectedLS),
                       '%.3f'% (totalRecorded)]]
    print tablePrinter.indent (toprowlabels+datatable, hasHeader = True, separateRows = False, prefix = '| ',
                               postfix = ' |', justify = 'right', delim = ' | ',
                               wrapfunc = lambda x: wrap_onspace (x, 20))
    print ' ==  =  Total : '
    print tablePrinter.indent (lastrowlabels+totaltable, hasHeader = True, separateRows = False, prefix = '| ',
                               postfix = ' |', justify = 'right', delim = ' | ',
                               wrapfunc = lambda x: wrap_onspace (x, 20))


def dumpOverview (delivered, recorded, hltpath = ''):
    #toprowlabels = ['run', 'delivered', 'recorded', 'hltpath']
    datatable = []
    for runidx, deliveredrowdata in enumerate (delivered):
        rowdata = []
        rowdata += [deliveredrowdata[0], deliveredrowdata[2]]
        if deliveredrowdata[1] == 'N/A': #run does not exist
            rowdata += ['N/A', 'N/A']
            datatable.append (rowdata)
            continue
        recordedLumi = calculateTotalRecorded (recorded[runidx][2])
        lumiinPaths = calculateEffective (recorded[runidx][1], recordedLumi)
        if hltpath != '' and hltpath != 'all':
            if lumiinPaths.has_key (hltpath):
                rowdata += [recordedLumi, lumiinPaths[hltpath]]
            else:
                rowdata += [recordedLumi, 'N/A']
        else:
            rowdata += [recordedLumi, recordedLumi]
        datatable.append (rowdata)
    return datatable


def xingLuminosityForRun (dbsession, runnum, parameters, minLumValue = 1e-3, lumiXingDict = {}):
    '''Given a run number and a minimum xing luminosity value,
    returns a dictionary (keyed by (run, lumi section)) where the
    value is a list of tuples of (xingID, xingLum).

    - For all xing luminosities, simply set minLumValue to 0.

    - If you want one dictionary for several runs, pass it in to
      "lumiXingDict"


    select 
    s.cmslsnum, d.bxlumivalue, d.bxlumierror, d.bxlumiquality, d.algoname from LUMIDETAIL d, LUMISUMMARY s where s.runnum = 133885 and d.algoname = 'OCC1' and s.lumisummary_id = d.lumisummary_id order by s.startorbit, s.cmslsnum
    '''
    try:
        runnum = int (runnum)
        dbsession.transaction().start (True)
        schema = dbsession.schema (parameters.lumischema)
        if not schema:
            raise 'cannot connect to schema ', parameters.lumischema
        detailOutput = coral.AttributeList()
        detailOutput.extend ('startorbit',    'unsigned int')
        detailOutput.extend ('cmslsnum',      'unsigned int')
        detailOutput.extend ('bxlumivalue',   'blob')
        detailOutput.extend ('bxlumierror',   'blob')
        detailOutput.extend ('bxlumiquality', 'blob')
        detailOutput.extend ('algoname',      'string')
        detailCondition = coral.AttributeList()
        detailCondition.extend ('runnum',   'unsigned int')
        detailCondition.extend ('algoname', 'string')
        detailCondition['runnum'].setData (runnum)
        detailCondition['algoname'].setData ('OCC1')
        query = schema.newQuery()
        query.addToTableList  (parameters.lumisummaryname, 's')
        query.addToTableList  (parameters.lumidetailname,  'd')
        #query.addToOrderList  ('s.STARTORBIT',    'startorbit')
        query.addToOutputList ('s.STARTORBIT',    'startorbit')
        query.addToOutputList ('s.CMSLSNUM',      'cmslsnum')
        query.addToOutputList ('d.BXLUMIVALUE',   'bxlumivalue')
        query.addToOutputList ('d.BXLUMIERROR',   'bxlumierror')
        query.addToOutputList ('d.BXLUMIQUALITY', 'bxlumiquality')
        query.addToOutputList ('d.ALGONAME',      'algoname')
        query.setCondition ('s.RUNNUM = :runnum and d.ALGONAME = :algoname and s.LUMISUMMARY_ID = d.LUMISUMMARY_ID',
                            detailCondition)
        query.addToOrderList ('s.CMSLSNUM')
        query.defineOutput (detailOutput)
        cursor = query.execute()
        count = 0
        while cursor.next():
            ## ## Note: If you are going to break out of this loop early,
            ## ## make sure you call cursor.close():
            ## 
            ## if count > 20 :
            ##     cursor.close()
            ##     break
            ## count  +=  1
            cmslsnum    = cursor.currentRow()['cmslsnum'].data()
            algoname    = cursor.currentRow()['algoname'].data()
            bxlumivalue = cursor.currentRow()['bxlumivalue'].data()
            startorbit  = cursor.currentRow()['startorbit'].data()
            
            xingArray = array.array ('f')
            xingArray.fromstring( bxlumivalue.readline() )
            numPrinted = 0
            xingLum = []
            for index, lum in enumerate (xingArray):
                lum  *=  parameters.normFactor
                if lum < minLumValue:
                    continue
                xingLum.append( (index, lum) )
            lumiXingDict[ (runnum, cmslsnum) ] = xingLum
        del query
        dbsession.transaction().commit()
        return lumiXingDict      
    except Exception, e:
        print str (e)
        print "whoops"
        dbsession.transaction().rollback()
        del dbsession


def flatten (obj):
    '''Given nested lists or tuples, returns a single flattened list'''
    result = []
    for piece in obj:
        if hasattr (piece, '__iter__') and not isinstance (piece, basestring):
            result.extend( flatten (piece) )
        else:
            result.append (piece)
    return result    


def mergeXingLumi (generalInfo, xingLumiDict):
    '''Given general xing information and a xingLumiDict, the xing
    luminosity information is merged with the general information'''
    for line in generalInfo:
        if len (line) < 2:
            continue
        try:
            key = ( int (line[0]), int (line[1]) )
        except:
            # if we're here, then these aren't integers so we want to
            # skip the line
            continue
        xingLumiValues = xingLumiDict.get (key)
        if xingLumiValues:
            line.extend( flatten (xingLumiValues) )
            

##############################
## ######################## ##
## ## ################## ## ##
## ## ## Main Program ## ## ##
## ## ################## ## ##
## ######################## ##
##############################

if __name__ == '__main__':
    parameters = ParametersObject()
    #parser = argparse.ArgumentParser (description = "Lumi Calculations")
    allowedActions = ['overview', 'delivered', 'recorded', 'lumibyls', 'lumibylsXing']
    beamModeChoices = [ "stable", "quiet", "either"]
    parser = optparse.OptionParser ("Usage: %%prog [--options] action\naction one of %s" % allowedActions,
                                    description = "Lumi Calculations")
    # add optional arguments
    parser.add_option ('--parameters', dest = 'connect', action = 'store',
                         help = 'connect string to lumiDB (optional, default to frontier://LumiProd/CMS_LUMI_PROD)')
    parser.add_option ('-P', dest = 'authpath', action = 'store',
                         help = 'path to authentication file')
    parser.add_option ('-n', dest = 'normfactor', type='float', default=1.,
                         help = 'normalization factor (default %default)')
    parser.add_option ('-r', dest = 'runnumber', action = 'store',
                         help = 'run number')
    parser.add_option ('-i', dest = 'inputfile', action = 'store',
                         help = 'lumi range selection file')
    parser.add_option ('-o', dest = 'outputfile', action = 'store',
                         help = 'output to csv file')
    parser.add_option ('-b', dest = 'beammode', default='stable', choices=beamModeChoices,
                         help = "beam mode, optional for delivered action, default ('%%default' out of %s)" % beamModeChoices)
    parser.add_option ('--lumiversion', dest = 'lumiversion', type='string', default='0001',
                         help = 'lumi data version, optional for all, default %default')
    parser.add_option ('--hltpath', dest = 'hltpath', action = 'store',
                         help = 'specific hltpath to calculate the recorded luminosity, default to all')
    parser.add_option ('--siteconfpath', dest = 'siteconfpath', action = 'store',
                         help = 'specific path to site-local-config.xml file, default to $CMS_PATH/SITECONF/local/JobConfig, ' \
                         'if path undefined, fallback to cern proxy&server')
    parser.add_option ('--verbose', dest = 'verbose', action = 'store_true', default = False,
                         help = 'verbose mode for printing' )
    parser.add_option ('--nowarning', dest = 'nowarning', action = 'store_true', default = False,
                         help = 'suppress bad for lumi warnings' )
    parser.add_option ('--debug', dest = 'debug', action = 'store_true',
                         help = 'debug')
    parser.add_option ('--xingMinLum', dest = 'xingMinLum', type='float', default=1e-3,
                         help = 'Minimum luminosity considered for "lsbylsXing" action, default %default')
    # parse arguments
    (options, args) = parser.parse_args()
    if len (args) != 1:
        parser.print_usage()
        raise RuntimeError, "Exactly one action must be provided"
    action = args[0]
    if action not in allowedActions:
        raise RuntimeError, "Action must be one of %s" % allowedActions

    connectstring = options.connect or 'frontier://LumiProd/CMS_LUMI_PROD'
    connectparser = connectstrParser.connectstrParser (connectstring)
    connectparser.parse()
    usedefaultfrontierconfig = False
    cacheconfigpath = ''
    if connectparser.needsitelocalinfo():
        if not options.siteconfpath:
            cacheconfigpath = os.environ['CMS_PATH']
            if cacheconfigpath:
                cacheconfigpath = os.path.join (cacheconfigpath, 'SITECONF', 'local', 'JobConfig', 'site-local-config.xml')
            else:
                usedefaultfrontierconfig = True
        else:
            cacheconfigpath = options.siteconfpath
            cacheconfigpath = os.path.join (cacheconfigpath, 'site-local-config.xml')
        ccp = cacheconfigParser.cacheconfigParser()
        if usedefaultfrontierconfig:
            ccp.parseString (parameters.defaultfrontierConfigString)
        else:
            ccp.parse (cacheconfigpath)
        connectstring = connectparser.fullfrontierStr (connectparser.schemaname(), ccp.parameterdict())
    svc = coral.ConnectionService()
    if options.debug :
        msg = coral.MessageStream ('')
        msg.setMsgVerbosity (coral.message_Level_Debug)
        parameters.verbose = True
    parameters.verbose = options.verbose
    parameters.noWarnings = options.nowarning
    if options.authpath:
        os.environ['CORAL_AUTH_PATH'] = options.authpath
    parameters.norm = options.normfactor
    parameters.lumiversion = options.lumiversion
    parameters.beammode = options.beammode
    if not options.inputfile and not options.runnumber:
        raise "must specify either a run (-r) or an input run selection file (-i)"
    lumiXing = False
    if action ==  'lumibylsXing':
        action = 'lumibyls'
        lumiXing = True
        # we can't have lumiXing mode if we're not writing to a CSV
        # file
        if not options.outputfile:
            raise RuntimeError, "You must specify an outputt file in 'lumibylsXing' mode"
    session = svc.connect (connectstring, accessMode = coral.access_Update)
    session.typeConverter().setCppTypeForSqlType ("unsigned int", "NUMBER (10)")
    session.typeConverter().setCppTypeForSqlType ("unsigned long long", "NUMBER (20)")
    inputfilecontent = ''
    fileparsingResult = ''
    if not options.runnumber and options.inputfile:
        basename, extension = os.path.splitext (options.inputfile)
        if extension == '.csv': # if file ends with .csv, use csv parser, else parse as json file
            fileparsingResult = csvSelectionParser.csvSelectionParser (options.inputfile)
        else:
            f = open (options.inputfile, 'r')
            inputfilecontent = f.read()
            fileparsingResult =  selectionParser.selectionParser (inputfilecontent)
        if not fileparsingResult:
            print 'failed to parse the input file', options.inputfile
            raise 
    lumidata = []

    # Delivered
    if action ==  'delivered':
        if options.runnumber:
            lumidata.append (deliveredLumiForRun (session, parameters, options.runnumber))
        else:
            lumidata = deliveredLumiForRange (session, parameters, fileparsingResult)    
        if not options.outputfile:
            printDeliveredLumi (lumidata, '')
        else:
            lumidata.insert (0, ['run', 'nls', 'delivered', 'beammode'])
            dumpData (lumidata, options.outputfile)

    # Recorded
    if action ==  'recorded':
        hltpath = ''
        if options.hltpath:
            hltpath = options.hltpath
        if options.runnumber:
            lumidata.append (recordedLumiForRun (session, parameters, options.runnumber))
        else:
            lumidata = recordedLumiForRange (session, parameters, fileparsingResult)
        if not options.outputfile:
            printRecordedLumi (lumidata, parameters.verbose, hltpath)
        else:
            todump = dumpRecordedLumi (lumidata, hltpath)
            todump.insert (0, ['run', 'hltpath', 'recorded'])
            dumpData (todump, options.outputfile)

    # Overview
    if action ==  'overview':
        delivereddata = []
        recordeddata = []
        hltpath = ''
        if options.hltpath:
            hltpath = options.hltpath
        if options.runnumber:
            delivereddata.append (deliveredLumiForRun (session, parameters, options.runnumber))
            recordeddata.append (recordedLumiForRun (session, parameters, options.runnumber))
        else:
            delivereddata = deliveredLumiForRange (session, parameters, fileparsingResult)
            recordeddata = recordedLumiForRange (session, parameters, fileparsingResult)
        if not options.outputfile:
            printOverviewData (delivereddata, recordeddata, hltpath)
        else:
            todump = dumpOverview (delivereddata, recordeddata, hltpath)
            if not hltpath:
                hltpath = 'all'
            todump.insert (0, ['run', 'delivered', 'recorded', 'hltpath:'+hltpath])
            dumpData (todump, options.outputfile)

    # Lumi by lumisection
    if action ==  'lumibyls':
        recordeddata = []
        xingLumiDict = {}
        if options.runnumber:
            recordeddata.append( recordedLumiForRun (session, parameters , options.runnumber) )
            if lumiXing:
                # don't know if we really need this environment
                # variable or not...
                # os.environ['CORAL_AUTH_PATH'] = '/afs/cern.ch/cms/DB/lumi'
                xingLumiDict = xingLuminosityForRun (session, options.runnumber, parameters, options.xingMinLum)
        else:
            recordeddata = recordedLumiForRange (session, parameters, fileparsingResult)
        if not options.outputfile:
            printPerLSLumi (recordeddata, parameters.verbose)
        else:
            todump = dumpPerLSLumi (recordeddata)
            todump.insert (0, ['run', 'ls', 'delivered', 'recorded'])
            if lumiXing:
                mergeXingLumi (todump, xingLumiDict)
            dumpData (todump, options.outputfile)
    del session
    del svc
    
