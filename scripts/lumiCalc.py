#!/usr/bin/env python
VERSION = '2.00'
import os, sys
import coral
import array
import optparse
from RecoLuminosity.LumiDB import csvSelectionParser
from RecoLuminosity.LumiDB.wordWrappers import wrap_always, wrap_onspace, wrap_onspace_strict
import RecoLuminosity.LumiDB.lumiQueryAPI as LumiQueryAPI

from pprint import pprint


##############################
## ######################## ##
## ## ################## ## ##
## ## ## Main Program ## ## ##
## ## ################## ## ##
## ######################## ##
##############################

if __name__ == '__main__':
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
    if not args:
        parser.print_usage()
        sys.exit()
    if len (args) != 1:
        parser.print_usage()
        raise RuntimeError, "Exactly one action must be provided"
    action = args[0]
    if action not in allowedActions:
        raise RuntimeError, "Action must be one of %s" % allowedActions

    # get database session hooked up
    if options.authpath:
        os.environ['CORAL_AUTH_PATH'] = options.authpath
    session, svc =  LumiQueryAPI.setupSession (options.connect or \
                                               'frontier://LumiProd/CMS_LUMI_PROD',
                                               options.siteconfpath, options.debug)

    ## Save what we need in the parameters object
    parameters = LumiQueryAPI.ParametersObject()
    parameters.verbose     = options.verbose
    parameters.noWarnings  = options.nowarning
    parameters.norm        = options.normfactor
    parameters.lumiversion = options.lumiversion
    parameters.beammode    = options.beammode

    ## Let's start the fun
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
            lumidata.append ( LumiQueryAPI.deliveredLumiForRun (session, parameters, options.runnumber))
        else:
            lumidata =  LumiQueryAPI.deliveredLumiForRange (session, parameters, fileparsingResult)    
        if not options.outputfile:
             LumiQueryAPI.printDeliveredLumi (lumidata, '')
        else:
            lumidata.insert (0, ['run', 'nls', 'delivered', 'beammode'])
            LumiQueryAPI.dumpData (lumidata, options.outputfile)

    # Recorded
    if action ==  'recorded':
        hltpath = ''
        if options.hltpath:
            hltpath = options.hltpath
        if options.runnumber:
            lumidata.append ( LumiQueryAPI.recordedLumiForRun (session, parameters, options.runnumber))
        else:
            lumidata =  LumiQueryAPI.recordedLumiForRange (session, parameters, fileparsingResult)
        if not options.outputfile:
             LumiQueryAPI.printRecordedLumi (lumidata, parameters.verbose, hltpath)
        else:
            todump = dumpRecordedLumi (lumidata, hltpath)
            todump.insert (0, ['run', 'hltpath', 'recorded'])
            LumiQueryAPI.dumpData (todump, options.outputfile)

    # Overview
    if action ==  'overview':
        delivereddata = []
        recordeddata = []
        hltpath = ''
        if options.hltpath:
            hltpath = options.hltpath
        if options.runnumber:
            delivereddata.append (LumiQueryAPI.deliveredLumiForRun (session, parameters, options.runnumber))
            recordeddata.append  (LumiQueryAPI.recordedLumiForRun  (session, parameters, options.runnumber))
        else:
            delivereddata = LumiQueryAPI.deliveredLumiForRange (session, parameters, fileparsingResult)
            recordeddata  = LumiQueryAPI.recordedLumiForRange  (session, parameters, fileparsingResult)
        if not options.outputfile:
            LumiQueryAPI.printOverviewData (delivereddata, recordeddata, hltpath)
        else:
            todump =  LumiQueryAPI.dumpOverview (delivereddata, recordeddata, hltpath)
            if not hltpath:
                hltpath = 'all'
            todump.insert (0, ['run', 'delivered', 'recorded', 'hltpath:'+hltpath])
            LumiQueryAPI.dumpData (todump, options.outputfile)

    # Lumi by lumisection
    if action ==  'lumibyls':
        recordeddata = []
        xingLumiDict = {}
        if options.runnumber:
            recordeddata.append( LumiQueryAPI.recordedLumiForRun (session, parameters , options.runnumber) )
            if lumiXing:
                # don't know if we really need this environment
                # variable or not. (It works with out it here at FNAL...)
                # os.environ['CORAL_AUTH_PATH'] = '/afs/cern.ch/cms/DB/lumi'
                xingLumiDict =  LumiQueryAPI.xingLuminosityForRun (session, options.runnumber, parameters, options.xingMinLum)
        else:
            recordeddata =  LumiQueryAPI.recordedLumiForRange (session, parameters, fileparsingResult)
        if not options.outputfile:
            LumiQueryAPI.printPerLSLumi (recordeddata, parameters.verbose)
        else:
            todump =  LumiQueryAPI.dumpPerLSLumi (recordeddata)
            todump.insert (0, ['run', 'ls', 'delivered', 'recorded'])
            if lumiXing:
                 LumiQueryAPI.mergeXingLumi (todump, xingLumiDict)
            LumiQueryAPI.dumpData (todump, options.outputfile)
    ## del session
    ## del svc
    
