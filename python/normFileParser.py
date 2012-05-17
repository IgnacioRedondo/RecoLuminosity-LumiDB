import ConfigParser,os.path

#################################
#norm file format spec
#lines beginning with a semicolon ';' a pound sign '#' or the letters 'REM' (uppercase or lowercase) will be ignored. 
#section uppercase
# [NORMDEFINITION] #section required only if first create
#   name=pp7TeV   #priority to commandline --name option if present
#   context=PROTPHYS_3500
#   comment=
#   lumitype=
#   iscontextdefault=
# [NORMDATA] # section required
#   since= #priority to commandline --since option if present
#   corrector=
#   norm_occ1=
#   norm_occ2=
#   ...
#################################

class normFileParser(object):
    def __init__(self,filename):
        self.__parser=ConfigParser.ConfigParser()
        self.__inifilename=filename
        self.__defsectionname='NormDefinition'
        self.__datasectionname='NormData'
    def parse(self):
        '''
        output:
           [{defoption:value},{dataoption:value}]
        '''
        if not os.path.exists(self.__inifilename) or not os.path.isfile(self.__inifilename):
            raise ValueError(self.__inifilename+' is not a file or does not exist')
        self.__parser.read(self.__inifilename)
        result=[]
        for section in [self.__defsectionname,self.__datasectionname]:
            options=self.__parser.options(section)
            sectionresult={}
            for o in options:
                try:
                    sectionresult[o]=self.__parser.get(section,o)
                    if sectionresult[o]==-1:
                        print 'skip: %s'%o
                except:
                    if section==self.__datasectionname:
                        print self.__datasectionname+' is required'
                        raise
                    result.append({})
                    continue
            result.append(sectionresult)
        return result
    
if __name__ == "__main__":
    s='testnorm.cfg'
    parser=normFileParser(s)
    print parser.parse()

