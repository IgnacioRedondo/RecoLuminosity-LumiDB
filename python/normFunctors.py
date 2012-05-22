import re,ast
class normFunctionFactory(object):
    '''
    luminorm and correction functions.
    The result of the functions are correction factors, not final luminosity
    all functions take 5 run time parameters, and arbituary named params
    '''

    def fPoly(self,luminonorm,intglumi,nBXs,whatev,whatav,a0=1.0e03,c1=0.0,a1=0.0,a2=0.0,drift=0.0,afterglow=''):
        '''
        default is just a /mb to /ub converter
        input: luminonorm in /mb

        '''
        avglumi=0.
        if c1 and nBXs>0:
            avglumi=c1*luminonorm/nBXs
        Afterglow=1.0
        if len(afterglow)!=0:
            afterglowmap=ast.literal_eval(afterglow)
            for (bxthreshold,correction) in afterglowmap:
                if nBXs >= bxthreshold :
                    Afterglow = correction
        driftterm=1.0
        if drift and intglumi:
            driftterm=drift*intglumi
        result=a0*Afterglow/(1+a1*avglumi+a2*avglumi*avglumi)*driftterm
        return result

    def fPolyScheme(self,luminonorm,intglumi,nBXs,fillschemeStr,fillschemePatterns,a0=1.0e03,c1=0.0,a1=0.0,a2=0.0,drift=0.0):
        '''
        input: fillschemePatterns [(patternStr,afterglow])
        '''
        avglumi=0.
        if c1 and nBXs>0:
            avglumi=c1*luminonorm/nBXs
        Afterglow=1.0
        if fillschemeStr and fillschemePatterns:
            for (apattern,cfactor) in afterglowPatterns:
                if re.match(apattern,fillscheme):
                    Afterglow=cfactor
        driftterm=1.0
        if drift and intglumi:
            driftterm=drift*intglumi
        result=a0*Afterglow/(1+a1*avglumi+a2*avglumi*avglumi)*driftterm
        return result
    
def normFunctionCaller(funcName,*args,**kwds):
    fac=normFunctionFactory()
    try:
        myfunc=getattr(fac,funcName,None)
    except AttributeError:
        print '[ERROR] unknown correction function '+funcName
        raise
    if callable(myfunc):
        return myfunc(*args,**kwds)
    else:
        raise ValueError('uncallable function '+funcName)
if __name__ == '__main__':
    luminonorm=23.0
    constParams={'a0':100.0}
    argvals=[123.,0.,1331,0.0,0.0]
    print normFunctionCaller('fPoly',*argvals,**constParams)
    argvals=[123.,0.,1331,0.0,0.0]
    polyParams={'a0':6370.0,'drift':0.067,'afterglow':'[(700,0.97),(1310,0.94)]'}
    print normFunctionCaller('fPoly',*argvals,**polyParams)

