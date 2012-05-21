import re
class normFunctionFactory(object):
    '''
    luminorm and correction functions.
    The result of the functions are correction factors, not final luminosity
    all functions take 5 run time parameters, and arbituary named params
    '''
    
    def fConst(self,luminonorm,intglumi,nBXs,whatev,whatav,norm_occ1=1000.0):
        '''
        luminonorm in /mb
        by default only convert /mb to /ub
        '''
        return norm_occ1

    def fPolyAfterglowDrift(self,luminonorm,intglumi,nBXs,whatev,whatav,norm_occ1=1.0e03,norm_pu=0.0,a1=0.0,a2=0.0,drift=0.0):
        '''
        luminonorm in /mb
        '''
        avglumi=0.
        if nBXs>0:
            avglumi=norm_pu*luminonorm/nBXs
        afterglowmap=[]
        afterglowmap.append((213,0.992))
        afterglowmap.append((321,0.990))
        afterglowmap.append((423,0.988))
        afterglowmap.append((597,0.985))
        afterglowmap.append((700,0.984))
        afterglowmap.append((873,0.981))
        afterglowmap.append((1041,0.979))
        afterglowmap.append((1179,0.977)) 
        afterglowmap.append((1317,0.975))
        Afterglow=1.0
        for (bxthreshold,correction) in self.afterglowmap:
            if nBXs >= bxthreshold :
                Afterglow = correction
        driftterm=1.0
        if drift and intglumi:
            driftterm=drift*intglumi
        result=norm_occ1*Afterglow/(1+a1*avglumi+a2*avglumi*avglumi)*driftterm
        return result

    def fPolyDrift(self,luminonorm,intglumi,nBXs,whatev,whatav,norm_occ1=1.0e03,norm_pu=0.0,a1=0.0,a2=0.0,drift=0.0):
        '''
        luminonorm in /mb
        '''
        avglumi=0.
        if nBXs>0:
            avglumi=norm_pu*luminonorm/nBXs
        driftterm=1.0
        if drift and intglumi:
            driftterm=drift*intglumi
        result=norm_occ1/(1+a1*avglumi+a2*avglumi*avglumi)*driftterm
        return result
    
    def fPixelAfterglow(self,luminonorm,intglumi,nBXs,whatev,whatav,nBXs,norm_occ1=1.0):
        '''
        cannot parametrise because of 75ns
        '''
        Afterglow=1.0
        afterglowmap=[]
        afterglowmap.append((213,0.989))
        afterglowmap.append((423,0.985))
        afterglowmap.append((597,0.983))
        afterglowmap.append((699,0.98))
        afterglowmap.append((1041,0.976))
        afterglowmap.append((1179,0.974)) 
        afterglowmap.append((1317,0.972))        
        for (bxthreshold,correction) in self.afterglowmap:
            if nBXs >= bxthreshold :
                Afterglow = correction
        result=Afterglow*norm_occ1
        return result

    def fPixelFillScheme(self,luminonorm,intglumi,nBXs,fillschemeStr,fillschemePatterns,nBXs,norm_occ1=1.0):
        '''
        input: fillschemePatterns [(patternStr,afterglow])
        find afterglow by fillscheme patterm
        '''
        Afterglow=1.0
        for (apattern,cfactor) in afterglowPatterns:
        if re.match(apattern,fillscheme):
            Afterglow=cfactor
        return Afterglow*norm_occ1
    
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
    fConstParams={'norm_occ1':100.0}
    argvals=[123.,0.,1331,0.0,0.0]
    print normFunctionCaller('fConst',*argvals,**fConstParams)
    argvals=[123.,0.,1331,0.0,0.0]
    fPolyParams={'norm_occ1':6370.0,'drift':0.067}
    normFunctionCaller('fPolyDrift',*argvals,**fPolyParams)

