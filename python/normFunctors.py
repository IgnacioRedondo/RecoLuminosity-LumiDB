class normFunctionFactory(object):
    '''
    luminorm and correction functions.
    The result of the functions are correction factors, not final luminosity
    '''
    
    def fConst(self,normocc1=1000.0):
        '''
        by default only convert /mb to /ub
        '''
        return normocc1

    def fPolyAfterglowDrift(self,luminonorm,nBXs,intglumi,normocc1=1.0e03,punorm=0.0,a1=0.0,a2=0.0,drift=1.0):
        avglumi=0.
        if nBXs>0:
            avglumi=punorm*luminonorm/nBXs
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
        driftterm=drift
        if intglumi:
            driftterm=drift*intglumi
        result=normocc1*Afterglow/(1+a1*avglumi+a2*avglumi*avglumi)*driftterm
        return result

    def fPolyDrift(self,luminonorm,intglumi,normocc1=1.0e03,punorm=0.0,a1=0.0,a2=0.0,drift=1.0):
        avglumi=0.
        if nBXs>0:
            avglumi=punorm*luminonorm/nBXs
        driftterm=drift
        if intglumi:
            driftterm=drift*intglumi
        result=normocc1/(1+a1*avglumi+a2*avglumi*avglumi)*driftterm
        return result
    
    def fPixelAfterglow(self,nBXs,norm=1.0):
        return norm
    
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
    fConstParams={'normocc1':100.0}
    print normFunctionCaller('fConst',**fConstParams)
    fPolyParams={'normocc1':6370.0,'drift':0.067,'normocc2':0.93}
    normFunctionCaller('fPolyDrift',luminonorm,0.0,**fPolyParams)
    
