class normFunctionFactory(object):
    def fConst(self,luminonorm,normocc1=1000.0):
        return luminonorm*normocc1
    
    def fPoly(self,luminonorm,normocc1=1000.0,normocc2=30.0,drift=0.07):
        return luminonorm*normocc1*drift
    
    def fPolyAfterglow(self,luminonorm,normocc1=1000.0,normocc2=30.0,drift=0.07):
        afterglow=0.99
        return luminonorm*normocc1*drift*afterglow
        
def normFunctionCaller(funcName,*args,**kwds):
    fac=normFunctionFactory()
    try:
        myfunc=getattr(fac,funcName,None)
    except AttributeError:
        print '[ERROR] unknown correction function '+funcName
        raise
    if callable(myfunc):
        return myfunc(*args,**kwds)

if __name__ == '__main__':
    luminonorm=23.0
    fConstParams={'normocc1':100.0}
    print normFunctionCaller('fConst',luminonorm,**fConstParams)
    fPolyParams={'normocc1':6370.0,'drift':0.067,'normocc2':0.93}
    print normFunctionCaller('fPoly',luminonorm,**fPolyParams)
    
