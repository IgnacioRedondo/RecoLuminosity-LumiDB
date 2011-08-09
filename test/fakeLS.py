import sys,datetime,time,csv,math
def main(*args):
    filename=args[1]
    time_format='%y/%m/%d %H:%M:%S'
    timereader=csv.reader(open(filename,'rb'),delimiter=',')
    lslength=23.357
    totinsec=0
    totinls=0
    previousStart=0
    previousRun=0
    counter=0
    print 'runnum durationinsec numls'
    timedata=[]
    for row in timereader:
        if len(row)!=3: continue
        timedata.append((row[0],row[1],row[2]))
    for row in timedata:
        start=time.mktime(time.strptime(row[1],time_format))
        stop=time.mktime(time.strptime(row[2],time_format))
        #print row[0],start ,stop
        lastrunduration=0
        numLS=0
        if counter!=0:
            lastrunduration=start-previousStart
            numLS=math.ceil(lastrunduration/lslength)
            totinls+=numLS
            totinsec+=lastrunduration
            print previousRun,lastrunduration,int(numLS)        
        previousStart=start
        previousRun=row[0]
        counter+=1
        if counter==len(timedata):
            thisrunduration=stop-start
            numLS=math.ceil(thisrunduration/lslength)
            totinls+=numLS
            totinsec+=thisrunduration
            print row[0],thisrunduration,int(numLS)    
    print '==='
    print 'tot LS ',int(math.ceil(totinls))
    print 'tot in sec ',totinsec
    print 'tot in hour ',float(totinsec)/float(3600)

if __name__=='__main__':
    sys.exit(main(*sys.argv))
