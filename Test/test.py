import re
import os
import time
import sys

#global variables
resultlogf=None
resulttablef=None
analysisresultf=None
loglinenumber=0

sepcharset=set([',','.','/','|','\\' ])
ignorecharset=set([' ','\"',':','}'])

fieldset=set()
fielddict={}
calltimes={}

class LogFilter(object):
    # StrictMatch: if false, only match the value, if true, match and value, and match the key ignore case
    def __init__(self):
        self.__strFilter=[]
        self.__pairFilter={}
        self.__analysisFields=[]
        self.__excludeFilter=[]
    
    def __del__(self):
        del self.__strFilter[:]
        del self.__strFilter
        self.__pairFilter.clear()
        del self.__pairFilter
        del self.__analysisFields[:]
        del self.__analysisFields
    
    def addStrFilter(self,condition):
        self.__strFilter.append(condition)
    
    def addPairFilter(self,key,value):
        self.__pairFilter[key]=value
    
    def addExcludeFilter(self,excludeStr):
        print(excludeStr)
        self.__excludeFilter.append(excludeStr)

    def addAnalysisField(self,field):
        self.__analysisFields.append(field)

    def getAnalysisFields(self):
        return self.__analysisFields

    def toString(self):
        print('String filter:',self.__strFilter)
        print('Pair filter:',self.__pairFilter)
        print('Exclude filter:',self.__excludeFilter)
        print('Analysis filds:',self.__analysisFields)

    def matchLine(self,line):

        for excludef in self.__excludeFilter:
            if(line.find(excludef)>0):
                return False

        #Match given string
        for strf in self.__strFilter:
            if(line.find(strf)==-1):
                return False

        #Match given json pair, first match value, then match the key in lowercase backround
        pairiter=self.__pairFilter.iteritems()
        for key, value in pairiter:
            vindex=line.find(value)
            if(vindex==-1):
                return False

            # Match the key ignore case
            key=key.lower()
            klength=len(key)
            vlength=len(value)
            while(vindex!=-1):
                begin=vindex-klength-5
                if(begin<0):
                    begin=0
                tmp=line[begin:vindex].lower()
                if(tmp.find(key)!=-1):
                    break
                vindex=line.find(value,vindex+klength+vlength)
            if(vindex==-1):
                return False
        
        return True

# A memory to save the result for a matched log line
class LineResult:
    def __init__(self):
        self.__fields=[]
        self.__result={}

    def __del__(self):  
        del self.__fields[:]
        del self.__fields
        self.__result.clear()
        del self.__result
  
    def add(self,key,value):
        self.__fields.append(key)
        self.__result[key]=value

    def getValue(self, key):
        return self.__result.get(key,'')

    def toString(self):
        rStr=''
        for key in self.__fields:
            pair='%s:%s,'%(key,self.getValue(key))
            rStr+=pair

        rStr=rStr[:-1]
        return rStr

class Result:
    def __init__(self):
        self.__logSize=0
        self.__totalLine=0
        self.__timeCost=0
        self.__matchedLine=0
        self.__resultLines=[]


    def __del__(self):  
        del self.__resultLines[:]
        del self.__resultLines

    def setLogSize(self,logSize):
        self.__logSize=logSize

    def setTimeCost(self,timeCost):
        self.__timeCost=timeCost

    def setTotalLine(self,totalLine):
        self.__totalLine=totalLine

    def matchIncr(self):
        self.__matchedLine+=1

    def addResultLine(self,lineResult):
        self.__resultLines.append(lineResult)
        
    def getLogSize(self):
        return self.__logSize
    
    def getTimeCost(self):
        return self.__timeCost
    
    def getTotalLine(self):
        return self.__totalLine
    
    def getMatchedLine(self):
        return self.__matchedLine

    def getResultLines(self):
        return self.__resultLines

    def toString(self):
        stat='Analysis Log file with size %s cost %s. The log contains %d lines and matched %d lines.'%(self.__logSize,self.__timeCost, self.__totalLine,self.__matchedLine)
        return stat

def defineLogFilter():
    filterConfFile=None
    for dirname in os.listdir('.'):
        if(dirname.find('.conf')>-1 and not os.path.isdir(dirname)):
            filterConfFile=dirname
            break
    if(not filterConfFile):
        raise RuntimeError('''Can't find conf file!''')
    
    f=open(filterConfFile);
    print(filterConfFile)
    logFilter = LogFilter()
    for line in f:
        if(line==None or not line or line[0]=='#'):
            continue

        line=line.strip()
        length=len(line)
        if(length==0):
            continue
        print("result")
        print(line)
        if(line[0]=='&'):
            logFilter.addStrFilter(line[1:length].strip())
        elif(line[0]==':'):
            logFilter.addAnalysisField(line[1:length].strip())
        elif(line.find('=')>-1):
            pair=line.split('=')
            key=pair[0].strip()
            value=pair[1].strip()
            logFilter.addPairFilter(key,value)
        elif(line[0]=='!'):
            logFilter.addExcludeFilter(line[1:]) 
    f.close()
    return logFilter
      

def analysis(logFileName,logFilter):
    global resultlogf
    global resulttablef
    global analysisresultf
    global loglinenumber

    global fieldset
    global fielddict
    
    # LogFile size.
    size=os.path.getsize(logFileName)
    # Timestamp of begin point.
    startTime=time.time()
    
    result=Result()

    f=open(logFileName)
    lineCount=0
    matchCount=0
    for line in f:
        lineCount+=1
        line=line.strip()
        
        if(logFilter.matchLine(line)):
            loglinenumber+=1
            result.matchIncr()
            resultlogf.write(str(loglinenumber)+': '+line+'\n')
            deviceid=getValueInLine(line, 'deviceId')
            if(deviceid=="1234567890123456789012345678901234567890"):
                print(deviceid)
            if(calltimes.get(deviceid)==None):
                calltimes[deviceid]=0;
            calltimes[deviceid]=str(int(calltimes[deviceid])+1)
            
            lineResult=LineResult()
            for field in logFilter.getAnalysisFields():
                fieldvalue=getValueInLine(line,field)
                fieldset.add(fieldvalue)
                    
                sizestr=getValueInLine(line,'size')
                if(sizestr):
                    size=int(sizestr)
                else:
                    size=0
                if(fieldvalue in fielddict):
                    fielddict[fieldvalue][0]=fielddict[fieldvalue][0]+1
                    fielddict[fieldvalue][1]=fielddict[fieldvalue][1]+size
                else:
                    fielddict[fieldvalue]=[1,size]                
                #lineResult.add('time',line[1:24])
                #lineResult.add('msg',line)
            result.addResultLine(lineResult)
            analysisresultf.write(str(loglinenumber)+': '+lineResult.toString()+'\n')
    
    # Time cost stat
    endTime=time.time()
    cost=endTime-startTime

    result.setLogSize(size)
    result.setTimeCost(cost)
    result.setTotalLine(lineCount)

    #Necessary, close log file
    f.close()

    # Analysis process info.
    print('Analysis a logfile of %s bytes cost %f seconds, %d lines matchs % d lines'%(size,cost,lineCount,result.getMatchedLine()))
    return result

def getValueInLine(line,field):
    matchindex=line.find(field)
    begin=matchindex+len(field)
    end=len(line)
    matchbegin=begin
    matchend=end
    for index in range(begin,end):
        if(line[index] in sepcharset):
            matchend=index
            break
    for index in range(begin,matchend)[::-1]:
        if(not line[index] in ignorecharset):
            matchend=index+1
            break

    for index in range(begin,matchend):
        if(not line[index] in ignorecharset):
            matchbegin=index
            break
    if(matchbegin==begin):
        matchbegin=end
    return line[matchbegin:matchend]

def analysisLog(logPath='.',isDetail=True):
    if(logPath==None or len(logPath)==0):
        logPath='.'
    
    if(not os.path.exists(logPath)):
        raise IOError('\"%s\" is not a file path!'%(logPath))

    resultlog='result.log'
    resulttable='resulttable.txt'
    analysisresult='result.txt'

    if(os.path.exists(resultlog)):
        os.remove(resultlog)
    if(os.path.exists(resulttable)):
        os.remove(resulttable)
    if(os.path.exists(analysisresult)):
        os.remove(analysisresult)

    global resultlogf
    global resulttablef
    global analysisresultf
    resultlogf=open(resultlog,'wb')
    resulttablef=open(resulttable,'wb')
    analysisresultf=open(analysisresult,'wb')

    logFilter=defineLogFilter()
    print(logFilter.toString()) 
    results=[]
    logCount=0
    if(os.path.isdir(logPath)):
        print(logPath)
        for parent,dirnames,filenames in os.walk(logPath):
            for fname in filenames:
                if(fname.find('.log')>-1 and not fname.endswith('.zip')):
                    logCount+=1
                    logfn=parent+os.sep+fname
                    print('Analysising log file:',logfn)
                    results.append(analysis(logfn,logFilter))
    else:
        logCount+=1
        results.append(analysis(logPath,logFilter))

    size=0
    timecost=0
    totalLines=0
    matchedLines=0

    for result in results:
        size+=result.getLogSize()
        timecost=result.getTimeCost()
        totalLines+=result.getTotalLine()
        matchedLines+=result.getMatchedLine()

    print('%d log files total size is %s, cost time %f, %d lines matched %d lines.'%(logCount,size,timecost,totalLines,matchedLines))

    resultlogf.close()  
    resulttablef.close()
    analysisresultf.close()
    
    print('End')

    return results


def __localAnalysisTest(argv):
    if(len(argv)==0 or argv[0]=='false' or argv[0]=='False'):
        isDetailed=false

    results=analysisLog('logs');
    heartbeatCount=0
    totalCount=0
    for result in results:
        totalCount+=result.getMatchedLine()
        for line in result.getResultLines():
            method=line.getValue('method')
            
            if(method!='' and method=='heartBeat'):
               heartbeatCount+=1
    print('In '+str(totalCount)+' method, '+str(heartbeatCount)+' is heartbear!')

    print(fieldset)
    print(fielddict)
    
    for deviceid in calltimes:
        if(deviceid!='null'):
            print(deviceid +":"+str(calltimes[deviceid]))

if __name__=="__main__":
    __localAnalysisTest(sys.argv)

