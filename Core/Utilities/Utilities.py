import os
import sys
import re
from xml.dom import minidom
from arbiter.Core.Utilities.Constants import *
from arbiter.Core.Utilities.dataBaseTools import *


def readend( testfile ):
  f = open( testfile, 'a+' )
  n = -1
  while True:
    n = n - 1
    f.seek( n, 2 )
    if f.read( 1 ) == '\n':
      break
  endLine = f.readline().strip()
  f.close()
  return endLine

def loadJobFromXML( job, xmlFile ):
  xml = minidom.parse( xmlFile ).firstChild
  job.jobID = int( xml.attributes['jobID'].value.encode() )
  job.name = xml.attributes['jobName'].value.encode()
  job.user = xml.attributes['user'].value.encode()
  for snode in xml.childNodes:
    if snode.nodeName == 'step':
      stepType = snode.attributes['type'].value.encode()
      stepUserName = snode.attributes['userName'].value.encode()
      job.workflow.addStep( stepType, stepUserName, job.jobID )
      job.stepCount += 1
      for pnode in snode.childNodes:
        if pnode.nodeName == 'parameter':
          parameterName = pnode.attributes['name'].value.encode()
          parameterType = pnode.attributes['type'].value.encode()
          parameterValue = pnode.attributes['value'].value.encode()
          parameterExtra = pnode.attributes['extra'].value.encode()
          job.setStepParameter( stepUserName, parameterName, parameterValue, parameterExtra, parameterType )
        if pnode.nodeName == 'splitter':
          splitterInfo = {}
          for k,v in pnode.attributes.items():
            splitterInfo[k.encode()] = v.encode()
          job.setStepSplitter( stepUserName, splitterInfo )
  job.updateDB()
  return True

def checkStatus( jobName ):
  databaseTool = dbTool()
  stepCount = databaseTool.getStepCount( int( jobName ) )
  allStatDict = {}
  for i in range( stepCount ):
    jobDirectory = getJobDirectory( jobName, i )
    optionLogDict = {}
    optionfiles = []
    statDict = {}
    statTempDict = {}
    nameDirectDict = {}
    optionListFile = open( jobDirectory + 'optionList.txt' )
    optionFileTemp = optionListFile.readlines()
    optionListFile.close()
    for optionFile in optionFileTemp:
      optionfiles.append( optionFile.strip() )
      name = optionFile.strip().split( '/' )[-1]
      statDict[name] = ''
      nameDirectDict[name] = optionFile.strip()
    for optionfile in optionfiles:
      logfile = optionfile + '.bosslog'
      optionLogDict[optionfile] = logfile
    #mind! Add user information here
    cmdReturn = os.popen( 'qstat -u lit' ).readlines()
    if len( cmdReturn ) >= 6:
      for returnLine in cmdReturn[5:]:
        pat = re.compile( '\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}(\S{1,255})' )
        if pat.search( returnLine ):
          jobName = pat.search( returnLine ).groups()[0]
        else:
          continue
        pat = re.compile( '\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}\S{1,255}\s{1,255}(\S{1,255})' )
        if pat.search( returnLine ):
          jobStatus = pat.search( returnLine ).groups()[0]
        else:
          continue
        if jobName not in statTempDict.keys():
          statTempDict[jobName] = jobStatus
        else:
          if statTempDict[jobName] != jobStatus:
            statTempDict[jobName] = 'unknown'
    for jobName,jobStatus in statTempDict.items():
      if jobName in statDict.keys():
        if jobStatus == 'R':
          statDict[jobName] = 'running'
        if jobStatus == 'Q': 
          statDict[jobName] = 'waiting'
    for jobName in statDict.keys():
      logfile = optionLogDict[nameDirectDict[jobName]]
      try:
        f = open( logfile, 'r' )
      except:
        if not statDict[jobName]:
          statDict[jobName] = 'failed'
        continue
      fstring = f.read()
      f.close()
      if ('INFO Application Manager Stopped successfully' in fstring) and ('INFO Application Manager Finalized successfully') in fstring and ('INFO Application Manager Terminated successfully' in fstring):
        statDict[jobName] = 'done'
      else:
        if not statDict[jobName]:
          statDict[jobName] = 'failed'
    allStatDict[i] = statDict
  return allStatDict

def checkWorkflowStatus( jobName ):
  databaseTool = dbTool()
  stepCount = databaseTool.getStepCount( int( jobName ) )
  for i in range( stepCount ):
    jobDirectory = getJobDirectory( jobName, i )
    if not os.path.exists( jobDirectory + 'optionList.txt' ):
      return 'unDone'
  result = checkStatus( jobName )
  for stepStatDict in result.values():
    if not stepStatDict:
      return 'unDone'
    for stat in stepStatDict.values():
      if stat != 'done':
        return 'unDone'
  return 'done'

def checkStepStatus( jobName, stepID ):
  jobDirectory = getJobDirectory( jobName, stepID )
  if not os.path.exists( jobDirectory + 'optionList.txt' ):
    return 'unDone'
  result = checkStatus( jobName )
  for stat in result[int( stepID )].values():
    if stat != 'done':
      return 'unDone'
  return 'done'

def getJobDirectory( jobID, stepID ):
  global tempDirectory
  try:
    IDName = int( jobID )
    jobDirectory = tempDirectory + 'workflowTemp/' + str( IDName ) + '/'
    stepDirectory = os.listdir(jobDirectory)
    finalDirectory = jobDirectory + stepDirectory[int( stepID )] + '/'
    return finalDirectory
  except ValueError:
    return False

def getUserName():
  user = os.popen('echo $USER').read()
  user = user.strip()
  return user

def checkUserName( user ):
  userName = os.popen('echo $USER').read()
  userName = user.strip()
  if user != userName:
    print 'user not matched!'
    sys.exit()
  return True
