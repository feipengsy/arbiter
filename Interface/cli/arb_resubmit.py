from arbiter.Core.Utilities.baseScript import *
from arbiter.Interface.API.System import *

class arb_resubmit( baseScript ):
  """
    Re-submit sub-jobs of the workflow.
    User needs to specify the workflowID and stepID. 
    option:
    -f/-F/--failed: Submit jobs that status are failed
    -r/-R/--running: Submit jobs that status are running
    -w/-W/--waiting: Submit jobs that status are waiting
    -d/-D/--done: Submit jobs that status are done
    -u/-U/--unkonwn: Submit jobs that status are unknown
    -a/-A/--all: Submit all sub-jobs
    
    Usage: arb resubmit ([argument...]) [workflowID] [stepID] ([jobname1] [jobname2]...)
  """

  def __init__( self, argv):
    baseScript.__init__( self )
    self.argvDict = { 'f' : 'unbound', 'F' : 'unbound', 'failed' : 'unbound', 
                      'r' : 'unbound', 'R' : 'unbound', 'running' : 'unbound', 
                      'w' : 'unbound', 'W' : 'unbound', 'waiting' : 'unbound',
                      'd' : 'unbound', 'D' : 'unbound', 'done' : 'unbound',
                      'u' : 'unbound', 'U' : 'unbound', 'unknown' : 'unbound',
                      'a' : 'unbound', 'A' : 'unbound', 'all' : 'unbound' }
    self.argv = argv

  def execute( self ):    
    result = self.checkRationality()
    if not result['OK']:
      return False
    result = self.parseArgv()
    if not result['OK']:
      return False
    result = result['Value']
    if len( result['unbound'] ) < 2:
      print self.__doc__
      return False
    try:
      worklfowID = int(result['unbound'][0])
      stepID = int(result['unbound'][1])
    except:
      print self.__doc__
      return False
    jobList = result['unbound'][2:]
    arbiter = system()
    statusresult = arbiter.checkStatus( worklfowID )
    if not statusresult['OK']:
      return False
    statusDict = statusresult['Value'][stepID]
    for job in jobList:
      if job not in statusDict.keys():
        print 'There is no sub-job %s in worklfow %s step %s' % ( job, workflowID, stepID )
        jobList.remove( job )
    if 'a' in result.keys() or 'A' in result.keys() or 'all' in result.keys():
      jobList = statusDict.keys()
    else:
      if 'f' in result.keys() or 'F' in result.keys() or 'failed' in result.keys():
        for job in statusDict.keys():
          if statusDict[job] == 'failed' and job not in jobList:
            jobList.append( job )
      if 'r' in result.keys() or 'R' in result.keys() or 'running' in result.keys():
        for job in statusDict.keys():
          if statusDict[job] == 'running' and job not in jobList:
            jobList.append( job )
      if 'w' in result.keys() or 'W' in result.keys() or 'waiting' in result.keys():
        for job in statusDict.keys():
          if statusDict[job] == 'waiting' and job not in jobList:
            jobList.append( job )
      if 'd' in result.keys() or 'D' in result.keys() or 'done' in result.keys():
        for job in statusDict.keys():
          if statusDict[job] == 'done' and job not in jobList:
            jobList.append( job )
      if 'u' in result.keys() or 'U' in result.keys() or 'unknown' in result.keys():
        for job in statusDict.keys():
          if statusDict[job] == 'unknown' and job not in jobList:
            jobList.append( job )
      infoDict = { 'jobID' : workflowID, 'stepID' : stepID, 'optionList' : jobList }
      result = arbiter.resubmit( infoDict )
      if not result['OK']:
        return False
      return True    