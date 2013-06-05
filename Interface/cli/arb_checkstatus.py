from arbiter.Core.Utilities.baseScript import *
from arbiter.Interface.API.System import *

class arb_checkstatus(baseScript):
  """
   List the status of all sub-jobs of a workflow. 
   User must specify workflowID, but stepID is optional
   
   usage: arb checkstatus [workflowID] ([stepID])
  """

  def __init__( self, argv ):
    baseScript.__init__()
    self.argvDict = {}
    self.argv = argv

  def execute( self ):
    result = self.checkRationality()
    if not result['OK']:
      return False
    result = self.parseArgv()
    if not result['OK']:
      return False
    result = result['Value']
    if not result['unbound'] or len( result['unbound'] ) > 2:
    # user must specify the workflow ID.
      print self.__doc__
      return False
    wsNum = result['unbound']
    stepID = ''
    workflowID = ''
    if len( wsNum ) == 2:
    # this means that user specified the step ID.
      try:
        stepID = int( wsNum[1] )
      except ValueError:
        print 'stepID must be a number'
        return False
    try:
      workflowID = int( wsNum[0] )
    except ValueError:
      print 'workflowID must be a number'
      return False
    # now get status information through API
    arbiter = system()
    statusDict = arbiter.checkStatus( str( workflowID ) )
    if not stepID:
      for stepCount, stepStatusDict in statusDict.items():
        print 'step' + str( stepCount ) + 'status:'
        for job, jobStatus in stepStatusDict.items():
          print job + ':' + jobStatus
    else:
      for stepCount, stepStatusDict in statusDict.items():
        if int(stepCount) == stepID:
          print 'step' + str( stepCount ) + 'status:'
          for job, jobStatus in stepStatusDict.items():
            print job + ':' + jobStatus
    return True
