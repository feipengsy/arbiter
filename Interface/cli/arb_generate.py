from arbiter.Core.Utilities.baseScript import *
from arbiter.Interface.API.System import *

class arb_generate( baseScript ):
  """
    Generate job-option file for the workflow.
    User must specify the workflowID, but stepID is optional. If stepID is not specified, will generate the first step.
    
    Usage: arb generate [workflowID] ([stepID])
  """
  
  def __init__( self, argv ):
    baseScript.__init__( self )
    self.argvDict = { }
    self.argv = argv

  def execute( self ):
    result = self.checkRationality()
    if not result['OK']:
      return False
    result = self.parseArgv()
    if not result['OK']:
      return False
    result = result['Value']
    if len( result['unbound'] ) > 2 or not result['unbound']:
      print self.__doc__
      return False
    for ID in result['unbound']:
      try:
        int( ID )
      except:
        print 'workflowID or stepID must be numbers'
        print self.__doc__
        return False
    arbiter = system()
    if len( result['unbound'] ) == 1:
      arbiter.generate( result['unbound'][0] )
      return True
    arbiter.generate( result['unbound'][0],  result['unbound'][1] )  
    