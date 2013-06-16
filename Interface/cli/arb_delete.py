from arbiter.Core.Utilities.baseScript import *
from arbiter.Interface.API.System import *

class arb_delete( baseScript ):
  """
    Delete workflow that is already recorded by arbiter. Including temp directory, and information in the database.
    User need to specify the workflowID.
    
    Usage: arb delete [workflowID] [workflowID]...
    
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
    if not result['unbound']:
      print self.__doc__
      return False
    for workflowID in result['unbound']:
      try:
        int( workflowID )
      except:
        print 'workflowID must be a number'
        print self.__doc__
        return False
    arbiter = system()
    for workflowID in result['unbound']:
      arbiter.delete( workflowID )
    return True