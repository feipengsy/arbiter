from arbiter.Core.Utilities.baseScript import *
from arbiter.Interface.API.System import *

class arb_submit(baseScript):
  """
    Submit the workflow. The first step will be executed.
    User needs to specify the workflowID
    
    usage: arb submit [workflowID]
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
    if len( result['unbound'] ) != 1:
      print self.__doc__
      return False
    try:
      workflowID = int( result['unbound'][0] )
    except:
      print 'workflowID must be a number'
      print self.__doc__
      return False
    arbiter = system()
    arbiter.submit( workflowID )
    return True