from arbiter.Core.Utilities.baseScript import *
from arbiter.Interface.API.System import *

class arb_listworkflow( baseScript ):
  """
    List the workflow recorded by arbiter.
    User can specify owner of the workflow. If not, will list all workflow.
    
    Usage: arb listworkflow -u(-U)(--user) [username]
  """

  def __init__( self, argv ):
    baseScript.__init__( self )
    self.argvDict = { 'u' : 'bound', 'U' : 'bound', '--user' : 'bound'}
    self.argv = argv
    
  def execute( self ):
    result = self.checkRationality()
    if not result['OK']:
      return False
    result = self.parseArgv()
    if not result['OK']:
      return False
    result = result['Value']
    if result['unbound']:
      print self.__doc__
      return False
    user = ''
    if result['bound']:
      if not result['bound'].values() or len( result['bound'].values() ) > 1:
        print self.__doc__
        return False
      user = result['bound'].values()[0]
    arbiter = system()
    result = arbiter.listJob( user )
    if not result['OK']:
      return False
    jobList = result['Value']
    lis = []
    for job in jobList:
      lis.append(job[0])
    lis.sort()
    for ID in lis:
      for job in jobList:
        if job[0] == ID:
          print str(job[0]) + '   ' + str(job[1]) + '   ' + str(job[2]) + '   ' + str(job[3]) 
    return True