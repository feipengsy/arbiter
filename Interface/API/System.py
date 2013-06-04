"""
  ARBITER API class  
  All arbiter functionality is exposed through the arbiter API

  The system API provides the following functionality:
  - Get the workflow list that were generated
  - Execute workflow that were generated or resubmit workflow or sub-jobs in it.
  - get the status of workflow
"""


from arbiter.Core.Utilities.ReturnValues import *
from arbiter.Core.Utilities.Utilities import *
from arbiter.Core.Utilities.Constants import *
from arbiter.Core.Utilities.dataBaseTools import *
from arbiter.Interface.API.Job import *
import os

class system:

  def __init__( self ):
    
    global tempDirectory
    self.tempDirectory = tempDirectory
    self.dbTool = dbTool()
    self.optionfiles = []
    #get the user name of batch farm $USER
    self.user = getUserName()

  def listJob( self, user ):
   """
      Get the workflow list
      returns: S_OK,S_ERROR
   """
    jobList = self.dbTool.getJobList( user )
    return S_OK( jobList )

  def generate( self, jobNum ):
   """
      Generate option files of all steps
   """
    xmlPath = self.tempDirectory + str( jobNum ) + '.xml'
    job = Job( '', xmlPath )
    job.generate()
    return S_OK()

  def execute( self, jobNum ):
   """
     Execute the first step of the workflow locally
   """
    xmlPath = self.tempDirectory + str( jobNum ) + '.xml'
    job = Job( '', xmlPath )
    job.execute()
    return S_OK()

  def submit( self, jobNum ):
   """
     Submit the first step of the workflow to the queue
   """
    xmlPath = self.tempDirectory + str( jobNum ) + '.xml'
    job = Job( '', xmlPath )
    job.submit()
    return S_OK()

  def resubmit( self, infoDict ):
   """
     Re-submit sub-jobs of a workflow
     infoDict{'jobNum':workflowID, 'stepNum':stepID, 'optionList':[optionNames...]}
   """
    jobNum = infoDict['jobNum']
    tempJob = Job( '', self.tempDirectory + str( jobNum ) + '.xml' )
    stepNum = infoDict['stepNum']
    opstep = tempJob.workflow.steps[int(stepNum)]
    stepName = opstep.name
    optionTempDirectory = ''
    for p in opstep.parameters:
      if p.name == 'optionFileDirectory':
        if p.value[-1] == '/':
          optionTempDirectory = p.value
        else:
          optionTempDirectory = p.value + '/'
    if not optionTempDirectory:
      optionDirectory = self.tempDirectory + 'workflowTemp/' + str( tempJob.jobNum ) + '/' + stepName + '/'
    else:
      optionDirectory = optionTempDirectory
    for optionFile in infoDict['optionList']:
      os.chdir( optionDirectory )
      os.system( 'boss -q %s' % optionFile )
    return S_OK()

  def checkStatus( self, jobName ):
   """
    get the status of the workflow
   """
    return checkStatus( jobName )
