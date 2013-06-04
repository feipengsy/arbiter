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
    self.user = getUserName()

  def listJob( self, user ):
    jobList = self.dbTool.getJobList( user )
    return jobList

  def generate( self, jobNum ):
    xmlPath = self.tempDirectory + str( jobNum ) + '.xml'
    job = Job( '', xmlPath )
    job.generate()

  def execute( self, jobNum ):
    xmlPath = self.tempDirectory + str( jobNum ) + '.xml'
    job = Job( '', xmlPath )
    job.execute()

  def submit( self, jobNum ):
    xmlPath = self.tempDirectory + str( jobNum ) + '.xml'
    job = Job( '', xmlPath )
    job.submit()

  def resubmit( self, infoDict ):
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

  def checkStatus( self, jobName ):
    return checkStatus( jobName )
