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
    result = self.dbTool.getJobList( user )
    if not result['OK']:
      return result
    jobList = result['Value']
    return S_OK( jobList )

  def generate( self, jobID ):
    """
      Generate option files of all steps
    """
    xmlPath = self.tempDirectory + str( jobID ) + '.xml'
    if not os.path.exists( xmlPath ):
      return S_ERROR( 'Can not find workflow %s' % jobID )
    job = Job( '', xmlPath )
    job.generate()
    return S_OK()

  def delete( self, jobID ):
    """
      Delete workflow
    """
    result = checkUserName( self.user )
    if not result:
      return S_ERROR( 'Can not remove workflow belong to others' )
    # remove the directory of the workflow
    xmlFile = self.tempDirectory + str( jobID ) + '.xml'
    if os.path.exists( xmlFile ):
      try:
        os.system( 'rm %s' % xmlFile )
      except:
        return S_ERROR( 'Can not remove xml file' )
    jobDirectory = self.tempDirectory + 'workflowTemp/' + str( jobID )
    if os.path.exists( jobDirectory ):
      try:
        os.system( 'rm -rf %s' % jobDirectory )
      except:
        return S_ERROR( 'Can not remove job directory' )
    # remove information from the database
    result = self.dbTool.deleteJob( jobID )
    if not result['OK']:
      return result
    return S_OK()

  def submit( self, jobID ):
    """
     Submit the first step of the workflow to the queue
    """
    xmlPath = self.tempDirectory + str( jobID ) + '.xml'
    if not os.path.exists( xmlPath ):
      return S_ERROR( 'Can not find workflow %s' % jobID )
    job = Job( '', xmlPath )
    job.submit()
    return S_OK()

  def resubmit( self, infoDict ):
    """
     Re-submit sub-jobs of a workflow
     infoDict{'jobID':workflowID, 'stepID':stepID, 'optionList':[optionNames...]}
    """
    jobID = infoDict['jobID']
    stepID = infoDict['stepID']
    optionFileList = infoDict['optionList']
    result = self.dbTool.getWorkarea( jobID, stepID )
    if not result['OK']:
      return result
    optionTempDirectory = result['Value']
    result = self.dbTool.checkOptionFile( optionFileList )
    if not result['OK']:
      return result
    optionFileDict = result['Value']
    for optionFile in optionFileDict['valid']:
      os.chdir( optionTempDirectory )
      os.system( 'boss -q %s' % optionFile )
    for optionFile in optionFileDict['invalid']:
      print 'Invalid job name: ' + optionFile
    return S_OK()

  def checkStatus( self, jobName ):
    """
      get the status of the workflow
    """
    return checkStatus( jobName )
