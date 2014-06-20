import os
import sys
from arbiter.Core.Workflow.Workflow import *
from arbiter.Core.Utilities.ReturnValues import *
from arbiter.Core.Utilities.Utilities import *
from arbiter.Core.Utilities.dataTools import *
from arbiter.Core.Utilities.Constants import *

class Job:

  def __init__( self, name = None, script = None ):

    global tempDirectory
    self.tempDirectory = tempDirectory
    self.name = None
    self.jobID = None
    self.debug = False
    self.workDirectory = os.getcwd()
    self.status = 'unSubmitted'
    self.stepCount = 0
    self.workflow = Workflow()
    self.dataTool = dataTool( self )
    if script:
      result = self.__loadJob( script )
      if not result['OK']:
        sys.exit(0)
      self.dataTool = dataTool( self )
      result = self.__checkExistence()
      if not result['OK']:
        sys.exit(0)
      result = self.updateData()
      if not result['OK']:
        sys.exit(0)
      #checkUserName( self.user )
    else:
      if name:
        self.setName( name )
        self.workflow.jobName = name
      else:
        self.setName( 'unDefined' )
        self.workflow.jobName = 'unDefined'
    result = self.__initializeJob()
    if not result['OK']:
      sys.exit(0)

  def __initializeJob( self ):
    if self.jobID != None:
    # this means job is already loaded from xml
      return S_OK()
    else:
      result = self.dataTool.getNewWorkflowID()
      if not result['OK']:
        return result      
      newID = int( result['Value'] )
      #dirDirectory = self.tempDirectory + 'workflowTemp/'
      #os.chdir( dirDirectory )
      #os.mkdir( newNumString )
      self.jobID = int( newID )
      self.workflow.jobID = self.jobID
      self.dataTool.jobID = self.jobID
      #self.dbTool.addJob( self )
      return S_OK()

  def __loadJob( self, script ):
    result = loadJobFromXML( self, script )
    return result

  def __checkExistence( self ):
    # check existence of directory first
    jobTempDirectory = self.tempDirectory + 'workflowTemp/'
    if not os.path.exists( jobTempDirectory ):
      return S_ERROR( 'Can not find jobTempDirectory' )
    jobDirectory = jobTempDirectory + str( self.jobID ) + '/'
    if not os.path.exists( jobDirectory ):
      return S_ERROR( 'Can not find job directory' )
    for step in self.workflow.steps:
      stepDirectory = jobDirectory + step.name
      if not os.path.exists( stepDirectory ):
        return S_ERROR( 'Can not find step directory' )
    # check database
    result = self.dataTool.checkExistence( self )
    if not result['OK']:
      return result
    return S_OK()

  def setName( self, jobName ):
    if not type( jobName ) == type( ' ' ):
      return S_ERROR( 'Expected a string for job name' )
    else:
      self.name = jobName
      return S_OK()

  def addStep( self, sinfo ):
    stepInfoList = sinfo.split('/')
    stepType = stepInfoList[0]
    stepName = stepInfoList[1]

    if stepType not in self.workflow.typeList:
      return S_ERROR( 'There is no %s step type' % stepType )
    self.workflow.addStep( stepType, stepName, self.jobID )
    self.stepCount += 1
    #self.dbTool.addStep( self )
    return S_OK()

  def setStepParameter( self, stepUserName, name, value, extra = None, ptype = None ):
    for step in self.workflow.steps:
      if stepUserName == step.userName:
        if name == 'optionFileDirectory':
          if not value:
            value = self.workDirectory
        step.setParameter( name, value, ptype, extra)
        if name == 'input':
          step.setInputData()
        return S_OK()
    return S_ERROR('Can not find step named %s' % stepUserName )

  def setStepSplitter( self, stepName, splitterInfo ):
    for step in self.workflow.steps:
      if stepName == step.userName:
        step.setSplitter( splitterInfo )
        return S_OK()
    return S_ERROR('Can not find step named %s' % stepName )

  def toXMLFile( self ):
    if self.debug:
      print 'debug mode detected!Will not generate xml file'
      return True
    result = self.create()
    if not result['OK']:
      self.delete()
      sys.exit(0)
    result = self.workflow.createCode( -1, False, False )
    if not result['OK']:
      self.delete()
      sys.exit(0)
    ret = self.workflow.toXML()
    xmlFileName = self.tempDirectory + str( self.jobID ) + '.xml'
    if os.path.exists( xmlFileName ):
      print 'xmlFile already exists, this is usually because someone cleaned old data files, please check and remove old worklfow information'
      sys.exit(0)
    xmlFile = open( self.tempDirectory + str( self.jobID ) + '.xml', 'w' )
    xmlFile.write( ret )
    xmlFile.close()
    print 'New workflow generated! The ID is : ' + str( self.jobID )
 
  def updateData( self ):
    statusDict = { }
    for stepID in range( self.stepCount ):
      result = checkStepStatus( self, stepID )
      if not result['OK']:
        return result
      stepStatus = result['Value']
      statusDict[stepID] = stepStatus
    result = self.dataTool.updateStep( statusDict )
    if not result['OK']:
      return result
    result = checkWorkflowStatus( self )
    if not result['OK']:
      return result
    result = self.dataTool.updateWorkflow( result['Value'] )
    if not result['OK']:
      return result
    return S_OK()

  def create( self ):
    # create the directory for the whole workflow and add workflow info to the database
    jobTempDirectory = self.tempDirectory + 'workflowTemp/'
    if not os.path.exists( jobTempDirectory ):
      return S_ERROR( 'cannot find the jobTempDirectory' )
    jobDirectory = jobTempDirectory + str( self.jobID )
    if os.path.exists( jobDirectory ):
      return S_ERROR( '%s already exists, this is usually because someone cleaned database, please check and remove old worklfow information' % jobDirectory )
    else:
      try:
        os.mkdir( jobDirectory )
      except:
        return S_ERROR( 'Can not create workflow.\nFailed to Create workflow' )
    result = self.dataTool.addJob( self )
    if not result['OK']:
      print 'Failed to create workflow'
      self.delete()
      return result

    # create directory for steps and add step info to the database
    for step in self.workflow.steps:
      result = step.createStep()
      if not result['OK']:
        print 'Fail to create workflow'
        self.delete()
        return result
      result = self.dataTool.addStep( step )
      if not result['OK']:
        print 'Fail to create workflow'
        self.delete()
        return result
    return S_OK()

  def delete( self ):
    # remove the directory of the workflow
    xmlFile = self.tempDirectory + str( self.jobID ) + '.xml'
    if os.path.exists( xmlFile ):
      try:
        os.system( 'rm %s' % xmlFile )
      except:
        return S_ERROR( 'Can not remove xml file' )
    jobDirectory = self.tempDirectory + 'workflowTemp/' + str( self.jobID )
    if os.path.exists( jobDirectory ):
      try:
        os.system( 'rm -rf %s' % jobDirectory )
      except:
        return S_ERROR( 'Can not remove job directory' )
    # remove information from the database
    result = self.dataTool.deleteJob( self.jobID )
    if not result['OK']:
      return result
    return S_OK()

  def submit( self ):
    optionList = self.workflow.createCode( 0 )
    self.workflow.submit( optionList )
    statusDict = { 0 : 'yes' }
    self.dataTool.updateStep( statusDict, 'onGoing' )
    
  def push( self ):
    result = self.dataTool.getNextStepID()
    if not result['OK']:
      return result
    stepID = int( result['Value'] )
    optionList = self.workflow.createCode( stepID )
    statusDict = { self.jobID : { stepID : { 'onGoing' : 'yes' } } }
    result = self.dataTool.updateStep( statusDict )
    if not result['OK']:
      return result
    self.workflow.submit( optionList )

  def generate( self ):
    optionList = self.workflow.createCode( 0, self.debug )
    if self.debug:
      self.delete()
