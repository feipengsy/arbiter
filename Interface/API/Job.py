import os
import sys
from arbiter.Core.Workflow.Workflow import *
from arbiter.Core.Utilities.ReturnValues import *
from arbiter.Core.Utilities.Utilities import *
from arbiter.Core.Utilities.dataBaseTools import *
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
    self.user = ''
    self.stepCount = 0
    self.workflow = Workflow()
    self.dbTool = dbTool()
    if script:
      self.__loadJob( script )
      result = self.__checkExistence()
      if not result['OK']:
        sys.exit(0)
      result = self.updateDB()
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
      result = self.dbTool.getNewWorkflowID()
      if not result['OK']:
        return result      
      newID = int( result['Value'] )
      #dirDirectory = self.tempDirectory + 'workflowTemp/'
      #os.chdir( dirDirectory )
      #os.mkdir( newNumString )
      self.user = getUserName()
      self.jobID = int( newID )
      self.workflow.jobID = self.jobID
      self.workflow.user = self.user
      #self.dbTool.addJob( self )
      return S_OK()

  def __loadJob( self, script ):
    result = loadJobFromXML( self, script )
    return result

  def __checkExistence( self ):
    # check existence of directory first
    jobTempDirectory = self.tempDirectory + 'workflowTemp/'
    if not os.path.exsits( jobTempDirectory ):
      return S_ERROR( 'Can not find jobTempDirectory' )
    jobDirectory = jobTempDirectory + str( self.jobID )
    if not os.path.exists( jobDirectory ):
      return S_ERROR( 'Can not find job directory' )
    for step in self.workflow.steps:
      stepDirectory = jobDirectory + step.name
      if not os.path.exists( stepDirectory ):
        return S_ERROR( 'Can not find step directory' )
    # check database
    result = self.dbTool.checkExistence( self )
    if not result['OK']:
      return result
    result = checkUserName( self.user )
    if not result:
      return S_ERROR( 'User name not matched' )
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
      return result
    ret = self.workflow.toXML()
    xmlFileName = self.tempDirectory + str( self.jobID ) + '.xml'
    if os.path.exists( xmlFileName ):
      print 'xmlFile already exists, this is usually because someone cleaned database, please check and remove old worklfow information'
      sys.exit(0)
    xmlFile = open( self.tempDirectory + str( self.jobID ) + '.xml', 'w' )
    xmlFile.write( ret )
    xmlFile.close()
    print 'New workflow generated! The ID is : ' + str( self.jobID )
 
  def updateDB( self ):
    for stepID in range( self.stepCount ):
      stepStatus = checkStepStatus( self.jobID, stepID )
      statusDict = { self.jobID : { stepID: { 'status' : stepStatus } } }
      result = self.dbTool.updateStep( statusDict )
      if not result['OK']:
        return result
    workflowStatus = checkWorkflowStatus( self.jobID )
    workflowInfo = { 'jobID' : self.jobID, 'status' : workflowStatus }
    result = self.dbTool.updateWorkflow( workflowInfo )
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
    result = self.dbTool.addJob( self )
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
      result = self.dbTool.addStep( step )
      if not result['OK']:
        print 'Fail to create workflow'
        self.delete()
        return result
    return S_OK()

  def delete( self ):
    # check if user name matched
    result = checkUserName( self.user )
    if not result:
      return S_ERROR( 'Can not remove workflow belong to others' )
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
    result = self.dbTool.deleteJob( self.jobID )
    if not result['OK']:
      return result
    return S_OK()
    

  def execute( self ):
    optionList = self.workflow.createCode()
    self.workflow.execute( optionList )

  def reexecute( self, infoDict ):
    for k,v in infoDict.items():
      opstep = self.workflow.steps[int( k )]
      stepName = opstep.name
      optionDirectory = ''
      for p in opstep.parameters:
        if p.name == 'optionFileDirectory':
          if p.value[-1] == '/':
            optionTempDirectory = p.value
          else:
            optionTempDirectory = p.value + '/'
      if not optionDirectory:
        optionDirectory = self.tempDirectory + 'workflowTemp/' + str( self.jobID ) + '/' + stepName + '/'
      for optionFile in v:
        os.chdir( optionDirectory )
        os.system( 'boss.exe %s' % optionFile )

  def submit( self ):
    optionList = self.workflow.createCode()
    self.workflow.submit( optionList )
    statusDict = { self.jobID : { 0 : { 'onGoing' : 'yes' } } }
    self.dbTool.updateStep( statusDict )

  def generate( self ):
    optionList = self.workflow.createCode( self.debug )
