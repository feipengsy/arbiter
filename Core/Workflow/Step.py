import os
import shutil
from arbiter.Core.Workflow.Utilities.Parameter import *
from arbiter.Core.Utilities.ResolveInputs import *
from arbiter.Core.Utilities.ReturnValues import *
from arbiter.Core.Workflow.Utilities.jobOptions_rec import *
from arbiter.Core.Workflow.Utilities.jobOptions_rec_data import *
from arbiter.Core.Workflow.Utilities.other import *
from arbiter.Core.Workflow.Utilities.Splitter import *
from arbiter.Core.Workflow.Workflow import *
from arbiter.Core.Utilities.Constants import *

class Step:

  def __init__( self ):
    global tempDirectory
    self.module = None
    self.parameters = ParametersCollection()
    self.type = None
    self.name = None
    self.userName = None
    self.stepID = 0
    self.jobID = None
    self.tempDirectory = tempDirectory
    self.inputData = []
    self.splitter = None
    self.typeDict = { 'SimulationRec' : 'jobOptionsRec', 'RealDataRec' : 'jobOptionsRecData', 'other' : 'other' }
    self.splitterDict = { 'dataSplitter' : 'dataSplitter', 'numberSplitter': 'numberSplitter' }

  def createStep( self ):
    stepDir = self.tempDirectory + 'workflowTemp/' + str( self.jobID ) + '/'
    os.chdir( stepDir )
    try:
      os.mkdir( self.name )
    except:
      return S_ERROR( 'can not create step' )
    return S_OK()

  def setType( self, stepType ):
    self.type = stepType

  def setName( self ):
    self.name = self.type + 'step' + str( self.stepID )

  def setUserName( self, stepUserName ):
    self.userName = stepUserName

  def setStepID( self, ID ):
    self.stepID = ID

  def setJobID( self, ID ):
    self.jobId = ID

  def setInputData( self ):
    for parameter in self.parameters:
      if parameter.name == 'input':
        self.inputData = resolveInputData( parameter.value, parameter.extra )

  def setParameter( self, name, value, ptype, extra):
    parameter = Parameter( name, value, ptype, extra)
    self.parameters.appendOrOverwirte( parameter )

  def setSplitter( self, splitterInfo ):
    if splitterInfo[ 'name' ] not in self.splitterDict.keys():
      return S_ERROR( 'No splitter named %s' % splitterInfo[ 'name' ] )
    self.splitter = globals()[ splitterInfo[ 'name' ] ]( splitterInfo )

  def createCode( self, debug = False ):
    generated = self.checkIfGenerated()
    if generated:
      return generated
    else:
      optionTempDirectory = ''
      for p in self.parameters:
        if p.name == 'optionFileDirectory':
          if p.value[-1] == '/':
            optionTempDirectory = p.value
          else:
            optionTempDirectory = p.value + '/'
      if not optionTempDirectory:
        optionTempDirectory = self.tempDirectory + 'workflowTemp/' + str(self.jobID) + '/' + self.name + '/'
      if self.splitter == None:
        generaterName = self.typeDict[ self.type ]
        generater = globals()[ generaterName ]( self.parameters )
        optionFileName = optionTempDirectory + self.name + '.txt'
        generater.toTXTFile( optionFileName )
        if not debug:
          optionListFile = open( self.tempDirectory + 'workflowTemp/' + str(self.jobID) + '/' + self.name + '/' + 'optionList.txt', 'w' )
          optionListFile.write( optionFileName + '\n' )
          optionListFile.close()
        return [optionFileName]
      else:
        if not debug:
          optionListFile = open( self.tempDirectory + 'workflowTemp/' + str(self.jobID) + '/' + self.name + '/' + 'optionList.txt', 'w' )
        subjobs = self.splitter.split( self )
        optionFileList = []
        for subjob in subjobs:
          generaterName = self.typeDict[ self.type ]
          generater = globals()[ generaterName ]( subjob.parameters )
          optionFileName = optionTempDirectory + subjob.name + '.txt'
          generater.toTXTFile( optionFileName )
          if not debug:
            optionListFile.write( optionFileName + '\n' )
          optionFileList.append( optionFileName )
        print 'option files for workflow ' + str(self.jobID) + ' ' + self.name + ' are generated in ' + optionTempDirectory
        if not debug:
          optionListFile.close()
        return optionFileList

  def checkIfGenerated( self ):
    try:
      listFileName = self.tempDirectory + 'workflowTemp/' + str( self.jobID ) + '/' + self.name + '/optionList.txt'
      f = open( listFileName, 'r' )
    except IOError:
      return False
    optionList = f.readlines()
    f.close()
    if not optionList:
      return False
    newOptionList = []
    for optionFile in optionList:
      optionFile = optionFile.strip()
      newOptionList.append( optionFile )
      if not os.path.exists( optionFile ):
        return False
    return newOptionList

  def toXML( self ):
    ret = '<step name="' + self.name + '" type="' + self.type \
    + '" userName= "' + self.userName + '" >\n'
    ret = ret + self.parameters.toXML()
    if self.splitter:
      ret = ret + '<splitter '
      for k,v in self.splitter.splitterInfo.items():
        ret = ret + k + '="' + str( v ) + '" '
      ret = ret + '></splitter>\n'
    ret = ret + '</step>\n'
    return ret
