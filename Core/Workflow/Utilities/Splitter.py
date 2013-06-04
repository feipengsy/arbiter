import types,os
from arbiter.Core.Workflow.subStep import *
from arbiter.Core.Utilities.ReturnValues import *

class dataSplitter:

  def __init__( self, splitterInfo ):
    self.splitterInfo = splitterInfo
    self.maxFileNum = None
    self.maxInputSize = None
    if splitterInfo[ 'maxFileNumber' ] != None:
      self.setMaxFileNumber( splitterInfo[ 'maxFileNumber' ] )
    if splitterInfo.has_key( 'maxInputSize' ):
      if splitterInfo[ 'maxInputSize' ] != None:
        self.setMaxInputSize( splitterInfo[ 'maxInputSize' ] )


  def setMaxFileNumber( self, num ):
    try:
      self.maxFileNum = int( num )
    except ValueError:
      return S_ERROR( 'unexpected max file number for each sub-job' )
    else:
      return S_OK()

  def setMaxInputSize( self, size ):
    try:
      self.maxFileSize = int( size )
    except ValueError:
      return S_ERROR( 'unexpected max input size for each sub-job' )
    else:
      return S_OK()

  def split( self, step ):
    allInputs = step.inputData
    subJobs = []
    subJobCount = 0
    # try to count how many subjobs there will be
    if self.maxFileNum != None:
      allNum = len( allInputs ) / self.maxFileNum
    position = len( str( allNum ) )
    # start to split
    while True:
      subJob = subStep()
      # generate subjob name
      subJobName = str( subJobCount )
      while True:
        if len( subJobName ) == position:
          break
        else:
          subJobName = '0' + subJobName

      subJob.setName( step.name + subJobName )
      for parameter in step.parameters:
        name = parameter.name
        value = parameter.value
        ptype = parameter.type
        extra = parameter.extra
        subJob.setParameter( name, value, ptype, extra )
        if name == 'optionName':
          subJob.setName( value + subJobName )
      subJob.setParameter( 'count', subJobCount, '', '' )
      for parameter in subJob.parameters:
        if parameter.name == 'input':
          inputExtra = parameter.extra
          subJob.parameters.remove( parameter )
      numCount = 0
      while True:
        if len( allInputs ) == 0:
          break
        subJob.setInputData( allInputs[0] )
        numCount += 1
        del allInputs[0]
        if self.maxFileNum != None:
          if numCount >= self.maxFileNum:
            break
      subJobCount += 1
      subJobs.append( subJob )
      if len( allInputs ) == 0:
        break
    return subJobs

class numberSplitter:
    
  def __init__( self, splitterInfo ):
    self.splitterInfo = splitterInfo
    self.setSplitingNumber( splitterInfo['number'] )

  def setSplitingNumber( self, num ):
    try:
      self.splitingNumber = int( num )
    except ValueError:
      return S_ERROR( 'unexpected spliting number for each sub-job' )
    else:
      return S_OK()

  def split( self, step ):
    subJobs = []
    subJobCount = 0
    # try to count how many subjobs there will be
    position = len( str( self.splitingNumber ) )
    # start to split
    while True:
      subJob = subStep()
      # generate subjob name
      subJobName = str( subJobCount )
      while True:
        if len( subJobName ) == position:
          break
        else:
          subJobName = '0' + subJobName
      subJob.setName( step.name + subJobName )
      for parameter in step.parameters:
        name = parameter.name
        value = parameter.value
        ptype = parameter.type
        extra = parameter.extra
        subJob.setParameter( name, value, ptype, extra )
        if name == 'optionName':
          subJob.setName( value + subJobName )
      subJob.setParameter( 'count', subJobCount, '', '' )
      subJobCount += 1
      subJobs.append( subJob )
      if subJobCount == self.splitingNumber:
        break
    return subJobs
