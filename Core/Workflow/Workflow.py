import os
from arbiter.Core.Workflow.Step import Step
from arbiter.Core.Workflow.Utilities.Parameter import *
from arbiter.Core.Utilities.Constants import *

class Workflow:

  def __init__( self ):
    global tempDirectory
    self.steps = stepsPool()
    self.tempDirectory = tempDirectory
    self.parameters = ParametersCollection()
    self.currentStepID = 0
    self.jobID = 0
    self.user = ''
    self.jobName = ''
    self.typeList = [ 'SimulationRec', 'RealDataRec', 'other' ]

  def addStep( self, stepType, stepName, jobID ):
    step = Step()
    step.setType( stepType )
    step.setUserName( stepName )
    step.setStepID( self.currentStepID )
    step.setJobID( jobID )
    step.setName()
    #step.createStep()
    self.currentStepID += 1
    self.steps.append( step )

  def creatCode( self ):
    resultList = []
    for step in self.steps:
      result = step.creatCode()
      resultList.append( result )
    # Be careful here!
    return resultList

  def execute( self, optionList ):
    for optionFile in optionList:
      olist = optionFile.split( '/' )
      direct = ''
      for element in olist[0:-1]:
        if element:
          direct = direct + '/' + element
      os.chdir( direct )
      os.system( 'boss.exe %s' % olist[-1] )
    return True

  def submit( self, optionList ):
    for optionFile in optionList:
      olist = optionFile.split( '/' )
      direct = ''
      for element in olist[0:-1]:
        if element:
          direct = direct + '/' + element
      os.chdir( direct )
      os.system( 'boss -q %s' % olist[-1] )
    return True

  def toXML( self ):
    ret = '<workflow jobID="' + str( self.jobID ) + '" jobName="' + self.jobName + '" user="' + self.user + '">\n'
    ret = ret + self.parameters.toXML()
    ret = ret + self.steps.toXML()
    ret = ret + '</workflow>'
    return ret
