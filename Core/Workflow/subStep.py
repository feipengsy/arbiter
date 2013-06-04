from arbiter.Core.Workflow.Utilities.Parameter import *

class subStep:

  def __init__( self, mainStep = None ):
    self.module = None
    self.parameters = ParametersCollection()
    self.type = None
    self.name = None
    self.inputData = []
    self.splitter = None
    self.typeDict = { 'SimulationRec' : 'jobOptionsRec', 'RealDataRec' : 'jobOptionsRecData' }
    self.splitterDict = { 'dataSplitter' : 'dataSplitter'}
    if mainStep:
      self.type = mainStep.type
      self.parameters = mainStep.parameters

  def setType( self, stepType ):
    self.type = stepType

  def setName( self, stepName ):
    self.name = stepName

  def setInputData( self, inputs ):
    if isinstance( inputs, list ):
      self.inputData += inputs
    elif type( inputs ) == type( '' ):
      self.inputData += [inputs]
    self.setParameter( 'input', self.inputData, '', '' )

  def setParameter( self, name, value, ptype, extra):
    parameter = Parameter( name, value, ptype, extra)
    self.parameters.appendOrOverwirte( parameter )


