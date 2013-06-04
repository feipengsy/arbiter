import random
import re

class other:

  def __init__( self, parameters = None ):

    self.ParametersDict = {}
    self.ParametersDict['input'] = []
    self.ParametersDict['output'] = []
    self.ParametersDict['table'] = ''

    self.optionTemplet = ''
    self.parametersList = parameters
    self.subJobCount = 1

  def resolveParametersList( self ):
    if self.parametersList != None:
      for parameter in self.parametersList:
        name = parameter.getName()
        value = parameter.getValue()
        if name == 'optionTemplet':
          self.optionTemplet = value
        if name == 'input':
          self.ParametersDict['input'] = value
        if name == 'count':
          self.subJobCount = value
        if name == 'table':
          self.ParametersDict['table'] = value
    return True

  def toTXT( self ):
    result = self.resolveParametersList()
    if result:
      #read job-option templet
      f = open( self.optionTemplet, 'r' )
      optionString = f.read()
      f.close()

      #generate input string
      inputString = ''
      for inputFile in self.ParametersDict['input']:
        inputString = inputString + '"' + inputFile + '",'
      inputString = inputString[:-1]

      #generate output string
      if self.ParametersDict['input']:
        if len( self.ParametersDict['input'] ) == 1:
          outputString = self.ParametersDict['input'][0]
          outTempList = outputString.split( '/' )
          outputString = outTempList[-1]
          outTempList = outputString.split( '.' )
          outputString = outTempList[0]
        else:
          outputString = ''
      else:
        outputString = ''
      for parameter in self.parametersList:
        name = parameter.getName()
        value = parameter.getValue()
        if name == 'output':
          if value[-1] == '/':
            outputString = value + outputString
          else:
            outputString = value + '/' + outputString
      #generate table string
      tableString = ''
      if self.ParametersDict['table']:
        tableString = self.ParametersDict['table']
        

      """
      # input assignment
      startIndex = optionString.find( 'RawDataInputSvc.InputFiles' )
      if startIndex != -1:
        endIndex = optionString.find( ';', startIndex)
        optionString.replace( optionString[startIndex:endIndex], 'RawDataInputSvc.InputFiles={' + inputString +'}' )

      startIndex = optionString.find( 'EventCnvSvc.digiRootInputFile' )
      if startIndex != -1:
        endIndex = optionString.find( ';', startIndex)
        optionString.replace( optionString[startIndex:endIndex], 'EventCnvSvc.digiRootInputFile={' + inputString +'}' )
      """

      # i/o assignment
      optionString = optionString.replace( '"#INPUT"', inputString )
      optionString = optionString.replace( '#INPUT', inputString )
      optionString = optionString.replace( '#OUTPUT', outputString )
      
      optionString = optionString.replace( '#NUMBER', str(self.subJobCount) )
      optionString = optionString.replace( '#RANDOM', str(random.randint(1000,9999)) )
      if tableString:
        pat = re.compile( '(EvtDecay.userDecayTableName.*".*".*;)' )
        optionString = optionString.replace( pat.search( optionString ).groups()[0], 'EvtDecay.userDecayTableName="' + tableString + '";' )

      return optionString

  def toTXTFile( self, filename ):
    f = open( filename, 'w+')
    ret = self.toTXT()
    f.write( ret )
    f.close
    return filename
