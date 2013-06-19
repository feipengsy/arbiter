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
        if name == 'output':
          if value[-1] == '/':
            self.ParametersDict['output'] = value
          else:
            self.ParametersDict['output'] = value + '/'
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
      #generate table string
      outputDirectory = ''
      if self.ParametersDict['output']:
        outputDirectory = self.ParametersDict['output']
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
        if pat.search( optionString ):
          optionString = optionString.replace( pat.search( optionString ).groups()[0], 'EvtDecay.userDecayTableName="' + tableString + '";' )
      pat = re.compile( '(RootCnvSvc.digiRootO[uU]tputFile *=.*)\n' )
      if pat.search( optionString ):
        ostring = pat.search( optionString ).groups()[0]
        # strip all the space quotes
        nstring = ostring.replace( ' ', '' )
        nstring = ostring.replace( '"', '' )
        nstring = ostring.replace( "'", '' )
        # add the output directory
        if outputDirectory:
          nstring = nstring.replace( 'putFile=', 'putFile=' + outputDirectory )
        # add the quotes
        nstring = nstring.replace( 'putFile=', 'putFile="' )
        nstring = nstring + '"'
        # add the semicolon
        if not nstring.endswith(';'):
          nstring = nstring + ';'
        # get the output
        pat = re.compile( 'RootCnvSvc.digiRootO[uU]tputFile="(.*)";' )
        outputFile = pat.search( nstring )[0]
        optionString = optionString.replace( ostring, nstring )      
      return ( self.ParametersDict['input'], outputFile, optionString )

  def toTXTFile( self, filename ):
    f = open( filename, 'w+')
    inputList, outputFile, ret = self.toTXT()
    f.write( ret )
    f.close()
    return ( inputList, outputFile, filename )
