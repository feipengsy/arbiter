from arbiter.Core.Utilities.ReturnValues import *

class baseScript:

  def __init__( self ):
    pass

  def checkRationality( self ):
    for arg in self.argv:
      if arg.startswith( '--' ):
        oarg = arg.strip( '--' )
        if oarg not in self.argvList:
          return S_ERROR( self.__doc__ )
      elif arg.startswith( '-' ):
        oarg = arg.strip( '-' )
        for s in oarg:
          if s not in self.argvList:
            return S_ERROR( self.__doc__ )
    return S_OK()

  def parseArgv( self ):
    argvList = self.argv
    parsedDict = { 'unbound' : [] }
    while True:
      if not argvList:
        break
      parsingArgv = argvList[0]
      del argvList[0]
      # check if the argv is a long key
      if parsingArgv.startswith( '--' ):
        parsingArgv = parsingArgv.strip( '--' )
        parsedDict[parsingArgv] = ''
        if self.argvDict[parsingArgv] == 'bound': 
          if not argvList:
            return S_ERROR( 'argument %s need a parameter bounded' % parsingArgv)
          elif '-' in argvList[0]:
            return S_ERROR( 'argument %s need a parameter bounded' % parsingArgv)
          else:
            parsedDict[parsingArgv] = argvList[0]
            del argvList[0]
        else:
          continue
      # then check if the argv is a short key
      elif parsingArgv.startswith( '-' ):
        parsingArgv = parsingArgv.strip( '-' )
        if len( parsingArgv ) > 1:
          for ele in parsingArgv:
            parsedDict[ele] = ''
            if self.argvDict[ele] == 'bound':
              return S_ERROR( 'argument %s need a parameter bounded' % ele)
        else:
          parsedDict[parsingArgv] = ''
          if self.argvDict[parsingArgv] == 'bound':
            if not argvList:
              return S_ERROR( 'argument %s need a parameter bounded' % parsingArgv)
            elif '-' in argvList[0]:
              return S_ERROR( 'argument %s need a parameter bounded' % parsingArgv)
            else:
              parsedDict[parsingArgv] = argvList[0]
              del argvList[0]
          else:
            continue
      # the argv is not a key, so it must be a unbound parameter
      else:
        parsedDict['unbound'].append(parsingArgv)
    return S_OK(parsedDict)
