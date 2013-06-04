class ParametersCollection( list ):

  def __init__( self ):
    list.__init__( self )

  def appendOrOverwirte( self, parameter ):
    for p in self:
      if p.name == parameter.name:
        self.remove(p)
    self.append( parameter )

  def toXML( self ):
    ret = ''
    for parameter in self:
      ret = ret + parameter.toXML()
    return ret


class Parameter:

  def __init__( self, name = None, value = None, type = None, extra = None ):

    self.name = ''
    self.value = ''
    self.type = 'string'
    self.extra = ''

    if name != None:
      self.name = name
    if value != None:
      self.value = value
    if type != None:
      self.type = type
    if extra != None:
      self.extra = extra

  def getName( self ):
    return self.name

  def getValue( self ):
    return self.value

  def getType( self ):
    return self.type

  def getExtra( self ):
    return self.extra

  def toXML( self ):
    return '<parameter name="' + self.name + '" type="' + str( self.type )\
    + '" value="' + str( self.getValue() ) + '" extra="' + self.extra + '">'\
    + '</parameter>\n'

class stepsPool( list ):

  def __init__( self ):
    list.__init__( self )

  def toXML( self ):
    ret = ''
    for step in self:
      ret = ret + step.toXML()
    return ret
