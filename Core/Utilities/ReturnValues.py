def S_ERROR( messageString = '' ):
  print messageString
  return { 'OK' : False, 'Message' : str( messageString ) }

def S_OK( value = '' ):
  return { 'OK' : True, 'Value' : value }
