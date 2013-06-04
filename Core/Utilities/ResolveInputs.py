import os, string, glob

def resolveInputData( inputData, extra = None ):
  if os.path.isdir( inputData ):
    return dataFromDir( inputData, extra )
  elif os.path.isfile( inputData ):
    f = open( inputData, 'r')
    fileTab = f.readline()
    if fileTab.lower() == 'input file list:\n':
      return dataFromFile( inputData )
    f.close()
  else:
    return [ inputData ]

def dataFromFile( inputData ):
  f = open( inputData, 'r' )
  templist = f.readlines()
  f.close()
  del templist[0]
  alist = []
  for f in templist:
    f = f.strip()
    alist.append(f)
  return sorted(alist)

def dataFromDir( inputData, extra ):
  afile = os.listdir( inputData )
  fileList = []
  for f in afile:
    if inputData[-1] == '/':
      filename = inputData + f
    else:
      filename = inputData + '/' + f
    if os.path.isfile( filename ):
      if extra != None:
        check = '.' + extra
        if f.endswith(check):
          fileList.append( filename )
      else:
        fileList.append( filename )
  return sorted(fileList)
