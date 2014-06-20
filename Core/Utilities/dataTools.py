import os

class dataTool:

  def __init__( self, job ):
    self.jobDirectory = job.tempDirectory + 'workflowTemp/' + str(job.jobID) + '/'
    self.tempDirecotry = job.tempDirectory

  def checkExistence( self, job ):
    stepCount = job.stepCount
    workflowInfoFileName = self.jobDirectory + 'WorkflowInfo'
    if not os.path.exists( workflowInfoFileName ):
      return S_ERROR( 'Can not fine WorkflowInfo, workflow may be already deleted.' )
    for stepID in range( stepCount ):
      stepInfoFileName = self.jobDirectory + str(stepID) + '/StepInfo'
      if not os.path.exists( stepInfoFileName ):
        return S_ERROR( 'Can not fine StepInfo, workflow may be already deleted.' )
    return S_OK()

  def updateFile( self, filename, line, value ):
    filename = self.jobDirectory + filename
    if not os.path.exists( filename ):
      return S_ERROR( 'Can not find file: %s, job may be already deleted.', %filename )
    f = open( filename, 'r' )
    fList = f.readlines()
    fList[line-1] = str(value) + '\n'
    f.close()
    f = open( filename, 'w' )
    f.writelines()
    f.close()
    return S_OK()
    
  def updateStep( self, statusDict, update = 'status' ):
    for stepID, status in statusDict.items():
      stepInfoFileName = self.jobDirectory + str(stepID) + '/StepInfo'
      if 'status' == update:
        result = self.updateFile( stepInfoFileName, 1, status )
      if 'onGoing' == update:
        result = self.updateFile( stepInfoFileName, 2, status  )
      return result

  def updateWorkflow( self, status, update = 'status' ):
    workflowInfoFileName = self.jobDirectory + 'WorkflowInfo'
    if 'status' == update:
      result = self.updateFile( workflowInfoFileName, 1, status )
    if 'stepCount' == update:
      result = self.updateFile( workflowInfoFileName, 2, status  )
    return result

  def getNewWorkflowID( self ):
    fileList = os.listdir( self.tempDirectory )
    newID = 0
    for filename in fileList:
      if os.path.isfile(filename):
        if filename.endswith('.xml'):
          try:
            newID = max( newID, int(finename[:-4]) )
          except:
            continue
    self.jobDirectory = job.tempDirectory + 'workflowTemp/' + str( newID ) + '/'
    return S_OK( newID )

  def addJob( self, job ):
    workflowInfoFileName = self.jobDirectory + 'WorkflowInfo'
    try:
      f = open( workflowInfoFileName, 'w' )
      f.write( '%s\n%s\n' % ( job.status, job.stepCount ) )
      f.close()
    except:
      return S_ERROR()
    return S_OK()

  def addStep( self, step ):
    stepInfoFileName = self.jobDirectory + str( step.stepID )  + '/StepInfo'
    try:
      f = open( stepInfoFileName, 'w' )
      #TODO step status
      f.write( '%s\n%s\n%s\n' % ( 'unDone', 'no', step.getOptionFileDirectory() ) )
      f.close()
    except:
      return S_ERROR()
    return S_OK()

  def getNextStepID( self, job ):
    pass
