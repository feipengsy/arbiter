import MySQLdb
from arbiter.Core.Utilities.ReturnValues import *

class dbTool:

  def __init__( self ):
    pass

  def connect( self ):
    try:
      conn = MySQLdb.connect(host="localhost",user="arbuser",passwd="123456",db="arbiterDB")
    except:
      return S_ERROR( 'Can not connect to the database' )
    return S_OK( conn )

  def addJob( self, job ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    param = ( job.jobID, job.name, job.status, job.stepCount, job.user )
    try:
      cur.execute( 'INSERT INTO WORKFLOW VALUES(%s,%s,%s,%s,%s)', param )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when adding workflow to database' )
    cur.close()
    conn.close()
    return S_OK()
  
  def addSubJob( self, workflowID, stepID, subjobName, inputFileList, outputFile ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    inputFile = ';'.join( inputFileList )
    param = ( workflowID, stepID, subjobName )
    try:
      cur.execute( 'delete from JOB where workflowID=%s and stepID=%s and jobName=%s', param )     
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when querying database' )
    param = ( workflowID, stepID, subjobName, 'unsubmitted', inputFile, outputFile )
    try:
      cur.execute( 'insert into JOB values(%s,%s,%s,%s,%s,%s)', param )     
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when adding subjob into database' )
    return S_OK()    

  def getInputFromStep( self, workflowID, stepID, generate ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    if generate:
      param = ( workflowID, stepID, 'done' )
      try:
        cur.execute( 'select outputFile from JOB where workflowID=%s and stepID=%s and status=%s', param )
      except:
        cur.close()
        conn.close()
        return S_ERROR( 'Error when getting inputs from database' )
    else:
      param = ( workflowID, stepID )
      try:
        cur.execute( 'select outputFile from JOB where workflowID=%s and stepID=%s', param )
      except:
        cur.close()
        conn.close()
        return S_ERROR( 'Error when getting inputs from database' )
    result = cur.fetchall()
    inputList = []
    if result:
      for tup in result:
        inputList.append( tup[0] )
    return result

  def deleteJob( self, jobID ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    try:
      cur.execute( 'delete from WORKFLOW where workflowNum=%s' % jobID )
      cur.execute( 'delete from STEP where workflowNum=%s' % jobID )
      cur.execute( 'delete from JOB where workflowID=%s' % jobID )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when deleting workflow from database' )
    cur.close()
    conn.close()
    return S_OK()

  def deleteSubJob( self, workflowID, stepID ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    param = ( workflowID, stepID )
    try:
      cur.execute( 'delete from JOB where workflowID=%s and stepID=%s', param )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when deleting subjob from database' )
    cur.close()
    conn.close()
    return S_OK()
      

  def checkExistence( self, job ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    try:
      cur.execute( 'select * from WORKFLOW where workflowNum=%s' % job.jobID )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when querying database' )
    result = cur.fetchall()
    if not result:
      return S_ERROR( 'Can not find job information in database' )
    for step in job.workflow.steps:
      try:
        cur.execute( 'select * from STEP where workflowNum=%s and stepNum=%s ' % ( job.jobID, step.stepID) )
      except:
        cur.close()
        conn.close()
        return S_ERROR( 'Error when querying database' )
      result = cur.fetchall()
      if not result:
        cur.close()
        conn.close()
        return S_ERROR( 'Can not find step information in database' )
    cur.close()
    conn.close()
    return S_OK( '' )  

  def getJobList( self, user ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    try:
      cur.execute('SELECT * FROM WORKFLOW WHERE user="%s"' % user)
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when querying database' )
    jobList = cur.fetchall()
    cur.close()
    conn.close()
    return S_OK( jobList )
  
  def addStep( self, step ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    param = ( step.jobID, step.stepID, 'unDone', 'no', step.getOptionFileDirectory() )
    try:
      cur.execute( 'INSERT INTO STEP VALUES(%s,%s,%s,%s,%s)', param )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when adding step to database' )
    cur.close()
    conn.close()
    return S_OK()

  def getNewWorkflowID( self ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    try:
      cur.execute('SELECT workflowNum FROM WORKFLOW')
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when querying database' )
    result = cur.fetchall()
    if not result:
      return S_OK(0)
    ID = int( max( result )[0] ) + 1
    cur.close()
    conn.close()
    return S_OK( ID )

  def getStepCount( self, jobID ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    try:
      cur.execute('SELECT stepCount FROM WORKFLOW WHERE workflowNum=%s' % jobID )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when querying database' )
    result = cur.fetchone()
    if not result:
      return S_ERROR( 'Can not find workflow %s' % jobID )
    cur.close()
    conn.close()
    return S_OK( int( result[0] ) )

  def getWorkarea( self, jobID, stepID):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    param = ( jobID, stepID )
    try:
      cur.execute( 'select workarea from STEP where workflowNum=%s and stepNum=%s', param )
    except:
      cur.close()
      conn.close()
      return S_ERROR( 'Error when querying database' )
    result = cur.fetchone()
    cur.close()
    conn.close()
    if not result:
      return S_ERROR( 'Can not find the step refered' ) 
    return S_OK( result[0] )    

  def updateWorkflow( self, workflowInfo ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    jobID = workflowInfo['jobID']
    del workflowInfo['jobID']
    for k,v in workflowInfo.items():
      if k == 'status':
        param = ( v, jobID )
        try:
          cur.execute( "UPDATE WORKFLOW SET status=%s WHERE workflowNum='%s'", param )
        except:
          cur.close()
          conn.close()
          return S_ERROR( 'Error when updating workflow in database' )
      if k == 'stepCount':
        param = ( v, jobID )
        try:
          cur.execute( "UPDATE WORKFLOW SET stepCount=%s WHERE workflowNum='%s'", param )       
        except:
          cur.close()
          conn.close()
          return S_ERROR( 'Error when updating workflow in database' )
    cur.close()
    conn.close()
    return S_OK()

  def updateStep( self, statusDict ):
    result = self.connect()
    if not result['OK']:
      return result
    conn = result['Value']
    cur = conn.cursor()
    for jobID,jobStepInfo in statusDict.items():
      for stepID,stepInfo in jobStepInfo.items():
        for name,value in stepInfo.items():
          if name == 'status':
            param = ( value, stepID, jobID )
            try:
              cur.execute( "UPDATE STEP SET status=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
            except:
              cur.close()
              conn.close()
              return S_ERROR( 'Error when updating step in database' )
          if name == 'onGoing':
            param = ( value, stepID, jobID )
            try:
              cur.execute( "UPDATE STEP SET onGoing=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
            except:
              cur.close()
              conn.close()
              return S_ERROR( 'Error when updating step in database' )
    cur.close()
    conn.close()
    return S_OK()