import MySQLdb
from arbiter.Core.Utilities.ReturnValues import *

class dbTool:

  def __init__( self ):
    pass

  def connect( self ):
    try:
      conn = MySQLdb.connect(host="DBHOSTNAME",user="DBUSER",passwd="DBPASSWORD",db="arbiterDB")
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
      return S_ERROR( 'Error when adding workflow to database' )
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
      return S_ERROR( 'Error when querying database' )
    result = cur.fetchall()
    if not result:
      return S_ERROR( 'Can not find job information in database' )
    for step in job.steps:
      try:
        cur.execute( 'select * from STEP where workflowNum=%s and stepNum=%s ' % ( job.jobID, step.stepID) )
      except:
        return S_ERROR( 'Error when querying database' )
      result = cur.fetchall()
      if not result:
        return S_ERROR( 'Can not find step information in database' )
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
    param = ( step.jobID, step.stepID, 'unDone', 'no' )
    try:
      cur.execute( 'INSERT INTO STEP VALUES(%s,%s,%s,%s)', param )
    except:
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
      return S_ERROR( 'Error when querying database' )
    ID = int( max( cur.fetchall() )[0] ) + 1
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
      cur.execute('SELECT stepCount FROM WORKFLOW WHERE workflowNum=%s', jobID )
    except:
      return S_ERROR( 'Error when querying database' )
    result = int( cur.fetchone()[0] )
    cur.close()
    conn.close()
    return S_OK( result )

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
          return S_ERROR( 'Error when updating workflow in database' )
      if k == 'stepCount':
        param = ( v, jobID )
        try:
          cur.execute( "UPDATE WORKFLOW SET stepCount=%s WHERE workflowNum='%s'", param )       
        except:
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
              return S_ERROR( 'Error when updating step in database' )
          if name == 'onGoing':
            param = ( value, stepID, jobID )
            try:
              cur.execute( "UPDATE STEP SET onGoing=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
            except:
              return S_ERROR( 'Error when updating step in database' )
    cur.close()
    conn.close()
    return S_OK()
