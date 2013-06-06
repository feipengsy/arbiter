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
    cur = conn.cursor()
    param = ( job.jobID, job.name, job.status, job.stepCount, job.user )
    cur.execute( 'INSERT INTO WORKFLOW VALUES(%s,%s,%s,%s,%s)', param )
    cur.close()
    conn.close()
    return True

  def getJobList( self, user ):
    conn = self.connect()
    cur = conn.cursor()
    cur.execute('SELECT * FROM WORKFLOW WHERE user="%s"' % user)
    jobList = cur.fetchall()
    cur.close()
    conn.close()
    return jobList
  
  def addStep( self, step ):
    conn = self.connect()
    cur = conn.cursor()
    param = ( step.jobID, step.stepID, 'unDone', 'no' )
    cur.execute( 'INSERT INTO STEP VALUES(%s,%s,%s,%s)', param )
    cur.close()
    conn.close()
    return True

  def getNewWorkflowID( self ):
    conn = self.connect()
    cur = conn.cursor()
    cur.execute('SELECT workflowNum FROM WORKFLOW')
    ID = int( max( cur.fetchall() )[0] ) + 1
    cur.close()
    conn.close()
    return ID

  def getStepCount( self, jobID ):
    conn = self.connect()
    cur = conn.cursor()
    cur.execute('SELECT stepCount FROM WORKFLOW WHERE workflowNum=%s', jobID )
    result = int( cur.fetchone()[0] )
    cur.close()
    conn.close()
    return result

  def updateWorkflow( self, workflowInfo ):
    conn = self.connect()
    cur = conn.cursor()
    jobID = workflowInfo['jobID']
    del workflowInfo['jobID']
    for k,v in workflowInfo.items():
      if k == 'status':
        param = ( v, jobID )
        cur.execute( "UPDATE WORKFLOW SET status=%s WHERE workflowNum='%s'", param )
      if k == 'stepCount':
        param = ( v, jobID )
        cur.execute( "UPDATE WORKFLOW SET stepCount=%s WHERE workflowNum='%s'", param )       
    cur.close()
    conn.close()
    return True

  def updateStep( self, statusDict ):
    conn = self.connect()
    cur = conn.cursor()
    for jobID,jobStepInfo in statusDict.items():
      for stepID,stepInfo in jobStepInfo.items():
        for name,value in stepInfo.items():
          if name == 'status':
            param = ( value, stepID, jobID )
            cur.execute( "UPDATE STEP SET status=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
          if name == 'onGoing':
            param = ( value, stepID, jobID )
            cur.execute( "UPDATE STEP SET onGoing=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
    cur.close()
    conn.close()
    return True
