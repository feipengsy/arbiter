import MySQLdb

class dbTool:

  def __init__( self ):
    pass

  def connect( self ):
    conn = MySQLdb.connect(host="offline.ihep.ac.cn",user="arbuser",passwd="123456",db="arbiterDB")
    return conn    

  def addJob( self, job ):
    conn = self.connect()
    cur = conn.cursor()
    param = ( job.jobNum, job.name, job.status, job.stepCount, job.user )
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
  
  def addStep( self, job ):
    conn = self.connect()
    cur = conn.cursor()
    param = ( job.stepCount, job.jobNum )
    cur.execute( 'UPDATE WORKFLOW SET stepCount=%s WHERE workflowNum=%s', param )
    param = ( job.jobNum, job.stepCount, 'unDone', 'no' )
    cur.execute( 'INSERT INTO STEP VALUES(%s,%s,%s,%s)', param )
    cur.close()
    conn.close()
    return True

  def getNewWorkflowNum( self ):
    conn = self.connect()
    cur = conn.cursor()
    cur.execute('SELECT workflowNum FROM WORKFLOW')
    num = int( max( cur.fetchall() )[0] ) + 1
    cur.close()
    conn.close()
    return num

  def getStepCount( self, jobNum ):
    conn = self.connect()
    cur = conn.cursor()
    cur.execute('SELECT stepCount FROM WORKFLOW WHERE workflowNum=%s', jobNum )
    result = int( cur.fetchone()[0] )
    cur.close()
    conn.close()
    return result

  def updateWorkflow( self, workflowInfo ):
    conn = self.connect()
    cur = conn.cursor()
    jobNum = workflowInfo['jobNum']
    del workflowInfo['jobNum']
    for k,v in workflowInfo.items():
      if k == 'status':
        param = ( v, jobNum )
        cur.execute( "UPDATE WORKFLOW SET status=%s WHERE workflowNum='%s'", param )
      if k == 'stepCount':
        param = ( v, jobNum )
        cur.execute( "UPDATE WORKFLOW SET stepCount=%s WHERE workflowNum='%s'", param )       
    cur.close()
    conn.close()
    return True

  def updateStep( self, statusDict ):
    conn = self.connect()
    cur = conn.cursor()
    for jobNum,jobStepInfo in statusDict.items():
      for stepNum,stepInfo in jobStepInfo.items():
        for name,value in stepInfo.items():
          if name == 'status':
            param = ( value, stepNum, jobNum )
            cur.execute( "UPDATE STEP SET status=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
          if name == 'onGoing':
            param = ( value, stepNum, jobNum )
            cur.execute( "UPDATE STEP SET onGoing=%s WHERE stepNum='%s' AND workflowNum='%s'", param )
    cur.close()
    conn.close()
    return True
