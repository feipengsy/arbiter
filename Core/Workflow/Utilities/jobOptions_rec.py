class jobOptionsRec:

  def __init__( self, parametersList = None ):
    self.IncludeDict = {}
    self.IncludeDict['InputDataConversion'] = "'$ROOTIOROOT/share/jobOptions_ReadRoot.txt'"
    self.IncludeDict['TriggerMaker'] = None
    self.IncludeDict['EventLoop'] = "'$OFFLINEEVENTLOOPMGRROOT/share/OfflineEventLoopMgr_Option.txt'"
    self.IncludeDict['BackgroundMixing'] = "'$BESEVENTMIXERROOT/share/jobOptions_EventMixer_rec.txt'"
    self.IncludeDict['CalibData'] = "'$CALIBSVCROOT/share/job-CalibData.txt'"
    self.IncludeDict['MagneticField'] = "'$MAGNETICFIELDROOT/share/MagneticField.txt'"
    self.IncludeDict['EventStartTime'] = "'$ESTIMEALGROOT/share/job_EsTimeAlg.txt'"
    self.IncludeDict['MdcRec'] = "'$MDCXRECOROOT/share/jobOptions_MdcPatTsfRec.txt'"
    self.IncludeDict['MdcKalmanRec'] = "'$KALFITALGROOT/share/job_kalfit_numf_data.txt'"
    self.IncludeDict['MdcDedxRec'] = "'$MDCDEDXALGROOT/share/job_dedx_all.txt'"
    self.IncludeDict['TrkExtRec'] = "'$TRKEXTALGROOT/share/TrkExtAlgOption.txt'"
    self.IncludeDict['TofRec'] = "'$TOFRECROOT/share/jobOptions_TofRec.txt'"
    self.IncludeDict['TofEnergyRec'] = "'$TOFENERGYRECROOT/share/TofEnergyRecOptions_MC.txt'"
    self.IncludeDict['EmcRec'] = "'$EMCRECROOT/share/EmcRecOptions.txt'"
    self.IncludeDict['EmcTimRec'] = None
    self.IncludeDict['MucRec'] = "'$MUCRECALGROOT/share/jobOptions_MucRec.txt'"
    self.IncludeDict['ROOTIO'] = "'$ROOTIOROOT/share/jobOptions_Dst2Root.txt'"
    self.IncludeDict['Calib'] = "'$CALIBSVCROOT/share/calibConfig_rec_mc.txt'"
    self.IncludeDict['EventAssembly'] = "'$EVENTASSEMBLYROOT/share/EventAssembly.txt'"
    self.IncludeDict['PrimaryVertexFit'] = "'$PRIMARYVERTEXALGROOT/share/jobOptions_kalman.txt'"
    self.IncludeDict['VeeVertexFit'] = "'$VEEVERTEXALGROOT/share/jobOptions_veeVertex.txt'"
    self.IncludeDict['HltMaker'] = "'$HLTMAKERALGROOT/share/jobOptions_HltMakerAlg.txt'"
    self.IncludeDict['EventNavigator'] = "'$EVENTNAVIGATORROOT/share/EventNavigator.txt'"
    self.InList = ['InputDataConversion', 'TriggerMaker','EventLoop', 'BackgroundMixing', 'CalibData', 'MagneticField', 'EventStartTime', 'MdcRec', 'MdcKalmanRec', 'MdcDedxRec', 'TrkExtRec', 'TofRec', 'TofEnergyRec', 'EmcRec', 'EmcTimRec', 'MucRec', 'ROOTIO', 'Calib', 'EventAssembly', 'PrimaryVertexFit', 'VeeVertexFit', 'HltMaker', 'EventNavigator']


    self.ParametersDict = {}
    self.ParametersDict['input'] = []
    self.ParametersDict['output'] = []
    self.ParametersDict['RandomSeed'] = '100'
    self.ParametersDict['OutputLevel'] = '5'
    self.ParametersDict['EventNumber'] = '50'
    self.PaList = ['input', 'output', 'RandomSeed', 'OutputLevel', 'EventNumber']


    self.parametersList = parametersList
    #self.CosmicFlag = CosmicFlag
    #self.MFFlag = MFFlag

  def resolveParametersList( self ):
    if self.parametersList != None:
      for parameter in self.parametersList:
        name = parameter.getName()
        value = parameter.getValue()
        extra = parameter.getExtra()
        if name in self.InList:
          if value:
            self.IncludeDict[name] = "'" + value + "'"
          if extra:
            for ret in extra:
              self.IncludeDict[name] += '\n'
              self.IncludeDict[name] += ret
              self.IncludeDict[name] += ';'
        if name in self.PaList:
          self.ParametersDict[name] = value
    return True

  def toTXT( self ):
    result = self.resolveParametersList()
    if result:
      ret = '//jobOptions_rec\n'
      for k in self.InList:
        v = self.IncludeDict[k]
        if v:
          ret += '//%s\n' % k
          ret += "#include %s\n" % v
      for k in self.PaList:
        v = self.ParametersDict[k]
        if k == 'input' or k == 'output':
          ret += '\n//I/O Assignment'
          if k == 'input':
            ret += '\nEventCnvSvc.digiRootOutputFile = {'
            for value in v:
              ret = ret + '"' + value + '" '
            ret += '};'
          elif k == 'output':
            ret += '\nRootCnvSvc.digiRootOutputFile = '
            for value in v:
              ret = ret + '"' + value + '"'
            ret += ';'
        elif k == 'RandomSeed':
          ret = ret +  '\n//' + k
          ret = ret + '\nBesRndmGenSvc.RndmSeed = ' + v + ';'
        elif k =="OutputLevel":
          ret = ret +  '\n//' + k
          ret = ret + '\nMessageSvc.OutputLevel = ' + v + ';'
        elif k =="EventNumber":
          ret = ret +  '\n//' + k
          ret = ret + '\nApplicationMgr.EvtMax = ' + v + ';'
      return ret
    else:
      return False

  def toTXTFile( self, filename ):
    f = open( filename, 'w+')
    ret = self.toTXT()
    f.write( ret )
    f.close
    return filename
