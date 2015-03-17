@echo off


REM ############################################################################
REM Folgende Parameter m�ssen �berpr�ft und evtl. angepasst werden

REM Java-Klassenpfad
SET jar=de.bsvrz.dua.aggrlve-runtime.jar

REM Argumente f�r die Java Virtual Machine
SET jvmArgs=-showversion -Dfile.encoding=ISO-8859-1 -Xms32m -Xmx256m -cp ..\%jar%
REM Parameter f�r den Datenverteiler
SET benutzer=Tester
SET passwortDatei=..\..\..\skripte-dosshell\passwd
SET dav1Host=localhost
SET dav1AppPort=8083



REM Applikation starten
CHCP 1252
TITLE Aggregation LVE
java %jvmArgs% ^
de.bsvrz.dua.aggrlve.AggregationLVE ^
-debugLevelStdErrText=info ^
-debugSetLoggerAndLevel=:config ^
-datenverteiler=%dav1Host%:%dav1AppPort% ^
-benutzer=%benutzer% ^
-authentifizierung=%passwortDatei% ^
-KonfigurationsBereichsPid=kb.duaTestObjekte

REM Nach dem Beenden warten, damit Meldungen gelesen werden k�nnen
PAUSE
