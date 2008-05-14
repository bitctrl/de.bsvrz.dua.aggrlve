#!/bin/bash
source ../../../skripte-bash/einstellungen.sh

echo =================================================
echo =
echo =       Pruefungen SE4 - DUA, SWE 4.9 
echo =
echo =================================================
echo 

index=0
declare -a tests
declare -a testTexts

#########################
# Name der Applikation #
#########################
appname=aggrlve

########################
#     Testroutinen     #
########################

tests[$index]="dtv.AggregationLVEDTVJahr"
testTexts[$index]="Testet die Aggregation von DTV-Jahreswerten"
index=$(($index+1))

tests[$index]="dtv.AggregationLVEDTVMonat"
testTexts[$index]="Testet die Aggregation von DTV-Monatswerten"
index=$(($index+1))

tests[$index]="dtv.AggregationLVETVTag"
testTexts[$index]="Testet die Aggregation von TV-Tageswerten"
index=$(($index+1))

tests[$index]="AggregationLVE15Test"
testTexts[$index]="Allgemeine Tests fuer 1- und 5-Minuten-Intervall (entspricht den Testvorschriften aus PruefSpez Version 2.0 Abschnitt 5.1.10.2 u. 5.1.10.3 erster Abschnitt)"
index=$(($index+1))

AggregationLVE_15_60Test
tests[$index]="AggregationLVE_15_60Test"
testTexts[$index]="Testet, ob die 15-Minuten-Intervalle richtig aus den 5 Minuten-Intervallen berechnet werden und ob die 30- bzw. 60-Minuten-Intervalle richtig aus den 15- bzw. 30-Minuten-Intervallen berechnet werden"
index=$(($index+1))

tests[$index]="AggregationsIntervallTest"
testTexts[$index]="Allgemeine Tests"
index=$(($index+1))

########################
#      ClassPath       #
########################
cp="../../de.bsvrz.sys.funclib.bitctrl/de.bsvrz.sys.funclib.bitctrl-runtime.jar"
cp=$cp":../de.bsvrz.dua."$appname"-runtime.jar"
cp=$cp":../de.bsvrz.dua."$appname"-test.jar"
cp=$cp":../../junit-4.1.jar"

########################
#     Ausfuehrung      #
########################

for ((i=0; i < ${#tests[@]}; i++));
do
	echo "================================================="
	echo "="
	echo "= Test Nr. "$(($i+1))":"
	echo "="
	echo "= "${testTexts[$i]}
	echo "="
	echo "================================================="
	echo 
	java -cp $cp $jvmArgs org.junit.runner.JUnitCore "de.bsvrz.dua."$appname"."${tests[$i]}
	pause 5
done

exit
