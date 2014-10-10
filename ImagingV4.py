#!/usr/bin/python
import os
import subprocess
import time
import argparse

from subprocess import Popen
from datetime import datetime

startTime = datetime.now()

#===================Set all arguments===============
parser = argparse.ArgumentParser(description='Image ALL OF THE THINGS!')
parser.add_argument('-T' , '-t', '--TaskSet', help='1 For Standard CABB imaging pipeline')
args = parser.parse_args()

if args.TaskSet is not None:
	TaskSet = int(args.TaskSet)
else:
	TaskSet = 1 

#get the Imaging Details here. Replace thing with a database call?

ImagingDetails = {}
ProcList = []

#================= Misc =================

ImagingDetails['MaxProcesses'] = 2
ImagingDetails['ProjectNum'] = "C2648"

ImagingDetails['FWHM'] = ""
ImagingDetails['Cell'] = ""
ImagingDetails['PositionAngle'] = ""

#================= Locations =================

ImagingDetails['SourcePath'] = "SourceTest"
ImagingDetails['DestinationPath'] = "DestinationTest"
ImagingDetails['DestinationLink'] = "i"

ImagingDetails['Images'] = ["pnt_1","pnt_2"]

#================= Invert =================

ImagingDetails['Imsize'] = "4,4,beam"
ImagingDetails['Offset'] = ""
ImagingDetails['Robust'] = "0"
ImagingDetails['Frequency'] = "2100"
ImagingDetails['Stokes'] = "i"
ImagingDetails['ActiveAntennasName'] = "12345"
ImagingDetails['ActiveAntennas'] = "\"-ant(6)\""
ImagingDetails['InvertOptions'] = "double,mfs,sdb"

#================= Selfcal =================

ImagingDetails['SelfCalAmount'] = 1
ImagingDetails['SelfCalIterations'] = 5500
ImagingDetails['SelfCalSigma'] = 22
ImagingDetails['SelfCalOptions'] = "mfs,phase"
ImagingDetails['SelfCalBin'] = 1
ImagingDetails['SelfCalInterval'] = 0.1

#================= MFClean =================

ImagingDetails['Iterations'] = 10000
ImagingDetails['Sigma'] = 5
ImagingDetails['CleanRegion'] = "perc(66)"

#================= Restor =================

ImagingDetails['RestorOptions'] = "mfs"

#================= Linmos =================

ImagingDetails['Bandwidth'] = 2.049

#Check to see if a particular item/folder exists within a particular folder. default folder to check is current one
def ReadFolder(ItemToFind, Path=os.getcwd()):
	for Files in os.listdir(Path):
		if ItemToFind == Files:
			break;
	else:
		return False;

	return True;

#Check to see how many processes are running at the same time, wait if max limit has been reached. 
def CheckProc(MaxProcesses):
	while len(ProcList) > MaxProcesses:
		print("===================Checking===================")

		time.sleep(3)

		for Proc in ProcList:
	 		if not(Proc.poll() is None):
				ProcList.remove(Proc)

#run the task UVaver
def UVaver(Image, ImagingDetails):
	Out = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])

	Task = "Uvaver "
	Task = Task + " vis='" + str(ImagingDetails['SourcePath']) + "/" + str(Image) + "'"
	Task = Task + " out='" + ImagingDetails['DestinationLink'] + "/" + str(Out) + "'"

	print Task
	ProcList.append(Popen(Task, shell=True))

#run the task Invert
def Invert(Image, ImagingDetails):
	UVaver = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])
	Map = str(Image) + ".map." + str(ImagingDetails['RoundNum'])
	Beam = str(Image) + ".beam." + str(ImagingDetails['RoundNum'])
	LogFile = str(Image) + ".invertlog." + str(ImagingDetails['RoundNum'])

	Task = "Invert "
	Task = Task + " vis='" + UVaver + "'"
	Task = Task + " map='" + Map + "'"
	Task = Task + " beam='" + Beam + "'"
	Task = Task + " imsize='" + str(ImagingDetails['Imsize']) + "'"
	Task = Task + " offset='" + str(ImagingDetails['Offset'])+ "'"
	Task = Task + " robust='" + str(ImagingDetails['Robust']) + "'"
	Task = Task + " select='" + str(ImagingDetails['ActiveAntennas']) + "'"
	Task = Task + " fwhm='" + str(ImagingDetails['FWHM']) + "'"
	Task = Task + " cell='" + str(ImagingDetails['Cell']) + "'"
	Task = Task + " stokes='" + str(ImagingDetails['Stokes']) + "'"
	Task = Task + " options='" + str(ImagingDetails['InvertOptions']) + "'"
	Task = Task + " > " + LogFile
	
	print Task
	ProcList.append(Popen(Task, shell=True))

#run the task MFClean
def MFClean(Image, ImagingDetails, SelfCalBool):
	Map = str(Image) + ".map." + str(ImagingDetails['RoundNum'])
	Beam = str(Image) + ".beam." + str(ImagingDetails['RoundNum'])
	Model = str(Image) + ".model." + str(ImagingDetails['RoundNum'])
	LogFile = str(Image) + ".invertlog." + str(ImagingDetails['RoundNum'])
	
	TheoreticalRMS = ""
	TheoreticalRMSArray = []

	LogFileArray = open(LogFile)

	for LogFileLine in LogFileArray:
		if "Theoretical" in LogFileLine:
			TheoreticalRMSArray = LogFileLine.split(" ")
			TheoreticalRMS = TheoreticalRMSArray[3]
	
	Task = "mfclean "
	Task = Task + " map='" + Map + "'"
	Task = Task + " beam='" + Beam + "'"
	Task = Task + " out='" + Model + "'"
	Task = Task + " region='" + ImagingDetails['CleanRegion'] + "'"

	if SelfCalBool == True:
		Task = Task + " niters='" + str(ImagingDetails['SelfCalIterations']) + "'"
		Task = Task + " cutoff='" + str(float(ImagingDetails['SelfCalSigma']) * float(TheoreticalRMS)) + "'"
	else:
		Task = Task + " niters='" + str(ImagingDetails['Iterations']) + "'"
		Task = Task + " cutoff='" + str(float(ImagingDetails['Sigma']) * float(TheoreticalRMS)) + "'"

	print Task
	ProcList.append(Popen(Task, shell=True))				

#run the task SelfCal
def SelfCal(Image, ImagingDetails):
	UVaver = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])
	Model = str(Image) + ".model." + str(ImagingDetails['RoundNum'])

	Task = "selfcal "
	Task = Task + " vis='"+ UVaver + "'"
	Task = Task + " model='" + Model + "'"
	Task = Task + " interval='" + str(ImagingDetails['SelfCalInterval']) + "'"
	#Task = Task + " select='" + str(InvertAntenna) + "'"
	Task = Task + " options='" + str(ImagingDetails['SelfCalOptions']) + "'"
		
	print Task
	ProcList.append(Popen(Task, shell=True))

#Run the task UVaver
def UVaverSelfCal(Image, ImagingDetails):
	Vis = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])
	Out = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'] + 1)

	Task = "Uvaver "
	Task = Task + " vis='" + Vis + "'"
	Task = Task + " out='" + Out + "'"

	print Task
	ProcList.append(Popen(Task, shell=True))

#Run the task Restor
def Restor(Image, ImagingDetails):
	Map = str(Image) + ".map." + str(ImagingDetails['RoundNum'])
	Beam = str(Image) + ".beam." + str(ImagingDetails['RoundNum'])
	Model = str(Image) + ".model." + str(ImagingDetails['RoundNum'])
	Restor = str(Image) + ".restor." + str(ImagingDetails['RoundNum'])

	Task = "restor "
	Task = Task + " map='" + Map + "'"
	Task = Task + " beam='" + Beam + "'"
	Task = Task + " model='" + Model + "'"
	Task = Task + " out='" + Restor + "'"
	Task = Task + " options='" + ImagingDetails['RestorOptions'] + "'"
	Task = Task + " fwhm='" + str(ImagingDetails['Robust']) + "'" 
	Task = Task + " pa='" + str(ImagingDetails['PositionAngle']) + "'"

	print Task
	ProcList.append(Popen(Task, shell=True))

#Run the task Linmos
def Linmos(ImagingDetails):
	Linmos = ImagingDetails['DestinationLink'] + "/" + str(ImagingDetails['ProjectNum']) + ".pbcorr"

	Task = "linmos "
	Task = Task + " in='" + ImagingDetails['DestinationLink'] + "/*restor*'"
	Task = Task + " out='" + Linmos + "'"
	Task = Task + " bw='" + str(ImagingDetails['Bandwidth']) + "'"

	print Task
	ProcList.append(Popen(Task, shell=True))

#Standard CABB Pipeline. UVaver, Invert, MFClean, SelfCal (optional - send to start. ), Restor, Linmos
def StandardCabbImaging(ImagingDetails):

	#if ReadFolder(ImagingDetails['DestinationPath']) == False:
	#	os.system("mkdir " + ImagingDetails['DestinationPath'])

	ImagingDetails['RoundNum'] = 1

	#===============Run UVaver to apply Calibrators=================
	for ImageName in ImagingDetails['Images']:
		CheckProc(ImagingDetails['MaxProcesses'])
		UVaver(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
	CheckProc(0)

	
	for RoundNum in range(1,ImagingDetails['SelfCalAmount'] + 1):
		print ("round num = " + str(RoundNum))
		ImagingDetails['RoundNum'] = RoundNum

		#===============Run Invert==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
			CheckProc(ImagingDetails['MaxProcesses'])
			Invert(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
		CheckProc(0)


		#===============Run MFClean==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
			CheckProc(ImagingDetails['MaxProcesses'])
			MFClean(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails, True);
		CheckProc(0)


		#===============Run SelfCal==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
			CheckProc(ImagingDetails['MaxProcesses'])
			SelfCal(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
		CheckProc(0)

		#===============Run UVaver to apply SelfCal==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
			CheckProc(ImagingDetails['MaxProcesses'])
			UVaverSelfCal(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
		CheckProc(0)

	#======================================================================================
	#======================================================================================
	#======================================================================================

	if ImagingDetails['SelfCalAmount'] >= 1:
		ImagingDetails['RoundNum'] += 1

	#===============Run Invert==================
	for ImageName in ImagingDetails['Images']:
		ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
		CheckProc(ImagingDetails['MaxProcesses'])
		Invert(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
	CheckProc(0)


	#===============Run MFClean==================
	for ImageName in ImagingDetails['Images']:
		ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
		CheckProc(ImagingDetails['MaxProcesses'])
		MFClean(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails, False);
	CheckProc(0)

	#===============Run Restor==================
	for ImageName in ImagingDetails['Images']:
		ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName
		CheckProc(ImagingDetails['MaxProcesses'])
		Restor(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
	CheckProc(0)

	#===============Run Linmos==================
	
	Linmos(ImagingDetails);
	#CheckProc(0)



if ReadFolder(ImagingDetails['DestinationPath']) == False:
	os.system("mkdir " + ImagingDetails['DestinationPath'])

if ReadFolder(ImagingDetails['DestinationLink']) == False:
	os.system("ln -s " + ImagingDetails['DestinationPath'] + "/ " + ImagingDetails['DestinationLink'])

ImagingDetails['MaxProcesses'] -= 1

#using the arguments from the usercall, run a selection of imaging tasks.
if TaskSet == 1:
	StandardCabbImaging(ImagingDetails);

CheckProc(0);

os.system("rm " + ImagingDetails['DestinationLink'])

print(      "\n\n\n\n+======================Finished======================+\n"       )
print("|            Time Taken    = " + str(datetime.now()) + "             |")
print("|            Time Finished = " + str(datetime.now()-startTime) + "             |")
print(            "\n+====================================================+\n"       )


















