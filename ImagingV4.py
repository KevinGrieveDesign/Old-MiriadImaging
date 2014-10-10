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

#get the Imaging Details here. Replace this manual mess with a database call?

ImagingDetails = {}
ProcList = []

#================= Misc =================

ImagingDetails['MaxProcesses'] = 7
ImagingDetails['ProjectNum'] = "C2648"

ImagingDetails['FWHM'] = "25.97,22.13"
ImagingDetails['Cell'] = "6.96,6.96"
ImagingDetails['PositionAngle'] = "-1.3"

#================= Locations =================

ImagingDetails['SourcePath'] = "SourceTest"
ImagingDetails['DestinationPath'] = "DestinationTest"
ImagingDetails['DestinationLink'] = "i"

ImagingDetails['Images'] = ["pnt_1","pnt_2"]
#ImagingDetails['Images'] = ["pnt_1","pnt_2","pnt_3","pnt_4","pnt_5","pnt_6","pnt_7","pnt_8","pnt_9","pnt_10","pnt_11","pnt_12","pnt_13","pnt_14","pnt_15","pnt_16","pnt_17","pnt_18","pnt_19","pnt_20","pnt_21","pnt_22","pnt_23","pnt_24","pnt_25","pnt_26","pnt_27","pnt_28","pnt_29","pnt_30","pnt_31","pnt_32","pnt_33","pnt_34","pnt_35","pnt_36","pnt_37","pnt_38","pnt_39","pnt_40","pnt_41","pnt_42","pnt_43","pnt_44","pnt_45","pnt_46","pnt_47","pnt_48","pnt_49","pnt_50","pnt_51","pnt_52","pnt_53","pnt_54","pnt_55","pnt_56","pnt_57","pnt_58","pnt_59","pnt_60","pnt_61","pnt_62","pnt_63","pnt_64","pnt_65","pnt_66","pnt_67","pnt_68","pnt_69","pnt_70","pnt_71","pnt_72","pnt_73","pnt_74","pnt_75","pnt_76","pnt_77"]

#================= Invert =================

ImagingDetails['Imsize'] = "3,3,beam"
ImagingDetails['Offset'] = "05:17:14.64,-66:58:58.20"
ImagingDetails['Robust'] = "0"
ImagingDetails['Frequency'] = "2100"
ImagingDetails['Stokes'] = "i"
ImagingDetails['ActiveAntennasName'] = "12345"
ImagingDetails['ActiveAntennas'] = "\"-ant(6)\""
ImagingDetails['InvertOptions'] = "double,mfs,sdb,mosaic"

#================= Selfcal =================
#===== Phase =====

ImagingDetails['PhaseSelfCalAmount'] = 2
ImagingDetails['PhaseSelfCalOptions'] = "mfs,phase"

ImagingDetails['PhaseSelfCalIterations'] = 5500
ImagingDetails['PhaseSelfCalSigma'] = [50,25]

ImagingDetails['PhaseSelfCalBin'] = 1
ImagingDetails['PhaseSelfCalInterval'] = 0.1

#=== Amplitude ===

ImagingDetails['AmplitudeSelfCalAmount'] = 1
ImagingDetails['AmplitudeSelfCalOptions'] = "mfs,amp"

ImagingDetails['AmplitudeSelfCalIterations'] = 5500
ImagingDetails['AmplitudeSelfCalSigma'] = [15]

ImagingDetails['AmplitudeSelfCalBin'] = 1
ImagingDetails['AmplitudeSelfCalInterval'] = 0.1

#================= MFClean =================

ImagingDetails['Iterations'] = 10000
ImagingDetails['Sigma'] = 5
ImagingDetails['CleanRegion'] = "perc(66)"

#================= Restor =================

ImagingDetails['RestorOptions'] = "mfs"

#================= Linmos =================

ImagingDetails['Bandwidth'] = 2.049

#================= End Reading Data =================

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
		time.sleep(3)

		for Proc in ProcList:
	 		if not(Proc.poll() is None):
				ProcList.remove(Proc)

#run the task UVaver
def UVaver(Image, ImagingDetails):
	Out = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])

	Task = "uvaver "
	Task = Task + " vis='" + str(ImagingDetails['SourcePath']) + "/" + str(Image) + "'"
	Task = Task + " out='" + ImagingDetails['DestinationLink'] + "/" + str(Out) + "'"

	print Task
	ProcList.append(Popen(Task, shell=True))

#run the task Invert. This pipes to a LogFile (currently no output to terminal)
def Invert(Image, ImagingDetails):
	UVaver = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])
	Map = str(Image) + ".map." + str(ImagingDetails['RoundNum'])
	Beam = str(Image) + ".beam." + str(ImagingDetails['RoundNum'])
	LogFile = str(Image) + ".invertlog." + str(ImagingDetails['RoundNum'])

	Task = "invert "
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

#run the task MFClean. This reads the invert log file and then gets the Theoretical RMS to then times by an amount (i.e 5 sigma) to se the clean cutoff level.
def MFClean(Image, ImagingDetails, SelfCal):
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

	if SelfCal == True:
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
	Task = Task + " options='" + str(ImagingDetails['SelfCalOptions']) + "'"
		
	print Task
	ProcList.append(Popen(Task, shell=True))

#Run the task UVaver
def UVaverSelfCal(Image, ImagingDetails):
	Vis = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'])
	Out = str(Image) + ".uvaver." + str(ImagingDetails['RoundNum'] + 1)

	Task = "uvaver "
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
	ImagingDetails['RoundNum'] = 1

	#===============Run UVaver to apply Calibrators=================
	for ImageName in ImagingDetails['Images']:
		CheckProc(ImagingDetails['MaxProcesses'])
		UVaver(ImageName + "." + str(ImagingDetails['Frequency']), ImagingDetails);
	CheckProc(0)

	
	for RoundNum in range(1,ImagingDetails['PhaseSelfCalAmount'] + ImagingDetails['AmplitudeSelfCalAmount'] + 1):
		#Assign the options depending on what round we are processing.

		ImagingDetails['RoundNum'] = RoundNum

		# functions (such as mfclean and selfcal will use the values stored below. They will be allied dynamically depedning on what type of self cal is taking place. )
		if RoundNum <= ImagingDetails['PhaseSelfCalAmount']:
			ImagingDetails['SelfCalAmount'] = ImagingDetails['PhaseSelfCalAmount']
			ImagingDetails['SelfCalOptions'] = ImagingDetails['PhaseSelfCalOptions']

			ImagingDetails['SelfCalIterations'] = ImagingDetails['PhaseSelfCalIterations']
			ImagingDetails['SelfCalSigma'] = ImagingDetails['PhaseSelfCalSigma'][RoundNum - 1]

			ImagingDetails['SelfCalBin'] = ImagingDetails['PhaseSelfCalBin']
			ImagingDetails['SelfCalInterval'] = ImagingDetails['PhaseSelfCalInterval']
		else:
			ImagingDetails['SelfCalAmount'] = ImagingDetails['AmplitudeSelfCalAmount']
			ImagingDetails['SelfCalOptions'] = ImagingDetails['AmplitudeSelfCalOptions']

			ImagingDetails['SelfCalIterations'] = ImagingDetails['AmplitudeSelfCalIterations']
			ImagingDetails['SelfCalSigma'] = ImagingDetails['AmplitudeSelfCalSigma'][RoundNum - ImagingDetails['PhaseSelfCalAmount'] - 1]

			ImagingDetails['SelfCalBin'] = ImagingDetails['AmplitudeSelfCalBin']
			ImagingDetails['SelfCalInterval'] = ImagingDetails['AmplitudeSelfCalInterval']


		#===============Run Invert==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
			CheckProc(ImagingDetails['MaxProcesses'])
			Invert(ImageName, ImagingDetails);
		CheckProc(0)


		#===============Run MFClean==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
			CheckProc(ImagingDetails['MaxProcesses'])
			MFClean(ImageName, ImagingDetails, True);
		CheckProc(0)


		#===============Run SelfCal==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
			CheckProc(ImagingDetails['MaxProcesses'])
			SelfCal(ImageName, ImagingDetails);
		CheckProc(0)

		#===============Run UVaver to apply SelfCal==================
		for ImageName in ImagingDetails['Images']:
			ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
			CheckProc(ImagingDetails['MaxProcesses'])
			UVaverSelfCal(ImageName, ImagingDetails);
		CheckProc(0)

	#======================================================================================
	#======================================================================================
	#======================================================================================

	if ImagingDetails['SelfCalAmount'] >= 1:
		ImagingDetails['RoundNum'] += 1

	#===============Run Invert==================
	for ImageName in ImagingDetails['Images']:
		ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
		CheckProc(ImagingDetails['MaxProcesses'])
		Invert(ImageName, ImagingDetails);
	CheckProc(0)


	#===============Run MFClean==================
	for ImageName in ImagingDetails['Images']:
		ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
		CheckProc(ImagingDetails['MaxProcesses'])
		MFClean(ImageName, ImagingDetails, False);
	CheckProc(0)

	#===============Run Restor==================
	for ImageName in ImagingDetails['Images']:
		ImageName = ImagingDetails['DestinationLink'] + "/" + ImageName + "." + str(ImagingDetails['Frequency'])
		CheckProc(ImagingDetails['MaxProcesses'])
		Restor(ImageName, ImagingDetails);
	CheckProc(0)

	#===============Run Linmos==================
	Linmos(ImagingDetails);

#========================Finish Standard CABB Imaging =====================================


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

print(      "\n\n\n\n+=========================Finished=========================+\n"       )
print("|        Time Taken    = " + str(datetime.now()) + "        |")
print("|        Time Finished = " + str(datetime.now()-startTime) + "                    |")
print(            "\n+==========================================================+\n"       )


















