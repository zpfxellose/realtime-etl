package sumscope.etl

import scala.sys.process._
import scala.xml.XML
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.language.postfixOps

import java.util.Calendar
import java.text.SimpleDateFormat

// abstract class for Transform and Load
abstract class Processor(conf : Conf) {
	import Env._

	val dataFolder : String 
	val logFolder : String 

	//logFilePath is determined when used, because of the exact timestamp is needed for the path
	def logFilePath = s"$logFolder/${dateFormat.format(java.util.Calendar.getInstance.getTime)}.log"

	val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
	val timeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss ")

	def targetTableDir = new File(dataFolder + "/" + conf.target.db.host + "_" + conf.target.schema + "/" + conf.target.table)

	val targetDateFormat = new java.text.SimpleDateFormat("/yyyy_MM/dd/")
	def targetDate = targetDateFormat.format(java.util.Calendar.getInstance.getTime)
	val targetTimeFormat = new java.text.SimpleDateFormat("HH_mm_ss")

	// get newer data path from extract, transform data folder. Newer than time provided
	def getNewerPath(year_mon : String, day : String, time : String, baseDir : String) : Array[File] = {
		val cur = (new java.util.Date).getTime
		val baseFile = new File(baseDir)
		if (baseFile.listFiles == null) return Array[File]()

		baseFile.listFiles.filter(dir => dir.getName.split('.').head == year_mon).flatMap(yDir =>{
			if (yDir.listFiles == null) Array[File]()
			else 
				try {
					yDir.listFiles.filter(dir => dir.getName.split('.').head == day).flatMap(dDir => {
						if (dDir.listFiles == null) Array[File]()
						else dDir.listFiles.filter(dir => {dir.getName.split('.').head > time})
					})
				} catch {
					case e: Exception => {
						Array[File]()
					}
				}
		}) ++
		baseFile.listFiles.filter(dir => dir.getName.split('.').head == year_mon).flatMap(yDir =>{
			if (yDir.listFiles == null) Array[File]()
			else
				try {
		            yDir.listFiles.filter(dir => {
		            	dir.getName.split('.').head > day
	            	}).flatMap(_.listFiles)
				} catch {
					case e: Exception => {
						Array[File]()
					}
				}
		}) ++
		baseFile.listFiles.filter(dir => dir.getName.split('.').head > year_mon).flatMap(_.listFiles.flatMap(_.listFiles))
	}

	// get the latest file under specified table directory
	def getLatestTime(tableDir : File) : (String, String, String) = {
	
		if (!tableDir.isDirectory)
    		return ("0000_00","00","99_99_99")

		if (tableDir.listFiles.isEmpty)
    		return ("0000_00","00","99_99_99")

		val yyyy_mm = tableDir.listFiles.map(_.getName).max
    		val tableYearDir = new File(tableDir + "/" + yyyy_mm)
    		
		if (!tableYearDir.isDirectory)
        		return ("0000_00","00","99_99_99")
    		
		if (tableYearDir.listFiles.isEmpty){
		        tableYearDir.delete
		        return getLatestTime(tableDir)
		}
		val dd = tableYearDir.listFiles.map(_.getName).max
    		val tableYearDayDir = new File(tableYearDir + "/" + dd)
    		
		if (!tableYearDayDir.isDirectory)
        		return ("0000_00","00","99_99_99")

    		if (tableYearDayDir.listFiles.isEmpty){
		        tableYearDayDir.delete
		        return getLatestTime(tableYearDir)
		}
	
		val hh_mm_ss = tableYearDayDir.listFiles.map(_.getName).max.split('.').apply(0)

		return (yyyy_mm,dd,hh_mm_ss)
	}
		
	
	def printAndLog(str : String, logFileWriter : BufferedWriter) {
		if (str == null) return
		if (str.trim == "") return
		logFileWriter.write(timeFormat.format(java.util.Calendar.getInstance.getTime) + str.replace("\n"," ") + "\n")
	}

	def run
}
