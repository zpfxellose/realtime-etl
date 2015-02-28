package sumscope.etl

import scala.sys.process._
import scala.xml.XML
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.language.postfixOps

import java.util.Calendar
import java.text.SimpleDateFormat

import scala.util.control.Exception.allCatch

case class Load(conf : Conf) extends Processor(conf) {
	import Env._
	val dataFolder = s"$baseDataFolder/load"
	val logFolder = s"$baseLogFolder/load"

	def run {
		//println("loading...")
		// for each source table
		conf.sources.foreach(src => {
		    def targetTableDir = new File(dataFolder + "/" + conf.target.db.host + "_" + conf.target.schema + "/" + conf.target.table + "/" + src.table)

			val t = getLatestTime(targetTableDir)
			val srcPaths = getNewerPath(t._1, t._2, t._3,s"$baseDataFolder/transform/${conf.target.db.host}_${conf.target.schema}/${conf.target.table}/${src.table}")
/*				val fileBase = (new File(s"$baseDataFolder/transform/${conf.target.db.host}_${conf.target.schema}/${conf.target.table}")).listFiles
				if (fileBase != null)
					fileBase.flatMap(f => getNewerPath(t._1, t._2, t._3, f.getPath))
				else
					Array[File]()
			}
*/
		
			def getTargetFile = dataFolder +  "/" + conf.target.db.host + "_" + conf.target.schema + "/" +  conf.target.table + "/" + src.table + targetDate + targetTimeFormat.format(java.util.Calendar.getInstance.getTime) + ".load"
			val targetFile = getTargetFile

			def isDoubleNumber(s: String): Boolean = (allCatch opt s.toDouble).isDefined

			lazy val allColNames = runShell(Seq("mysql", "--login-path=" + conf.target.db.name, "--skip-column-names", "--raw", "-e", "desc " + conf.target.schema+'.'+ conf.target.table)).map(_.split('\t').head)
			lazy val tarColNames = {
				if (conf.confType == "Dict")  //if it is a dict, let database set id column automatically with auto_increment setting
					allColNames.drop(1).take(conf.sources.head.mappingOrders.size)
				else 
					allColNames.take(conf.sources.head.mappingOrders.size)
			}
			lazy val keyIndexes = conf.targetKeys.map(key => tarColNames.indexOf(key))
	
			val logFileWriter = new BufferedWriter(new FileWriter(new File(logFilePath), true))
			var cnt = 0
			srcPaths.foreach(srcFile => if (srcFile.exists) {
				// println(srcFile.getPath)
				val lineNum = io.Source.fromFile(srcFile).getLines.size
				// println(srcFile.getPath)
				if (lineNum < 1000) {
					//insert using sql
					io.Source.fromFile(srcFile).getLines.foreach(line => {
						val formatedLine = line.split('\t').map(c => if (isDoubleNumber(c) || c == "NULL") c else s"'$c'").mkString(",")
						val cmdBody = {  //if it's dict and not distinct from large table, use update to keep up with source, otherwise, don't update
							if (conf.confType == "Dict" && src.distinct != true) "insert into " + conf.target.schema + "." + conf.target.table + " (" + tarColNames.mkString(",") + ", DATA_INSERT_TIME) values (" + formatedLine + ", now()) on duplicate key update " + tarColNames.map(x => x + "=Values(" +x+")").mkString(",") + ", DATA_UPDATE_TIME = now(); select last_insert_id();"
							else 												 "insert into " + conf.target.schema + "." + conf.target.table + " (" + tarColNames.mkString(",") + ", DATA_INSERT_TIME) values (" + formatedLine + ", now()) on duplicate key update " + tarColNames.map(x => x + "=Values(" +x+")").head + ", DATA_UPDATE_TIME = now(); select last_insert_id();"
						}
						// println(cmdBody)
						val res = runShell(Seq("mysql", "--login-path=" + conf.target.db.name, "--skip-column-names", "--raw", "-e", cmdBody)).head
//						println(res)
						//if it is a Dict, update the new key to dicts
						if (conf.confType == "Dict" && res != "0") {
							val cols = line.split('\t')
							val keyValues = keyIndexes.map(i => {
								val c = cols.apply(i)
								val quoates = "'(.*)'".r //remove ' at begining and the end of the column
								c match {
									case quoates(s) => s
									case _ => c
								}
							})
							dicts.getOrElseUpdate(conf.target.table, collection.mutable.Map[List[String],String]()).put(keyValues.toList, res)  // update dict if anything changes
							//println(dicts.get(conf.target.table).get)
						}
						cnt = cnt + 1
					})
				} else {
					// batch load
					if (conf.confType == "Dict") {
						val targetFileHandler = new File(targetFile)
						if (!targetFileHandler.exists) {
							targetFileHandler.getParentFile.mkdirs
							targetFileHandler.createNewFile
						}
						val targetFileWriter = new BufferedWriter(new FileWriter(targetFileHandler))
						targetFileWriter.write("start transaction;\n")
						io.Source.fromFile(srcFile).getLines.foreach(line => {
							val formatedLine = line.split('\t').map(c => if (isDoubleNumber(c) || c == "NULL") c else s"'$c'").mkString(",")
							val cmdBody = {  //if it's dict and not distinct from large table, use update to keep up with source, otherwise, don't update
								if (conf.confType == "Dict" && src.distinct != true) "insert into " + conf.target.schema + "." + conf.target.table + " (" + tarColNames.mkString(",") + ", DATA_INSERT_TIME) values (" +  formatedLine + ", now()) on duplicate key update " + tarColNames.map(x => x + "=Values(" +x+")").mkString(",") + ", DATA_UPDATE_TIME = now();"
								else "insert into " + conf.target.schema + "." + conf.target.table + " (" + tarColNames.mkString(",") + ", DATA_INSERT_TIME) values (" +  formatedLine + ", now()) on duplicate key update " + tarColNames.map(x => x + "=Values(" +x+")").head + ", DATA_UPDATE_TIME = now();"
							}
							// println(cmdBody)
							targetFileWriter.write(cmdBody + "\n")
							cnt = cnt + 1
						})
						targetFileWriter.write("commit;\n")
						targetFileWriter.close
						runShell(Seq(s"$scriptFolder/load.sh", targetFile, conf.target.db.name))
						targetFileHandler.delete
						if (conf.confType == "Dict") 
							Env.refreshDict(conf)
					} else {
						runShell(Seq("mysql", "--login-path=" + conf.target.db.name, "-e", s"load data local infile '${srcFile.getPath}' into table ${conf.target.schema}.${conf.target.table} CHARACTER SET utf8 set DATA_UPDATE_TIME = now(), DATA_INSERT_TIME = now()"))
						cnt = runShell(Seq("wc","-l", s"${srcFile.getPath}")).head.split(' ').head.toInt
					}
				}
			})
			
			if (cnt > 0){
				val loadOK = new File(targetFile+"OK")
				if (!loadOK.exists) {
					loadOK.getParentFile.mkdirs
					loadOK.createNewFile
				}
				printAndLog(s"${conf.target.table}: $cnt Records Loaded from ${src.table}", logFileWriter)
			}
			logFileWriter.close
		})
	}
}
