package sumscope.etl

import scala.sys.process._
import scala.xml.XML
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.language.postfixOps

import java.util.Calendar
import java.text.SimpleDateFormat


case class Transform(conf : Conf) extends Processor(conf) {
	import Env._
	val dataFolder = s"$baseDataFolder/transform"
	val logFolder = s"$baseLogFolder/transform"


	def run {
		// file is temprarily put in tmp folder, it will move to transform folder after work finish
        val targetFile = tmpFolder + s"/${conf.target.table}.tmp"
		val targetFileHandler = new File(targetFile)
		if (!targetFileHandler.exists) {
			targetFileHandler.getParentFile.mkdirs
			targetFileHandler.createNewFile
		}
		val logFileWriter = new BufferedWriter(new FileWriter(new File(logFilePath), true))

		// used to lookup pk using unique keys in dict table
		def pKeyLookup(keys : List[String], tableName : String) : String = {
			val res = dicts.getOrElse(tableName, {
				printAndLog(s"$tableName dicts does not exists", logFileWriter)
				logFileWriter.close
				System.exit(1)
				collection.mutable.Map[List[String],String]()
			}).get(keys).getOrElse("")
			//println(res)
			if (res == ""){
				printAndLog("Key Not Found: " + keys.mkString(",") + " " + tableName, logFileWriter)
				//printAndLog(dicts.getOrElse(tableName, collection.mutable.Map[List[String],String]()).toString, logFileWriter)
				//dicts.get(tableName).get.foreach(a => println(a._1.mkString(" ")))
			}
			res
		}
		//println("transforming..")
		// for every source table in the configure
		conf.sources.foreach(src =>{
			val writer = new BufferedWriter(new FileWriter(targetFileHandler))
			var cnt = 0
			def getTargetFile = dataFolder +  "/" + conf.target.db.host + "_" + conf.target.schema + "/" +  conf.target.table + s"/${src.table}" + targetDate + targetTimeFormat.format(java.util.Calendar.getInstance.getTime) + ".data"
			val t = getLatestTime(new File(dataFolder +  "/" + conf.target.db.host + "_" + conf.target.schema + "/" +  conf.target.table + s"/${src.table}"))

			// get the source file path that needs to be transformed
			val srcPaths = getNewerPath(t._1, t._2, t._3, s"$baseDataFolder/extract/${src.db.host}_${src.schema}/${src.table}").sortBy(_.getPath)

			// get column names of src table
        	lazy val colNames = runShell(Seq("mysql","--login-path=" + src.db.name, "--skip-column-names","--raw", "-e", "desc " + src.schema + "." + src.table)).map(l => l.trim.split('\t').head.trim)

        	// get index of every column in mappingOrders
			lazy val colIndexes = src.mappingOrders.map(m => (colNames.indexOf(m), m))
			//println(colIndexes.mkString(","))
			srcPaths.foreach(path => {printAndLog(s"Transform: ${path.getPath}", logFileWriter); if(path.exists) io.Source.fromFile(path).getLines.foreach(line => {
				var valid = 1
				val cols = line.split('\t')
				// var srcCnt = 0
				//filtering
				//for aFilter in src.filters
				src.filters.foreach(filter => {
					val fIndex = colNames.indexOf(filter.colName)
//					println(filter.colName, ":" + filter.values.mkString(" ") + " " + cols(fIndex))
					if (!filter.values.contains(cols(fIndex)))
						valid = 0
				})
				if (valid == 1){
					// do preProcess
					val preProcessedCols : List[String] = (0 to cols.size - 1).map(i => {
						var res = cols(i)
						src.preProcesses.foreach(preP =>{
							// println(colIndexes)
							// println(i)
							// println(colNames)
							if(colNames(i) == preP.column){
								// println(preP.scripts)
								preP.scripts.foreach(script =>{
									// println(s"${colNames(i)},${preP.column},${script} ${res}")
									val cmd = {
										if (res.trim == "") script + " \"" + res + "\""
										else script + " " + res
									}
									res = cmd.!!.trim
								})
							}
						})
						res
					}).toList
					// println(preProcessedCols)
					val resLine = colIndexes.map(index => {
						//for those need distinct, delflag is always 0
						if (index._1 == -1) {
//							val sql = """SQL(?<=\()[^)]+(?=\))""".r
							val sql = "SQL\\((.*)\\)".r
							val key = "KEY\\((.*)\\)".r
							index._2 match {
								case sql(s) => s
								case key(k) => {
									val kCols = k.split(',').map(kc => kc.trim).init
									val tab = k.split(',').map(kc => kc.trim).last
									val keyVal = pKeyLookup(kCols.map(kc => if (kc == "0") "0" else {
										val index = colNames.indexOf(kc)
										if (index == -1) {
											printAndLog(s"$kc is not a column in table ${src.table}", logFileWriter)  
											logFileWriter.close()
											System.exit(1)
										}
										preProcessedCols(index)
										// try{
										// 	cols(index)
										// } catch {
										// 	case e:Throwable => {
										// 		printAndLog(s"$kc is not found in table ${src.table}", logFileWriter)
										// 		logFileWriter.close()
										// 		System.exit(1)
										// 		""
										// 	}
										// }
									}).toList, tab)
									if (keyVal == "") {
										printAndLog(s"(${k.split(',').init.mkString(",")}) not found in $tab", logFileWriter)
										logFileWriter.close()
										System.exit(1)
										valid = 0  //if not find the key, ignore this line
									}
									keyVal
								}
								case _ => index._2.trim
							}
						}
						// if target table is dict, the source table needs to be distinct, set all delflag to 0
						else if (src.distinct && colNames(index._1) == "delflag")
							"0"
						else {
							val resCol = preProcessedCols(index._1).trim.replace("'","\\'").replace("\n"," ")
							// if (resCol == "NULL") resCol
							// else if (isDoubleNumber(resCol)) resCol
							// else "'" + resCol + "'"
							resCol
						}
					}).mkString("\t")
					//println(resLine)
					if (valid == 1) {
						writer.write(resLine + "\n")
						// srcCnt = srcCnt + 1
						cnt = cnt + 1
					}
				}
			})})
			writer.close
			if (cnt == 0)
				targetFileHandler.delete
			else{
				if (src.distinct)
					runShell(Seq("sort", "-u", "-o", targetFile, targetFile)).foreach(printAndLog(_, logFileWriter))
	
				val newTargetFile = getTargetFile
				if (conf.hasScript) {
					runShell(Seq("python",s"$scriptFolder/${conf.target.table}.py", targetFile, newTargetFile)).foreach(printAndLog(_, logFileWriter))
					// targetFileHandler.delete
				}
				else {
					runShell(Seq("mkdir", "-p", newTargetFile.split('/').init.mkString("/")))
					runShell(Seq("mv", targetFile, newTargetFile)); 
				}
				
				printAndLog(s"$newTargetFile: $cnt New Lines", logFileWriter)
			}
		})
		logFileWriter.close()
	}
}
