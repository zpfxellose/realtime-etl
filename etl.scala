package sumscope.etl

import scala.sys.process._
import scala.xml.XML
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.language.postfixOps

import java.util.Calendar
import java.text.SimpleDateFormat



object Etl extends App {

	val tmpFolderFile = new File(Env.tmpFolder)
	if (tmpFolderFile.exists) {
		Seq("rm", "-r", Env.tmpFolder).!!
		Seq("mkdir", "-p", Env.tmpFolder).!!
	}


	val needInsert = true

	while(true){
		Env.confs/*.filter(f => f.target.table == "DICT_BOND_SUBTYPE")*/.foreach(conf =>{
					//println(conf.target.table + ":")
			Transform(conf).run
			Load(conf).run
		})
		Thread.sleep(1000)
		print(".")
	}
}
