package sumscope.etl

import scala.sys.process._
import scala.xml.XML
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.language.postfixOps

import java.util.Calendar
import java.text.SimpleDateFormat

// case classes to parse conf xml file
case class Filter(colName : String, values : List[String])
case class PreProcess(column : String, scripts: List[String])
case class DB(name : String, host : String, username : String, pw : String)
case class SourceT(db : DB, schema : String, table : String, filters : List[Filter], mappingOrders : List[String], distinct : Boolean, preProcesses : List[PreProcess])
case class Table(db : DB, schema : String, table : String)
case class Conf(target : Table, sources : List[SourceT], confType : String, hasScript : Boolean){
    // Command to extract table create sql, to get keys
    lazy val keyCmd = if (confType == "Dict") Seq("mysql",s"--login-path=${target.db.name}", "--raw", "-e", s"show create table ${target.schema}.${target.table}") else Seq("")
    // get unique keys, only needed for Dict configures
    lazy val targetKeys = if (confType == "Dict") Env.runShell(keyCmd).filter(_.contains("UNIQUE KEY")).head.dropWhile(_ != '(').drop(1).init.replace("`","").split(',') else Array("")
    // get primary key, only needed for Dict configures
    lazy val targetPKey = if (confType == "Dict") Env.runShell(Seq("mysql","--login-path=" + target.db.name, "--raw", "--skip-column-names", "-e", s"desc ${target.schema}.${target.table}")).head.split('\t').head else ""
}

// object Env is used to put some global variables
object Env {
    // folders definination
    val curFolder = "pwd".!!.split('\n').head
    val baseDataFolder = s"$curFolder/data"
    val scriptFolder = s"$curFolder/scripts"
    val confFolder = s"$curFolder/conf"
    def confFile = XML.loadFile(new File(confFolder + "/conf_example.xml"))
    val baseLogFolder = s"$curFolder/log"
    val tmpFolder = s"$baseDataFolder/tmp"

    // all databases
    // DBs is a map from db's name to a DB object
    // DBs.get("220") to get the 220 DB object
    def DBs = (confFile \ "databases" \ "database").theSeq.map(db => {
        (db \ "name" text) -> DB(db \ "name" text, db \ "host" text, db \ "username" text, db \ "password" text)
    }).toMap

    // defined as function nor value, so the configuration file is parsed everytime confs is used
    // confs is the handler of the whole xml config file
    def confs : List[Conf] = (confFile \\ "configure").theSeq.map(conf => {
        val target = Table(DBs.get(conf \ "target" \ "database" text).get, conf \ "target" \ "schema" text, conf \ "target" \ "table" text)
        val sources = (conf \ "sources" \ "source").theSeq.map(src => {
            val filters = (src \ "filters" \ "filter").theSeq.map(filter => {
                Filter(filter \ "column" text, (filter \ "values" \ "value").theSeq.map(_.text).toList)
            }).toList
            val mappingOrders = (src \ "mappingOrder" \ "sourceCol").theSeq.map(s => s text).toList
            val distinct : Boolean = if ((src \ "distinct" text) == "true") true else false
            val preProcesses : List[PreProcess] = (src \ "PreProcesses" \ "PreProcess" ).theSeq.map(pre => {
                val col = pre \ "column" text
                val scripts = ( pre \ "script").theSeq.map(script => script.text).toList
                PreProcess(col, scripts)
            }).toList
            SourceT(DBs.get(src \ "database" text).get, src \ "schema" text, src \ "table" text, filters, mappingOrders, distinct, preProcesses)
        }).toList
        val confType : String = conf \ "type" text;
        val hasScript : Boolean = if ((conf \ "script" text) == "true") true else false
        Conf(target, sources, confType, hasScript);
    }).toList

    // dicts contains the key to code mapping of all dict tables
    // dicts are generated when the program start, it extract data from database to get all key code mapping 
    // it is defined lazy, so it is executed as late as possible, after the null value record is inserted
    // dicts.get(tableName) to get the dict for the table
    // dicts.get(tableName).get.get(values) to get the key
    lazy val dicts = {
        if (Etl.needInsert) initInsert()
        //we use mutable map here, so the dict can be updated in the future
        collection.mutable.Map() ++ confs.filter(_.confType == "Dict").map(conf =>{
        // println{s"Dict: ${conf.target.table}"}
        //println(s"select ${pKey},${keys.mkString(",")} from ${conf.target.schema}.${conf.target.table}")
            val res : collection.mutable.Map[List[String],String] = collection.mutable.Map() ++ runShell(Seq("mysql","--login-path=" + conf.target.db.name, 
                "--raw", "--skip-column-names", "-e", 
                s"select ${conf.targetPKey},${conf.targetKeys.mkString(",")} from ${conf.target.schema}.${conf.target.table}")).toList.map(l => {
                val c = l.split('\t')
        //      if (conf.target.table == "DICT_BOND_MARKET_TYPE") println(c.tail.toList)
                c.tail.toList -> c.head
            }).toMap
            (conf.target.table -> res)
        }).toMap
    }

    // regenerate the dict for particular target table
    def refreshDict(conf : Conf) {
        if (conf.confType != "Dict") return
        var res : collection.mutable.Map[List[String],String] = collection.mutable.Map() ++ runShell(Seq("mysql","--login-path=" + conf.target.db.name, 
            "--raw", "--skip-column-names", "-e", 
            s"select ${conf.targetPKey},${conf.targetKeys.mkString(",")} from ${conf.target.schema}.${conf.target.table}")).toList.map(l => {
            val c = l.split('\t')
            c.tail.toList -> c.head
        }).toMap
        dicts.put(conf.target.table, res)
    }
    

    // run shell command, if any error occurs, rerun the command, until it is successful
    def runShell(cmd : Seq[String]) : List[String] = {
        var done = false
//      println(cmd)
        while(!done){
            try{
                val res = cmd.lines.toList
                done = true
                return res
            } catch {
                case e : Throwable => {
                    println(cmd)
                    println(e)
                    List("")
                }
            }
        }
        return List("")
    }

    // do the NULL value insert, when the program starts
    def initInsert() {
        println("Inserting...")
        (confFile \\ "configure").theSeq.filter(f => !(f \ "insert" isEmpty)).map(conf => {
            val insertVals = (conf \ "insert" \\ "column").theSeq.map(_ text).mkString(",")
            println(Seq("mysql", "--login-path=" + (conf \ "target" \ "database" text), "-e", 
                "insert ignore into " + (conf \ "target" \ "schema" text) + "." + (conf \ "target" \ "table" text) + " values (" + insertVals + ")"
            ).!!.trim)
        })
    }
}
