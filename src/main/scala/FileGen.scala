import java.io._
import java.io.File
import scala.reflect.io.Directory
import better.files._

object FileGen {

  def iterator(): Unit = {
    val directory = file"./dataS/logs"
    directory.clear()
    Thread.sleep(9000)
    val numFiles = 20
    for (i <- 1 to numFiles) {
      genData(i)
      Thread.sleep(5000)
    }
  }

  def getCurrentDateTime: String = {
    val dateFormat = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
    dateFormat.format(new java.util.Date())
  }

  def getContentLength: Int = {
    (Math.random() * 10000).toInt
  }

  def genData(i: Int): Unit = {
    val logFile = new File(f"dataS/logs/access$i.log")
    val writer = new BufferedWriter(new FileWriter(logFile))
    val numLogs = 10000 // number of log entries to generate
    val ipAddressList = List("192.168.0.1", "10.0.0.1", "172.16.0.1")
    val httpMethodList = List("GET", "POST", "PUT", "DELETE")
    val httpPathList = List("/index.html", "/about.html", "/contact.html","/courses.html","/dataengineering","/big-data-analytics")
    val httpStatusCodeList = List("200", "301", "404", "500","204","206","400","405","408")
    val referrerList = List("https://www.google.com/","https://stackoverflow.com","https://www.reddit.com","https://github.com" ,"https://www.yahoo.com/", "https://www.bing.com/")
    val userAgentList = List("Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0)")

    for (_ <- 1 to numLogs) {
      val ipAddress = ipAddressList((Math.random() * ipAddressList.length).toInt)
      val httpMethod = httpMethodList((Math.random() * httpMethodList.length).toInt)
      val httpPath = httpPathList((Math.random() * httpPathList.length).toInt)
      val httpStatusCode = httpStatusCodeList((Math.random() * httpStatusCodeList.length).toInt)
      val referrer = referrerList((Math.random() * referrerList.length).toInt)
      val userAgent = userAgentList((Math.random() * userAgentList.length).toInt)

      val logEntry = s"$ipAddress - - [${getCurrentDateTime}] \"$httpMethod $httpPath HTTP/1.1\" $httpStatusCode ${getContentLength} \"$referrer\" \"$userAgent\"\n"
      writer.write(logEntry)
    }

    writer.close()
  }
  def main(args: Array[String]): Unit = {
    iterator()
  }
}
