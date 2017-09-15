package com.github.mirabout.calldb

import java.nio.file.Paths

import com.github.mauricio.async.db.Connection
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.github.mauricio.async.db.postgresql.util.URLParser
import org.specs2.execute.{AsResult, Result}
import org.specs2.mutable.Around
import org.specs2.specification.Scope

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.Source

trait WithConnectionLifecycle {
  private[this] var _connection: Connection = _
  protected val connectionString: String

  implicit def connection: Connection = _connection

  protected def connect(): Connection = {
    val configuration = URLParser.parse(connectionString)
    val connection = new PostgreSQLConnection(configuration)
    _connection = Await.result(connection.connect, 5.seconds)
    _connection
  }

  protected def disconnect(connection: Connection): Unit = {
    if (connection.isConnected) {
      Await.ready(connection.disconnect, 5.seconds)
    }
  }
}

class WithConnection(val connectionString: String) extends Around with Scope with WithConnectionLifecycle {
  override def around[T: AsResult](t: => T): Result = {
    // Make a local copy of connection val to mitigate laziness (?) issues
    val connection = connect()
    try {
      AsResult(t)
    } finally {
      disconnect(connection)
    }
  }
}

trait WithSqlExecutedBeforeAndAfter extends WithConnectionLifecycle {
  val sqlCommonName: String

  override def connect(): Connection = {
    val connection = super.connect()
    executeSql(connection, "begin transaction isolation level serializable")
    executeSql(connection, readFileAsString(mkFilePath("Before")))
    connection
  }

  override def disconnect(connection: Connection): Unit = {
    executeSql(connection, readFileAsString(mkFilePath("After")))
    executeSql(connection, "rollback")
    super.disconnect(connection)
  }

  private def mkFilePath(postfix: String): String = {
    val fileName = sqlCommonName + "_" + postfix + ".sql"
    val path = Paths.get(".", "src/test/sql", fileName)
    path.toAbsolutePath.normalize().toString
  }

  protected def readFileAsString(filename: String): String =
    Source.fromFile(filename).getLines().mkString("\n")

  protected def executeSql(connection: Connection, sqlString: String): Unit =
    Await.result(connection.sendQuery(sqlString), 5.seconds)
}

// Change connection string according to your test machine setup
class WithTestConnection extends WithConnection(WithTestConnection.ConnectionString)

object WithTestConnection {
  final val ConnectionString = "jdbc:postgresql://localhost:5432/testcalldb?user=testcalldb&password=testcalldb"
}

class WithTestConnectionAndSqlExecuted(val sqlCommonName: String) extends WithTestConnection with WithSqlExecutedBeforeAndAfter