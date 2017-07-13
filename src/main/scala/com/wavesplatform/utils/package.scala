package com.wavesplatform

import java.io.File

import org.h2.mvstore.MVStore

package object utils {
  import scala.math._

  def base58Length(byteArrayLength: Int): Int = ceil(log(256) / log(58) * byteArrayLength).toInt

  def createMVStore(fileName: String): MVStore = {
    Option(fileName).filter(_.trim.nonEmpty) match {
      case Some(s) =>
        val file = new File(s)
        file.getParentFile.mkdirs().ensuring(file.getParentFile.exists())

        val db = new MVStore.Builder().fileName(s).open()
        db.rollback()
        db
      case None =>
        new MVStore.Builder().open()
    }
  }
}
