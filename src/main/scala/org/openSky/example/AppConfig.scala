package org.openSky.example

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {
  private val config: Config = ConfigFactory.load()

  object filePath {
    private val filePathConfig: Config = config.getConfig("app.filePath")

    val sourceZipFilePath: String = filePathConfig.getString("sourceZIP")
    val sourceCSVFilePath: String = filePathConfig.getString("sourceCSV")
    val outputPath: String = filePathConfig.getString("outputResult")
    val outputTempPath: String = filePathConfig.getString("outputTemp")
  }
}