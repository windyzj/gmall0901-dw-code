package com.atguigu.gmall0901.dw.common.util

import java.util.Objects

import com.google.gson.GsonBuilder
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


object MyEsUtil {
  private val ES_HOST = "http://hadoop1"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)

  }


  def insertBulk(indexName:String,docList:List[Any]): Unit ={
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder
    bulkBuilder.defaultIndex(indexName).defaultType("_doc")
    println(docList.mkString("\n"))
    for (doc <- docList ) {

      val index: Index = new Index.Builder(doc) .build()
      bulkBuilder.addAction(index)
    }
    jest.execute(bulkBuilder.build())
    close(jest)
  }


  def main(args: Array[String]): Unit = {
     val jest: JestClient = getClient
     val doc="{\n  \"name\":\"li4\",\n  \"age\": 44\n}"
     val index: Index = new Index.Builder(doc).index("gmall0901_test").`type`("_doc").build()
     jest.execute(index)
  }

}