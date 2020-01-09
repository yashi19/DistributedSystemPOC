package org.dist.workshop.simplekafka

import org.I0Itec.zkclient.{IZkDataListener, ZkClient}

class MyControllerChangeListener(zkClient: ZkClient,controller : MyController) extends IZkDataListener{
    override def handleDataChange(dataPath: String, data: Any): Unit = {
      val existingControllerId:String = zkClient.readData(dataPath)
      controller.setCurrent(existingControllerId.toInt)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      controller.elect()
    }
}
