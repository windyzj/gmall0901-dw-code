package com.atguigu.gmall0901.dw.realtime.bean

case class OrderInfo(
                      consignee: String,
                      orderComment: String,
                      consigneeTel: String,
                      operateTime: String,
                      orderStatus: String,
                      paymentWay: String,
                      userId: String,
                      imgUrl: String,
                      totalAmount: Double,
                      expireTime: String,
                      deliveryAddress: String,
                      createTime: String,
                      trackingNo: String,
                      parentOrderId: String,
                      outTradeNo: String,
                      id: String,
                      tradeBody: String,
                      provinceId:String,
                      var createDate: String,
                      var createHour: String,
                      var createHourMinute: String
                    ) {

}
