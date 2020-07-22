package com.gw.utils


import com.alibaba.fastjson.{JSON, JSONObject}

object IsTrue {
  def isJson(content:String): Boolean ={
    try{
       val jobject:JSONObject = JSON.parseObject(content)
      val jj= jobject.size()
       if(jj>10) true else  false
    }catch {
      case _=> false

    }
  }
}
