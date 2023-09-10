package com.hx.util

import org.json4s.jackson.Serialization.write
import org.json4s.jackson.{Serialization, parseJson}
import org.json4s.{Formats, NoTypeHints}

/**
 * @author AC 
 */
object JsonSerializer {

  private implicit val format: Formats = Serialization.formats(NoTypeHints)

  /**
   * 反序列化json字符串
   *
   * @param json json字符串
   * @tparam T 反序列化类型
   * @return 反序列化类型的对象
   */
  def decode[T](json: String)(implicit tag: Manifest[T]): T = parseJson(json).extract[T]

  /**
   * 序列化对象为json
   *
   * @param obj 对象
   * @tparam T 对象类型
   * @return json字符串
   */
  def encode[T <: AnyRef](obj: T): String = write(obj)
}
