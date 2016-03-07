package io.github.miguel0afd

/**
  * Created by miguelangelfernandezdiaz on 07/03/16.
  */
object Utils {
  def randomInt(min: Int, max: Int) = {
    val r = scala.util.Random
    min + r.nextInt((max - min) + 1)
  }

}
