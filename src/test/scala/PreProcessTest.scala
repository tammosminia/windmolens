import java.util.Date

import org.scalatest.FunSuite
import PreProcess._

class PreProcessTest extends FunSuite {
  val now = new Date()

  test("testDirection_offset") {
    def input(direction: Double) = WindInput(now, 100, 10, 10, direction, 20)
    assert(direction_offset(Some(input(0)), input(0)) === 0)
    assert(direction_offset(Some(input(0)), input(2)) === 2)
    assert(direction_offset(Some(input(5)), input(2)) === -3)
    assert(direction_offset(Some(input(5)), input(0)) === -5)
    assert(direction_offset(None, input(0)) === 0)
    assert(direction_offset(None, input(5)) === 0)
  }

}
