import java.util.Date

import org.scalatest.FunSuite

class PreProcessTest extends FunSuite {
  import PreProcess._

  val now = new Date()

  test("testSpeed_x_change") {
    def input(speed: Double, change: Double) = WindInput(now, speed, 0, change)
    assert(speed_x_change(input(10.2, 1)) === 10.2)
    assert(speed_x_change(input(10.2, 0)) === 0)
    assert(speed_x_change(input(10.2, 10)) === 102)
    assert(speed_x_change(input(0, 10)) === 0)
  }

  test("testDirection_offset") {
    assert(direction_offset(WindInput(now, 0, 0, 0), WindInput(now, 0, 0, 0)) === 0)
    assert(direction_offset(WindInput(now, 0, 0, 0), WindInput(now, 0, 2, 0)) === 2)
    assert(direction_offset(WindInput(now, 0, 5, 0), WindInput(now, 0, 2, 0)) === -3)
  }

}
