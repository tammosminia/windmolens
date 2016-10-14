import java.util.Date

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck._

object PreProcessCheck extends Properties("PreProcess") {

  val genWindInput: Gen[WindInput] = for {
    speed <- Gen.choose(0.0, 200.0)
    direction <- Gen.choose(0.0, 360.0)
    speedChange <- Gen.choose(0.0, 10.0)
  } yield WindInput(new Date(), speed, direction, speedChange)

  implicit val arbWindInput = Arbitrary(genWindInput)

  property("direction_offset") = forAll { (input1: WindInput, input2: WindInput) =>
    val direction_offset = PreProcess.direction_offset(input1, input2)
    Math.abs(direction_offset) < 360
  }

}
