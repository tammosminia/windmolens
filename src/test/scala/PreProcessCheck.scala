import java.util.Date

import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck._
import PreProcess._

object PreProcessCheck extends Properties("PreProcess") {

  val genWindInput: Gen[WindInput] = for {
    mwhOutput <- Gen.choose(0.0, 100000.0)
    availability <- Gen.choose(0.0, 100.0)
    speed <- Gen.choose(0.0, 200.0)
    direction <- Gen.choose(0.0, 360.0)
    temperature <- Gen.choose(-10.0, 30.0)
  } yield WindInput(new Date(), mwhOutput, availability, speed, direction, temperature)

  implicit val arbWindInput = Arbitrary(genWindInput)

  property("direction_offset") = forAll { (last: Option[WindInput], current: WindInput) =>
    val direction_offset = PreProcess.direction_offset(last, current)
    Math.abs(direction_offset) < 360
  }

}
