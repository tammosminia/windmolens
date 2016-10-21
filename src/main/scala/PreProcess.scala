import java.util.Date

object PreProcess {
  case class WindInput(date: String, mwhOutput: Double, availability: Double, speed: Double, direction: Double, temperature: Double)
  case class ProcessedWindInput(date: String, mwhOutput: Double, availability: Double, speed: Double, direction: Double, temperature: Double, direction_offset: Double)

  def process(last: Option[WindInput], i: WindInput): ProcessedWindInput = {
    ProcessedWindInput(i.date, i.mwhOutput, i.availability, i.speed, i.direction, i.temperature, direction_offset(last, i))
  }

  def direction_offset(last: Option[WindInput], current: WindInput): Double = last match {
    case None => 0
    case Some(lastInput) => current.direction - lastInput.direction
  }
}
