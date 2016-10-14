import java.util.Date

case class WindInput(date: Date, speed: Double, direction: Double, average_wind_direction_change: Double)
case class ProcessedWindInput(date: Date, speed: Double, direction: Double, average_wind_direction_change: Double,
                              speed_x_change: Double, direction_offset: Double)

object PreProcess {
  def process(last: WindInput, current: WindInput): ProcessedWindInput = {
    ProcessedWindInput(current.date, current.speed, current.direction, current.average_wind_direction_change,
      speed_x_change(current), direction_offset(last, current))
  }

  def speed_x_change(current: WindInput): Double = current.speed * current.average_wind_direction_change

  def direction_offset(last: WindInput, current: WindInput): Double = current.direction - last.direction
}