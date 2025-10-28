from dataclasses import dataclass
from typing import Optional

@dataclass
class Alert:
    alert_type: str
    message: str

def evaluate(engine_temp: float, battery_voltage: float) -> Optional[Alert]:
    if engine_temp is not None and engine_temp > 90:
        return Alert(alert_type="Overheating", message=f"Engine temperature high: {engine_temp}°C")
    if battery_voltage is not None and battery_voltage < 11.0:
        return Alert(alert_type="Low Battery", message=f"Battery voltage low: {battery_voltage}V")
    return None
