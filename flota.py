import time
from beamngpy import BeamNGpy, Scenario, Vehicle, angle_to_quat
from beamngpy.sensors import Electrics, GPS, Damage
import json
from datetime import datetime, timezone
import msvcrt
from awscrt import io, mqtt
from awsiot import mqtt_connection_builder

beamng = BeamNGpy("localhost", 25252, home="PATH_TO_BEAMNG")
beamng.open()

auto01 = Vehicle("auto01", model="etk800", color="Green")
auto02 = Vehicle("auto02", model="etk800", color="Red")
auto03 = Vehicle("auto03", model="etk800", color="Blue")
auto04 = Vehicle("auto04", model="etk800", color="White")
auto05 = Vehicle("auto05", model="etk800", color="Black")
autos = [auto01, auto02, auto03, auto04, auto05]

electrics = Electrics()
damage = Damage()
for i in autos:
    i.sensors.attach("electrics", electrics)
    i.sensors.attach("damage", damage)

scenario = Scenario("west_coast_usa", "flota")
scenario.add_vehicle(
    auto01, pos=(-717.121, 101, 118.675), rot_quat=angle_to_quat((0, 0, 45))
)
scenario.add_vehicle(
    auto02, pos=(-713.567, -21.712, 102.373), rot_quat=angle_to_quat((0, 0, 45))
)
scenario.add_vehicle(
    auto03, pos=(-421.499, 180.484, 100.427), rot_quat=angle_to_quat((0, 0, 45))
)
scenario.add_vehicle(
    auto04, pos=(-572.057, 408.650, 109.019), rot_quat=angle_to_quat((0, 0, 45))
)
scenario.add_vehicle(
    auto05, pos=(-767.680, 368.681, 143.411), rot_quat=angle_to_quat((0, 0, 45))
)

scenario.make(beamng)
beamng.scenario.load(scenario)
beamng.scenario.start() 

ref_lon, ref_lat = 9.019681, -79.532475
gps_auto01 = GPS(
        "gps_auto01",
        beamng,
        auto01,
        pos=(0, 0, 0),
        ref_lon=ref_lon,
        ref_lat=ref_lat
    )
gps_auto02 = GPS(
        "gps_auto02",
        beamng,
        auto02,
        pos=(0, 0, 0),
        ref_lon=ref_lon,
        ref_lat=ref_lat
    )
gps_auto03 = GPS(
        "gps_auto03",
        beamng,
        auto03,
        pos=(0, 0, 0),
        ref_lon=ref_lon,
        ref_lat=ref_lat
    )
gps_auto04 = GPS(
        "gps_auto04",
        beamng,
        auto04,
        pos=(0, 0, 0),
        ref_lon=ref_lon,
        ref_lat=ref_lat
    )
gps_auto05 = GPS(
        "gps_auto05",
        beamng,
        auto05,
        pos=(0, 0, 0),
        ref_lon=ref_lon,
        ref_lat=ref_lat
    )
gps = [gps_auto01, gps_auto02, gps_auto03, gps_auto04, gps_auto05]

auto02.ai.set_mode("traffic")
auto03.ai.set_mode("traffic")
auto04.ai.set_mode("traffic")
auto05.ai.set_mode("traffic")

# === Parámetros de conexión ===
ENDPOINT = "ENDPOINT_MQTT_AWS"
CLIENT_ID = "CLIENT_ID"
PATH_TO_CERT = "CERT.PEM"
PATH_TO_KEY = "PRIVATE.KEY"
PATH_TO_ROOT = "ROOT-CA.CRT"
TOPIC = "TOPIC" 

# === Construir conexión MQTT ===
event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

mqtt_connection = mqtt_connection_builder.mtls_from_path(
    endpoint=ENDPOINT,
    cert_filepath=PATH_TO_CERT,
    pri_key_filepath=PATH_TO_KEY,
    ca_filepath=PATH_TO_ROOT,
    client_bootstrap=client_bootstrap,
    client_id=CLIENT_ID,
    clean_session=False,
    keep_alive_secs=30
)

print(f"Conectando al endpoint {ENDPOINT} con client ID {CLIENT_ID} …")
connect_future = mqtt_connection.connect()
connect_future.result()
print("Conectado.")

keys = [
    "abs","abs_active","airspeed","airflowspeed","altitude","avg_wheel_av",
    "brake","brake_lights","brake_input","check_engine","clutch","clutch_input",
    "clutch_ratio","driveshaft","engine_load","engine_throttle","esc","esc_active",
    "exhaust_flow","fog_lights","fuel","fuel_capacity","fuel_volume","gear","gear_a",
    "gear_index","gear_m","hazard","hazard_signal","headlights","highbeam","horn",
    "ignition","left_signal","lightbar","lights","lowbeam","lowfuel","lowhighbeam",
    "lowpressure","oil","oil_temperature","parking","parkingbrake","parkingbrake_input",
    "radiator_fan_spin","reverse","right_signal","rpm","rpmspin","rpm_tacho","running",
    "signal_l","signal_r","steering","steering_input","tcs","tcs_active","throttle",
    "throttle_input","turnsignal","two_step","water_temperature"
]

print("Enviando datos... Presiona 'q' para detener.\n")

while True:
    if msvcrt.kbhit():
        key = msvcrt.getch().decode('utf-8').lower()
        if key == 'q':
            print("\nDetenido por el usuario.")
            disconnect_future = mqtt_connection.disconnect()
            disconnect_future.result()
            beamng.disconnect()
            beamng.close()
            break

    for i in autos:
        i.sensors.poll()
        sensors = i.sensors
        electrics = sensors["electrics"]
        damage = sensors["damage"]["damage"]
        gps_data = gps[autos.index(i)].poll()

        data = {
        "id": i.vid,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "damage": round(damage, 2),
        "geopoint": f"{gps_data[0]['lon']},{gps_data[0]['lat']}",
        "wheelspeed": round(electrics["wheelspeed"] * 3.6, 2)
        }

        for key in keys:
            value = electrics.get(key)
            if isinstance(value, (float)):
                data[key] = round(value, 2)
            else:
                data[key] = value

        mensaje_json = json.dumps(data)

        print(f"Publicando al tópico {TOPIC}{i.vid}: {mensaje_json}")
        mqtt_connection.publish(
            topic=f"{TOPIC}{i.vid}",
            payload=mensaje_json,
            qos=mqtt.QoS.AT_MOST_ONCE
        )
        print("Publicado.")

    time.sleep(0.5)
